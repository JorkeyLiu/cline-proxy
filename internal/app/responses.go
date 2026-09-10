package app

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// chatStreamToResponses 将上游 chat.completions SSE 流转换为 Responses SSE 流 (single state)
func chatStreamToResponses(w http.ResponseWriter, upstream *http.Response, onUsage func(map[string]any), reqParams ...map[string]any) {
	var params map[string]any
	if len(reqParams) > 0 {
		params = reqParams[0]
	}
	state := newResponseState(params)
	emitter := newResponsesEmitter(w)
	emitter.emitCreatedAndInProgress(state, "")
	dec, err := newResponsesDecoder(upstream)
	if err != nil {
		log.Printf("  responses decoder init error: %v", err)
		return
	}
	hasError := false
	for {
		delta, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("  responses decoder error: %v", err)
			hasError = true
			break
		}
		state.Apply(delta, emitter, onUsage)
	}
	if hasError {
		// Do not emit successful completed; terminate without completed to signal failure.
		// We have already emitted added/delta for partial progress; client will see missing completed as failed.
		return
	}
	state.FinalizePendingTools(emitter)
	state.EmitDone(emitter)
	now := time.Now().Unix()
	completed := state.Snapshot("completed", true, &now)
	emitter.emit("response.completed", map[string]any{
		"response": completed,
	})
}

func handleResponses(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	var params map[string]any
	if err := json.Unmarshal(body, &params); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	rawModel, ok := params["model"]
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]string{"message": "model is required", "type": "invalid_request_error"}})
		return
	}
	modelStr, ok := rawModel.(string)
	if !ok || strings.TrimSpace(modelStr) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]string{"message": "model must be a non-empty string", "type": "invalid_request_error"}})
		return
	}
	model := strings.TrimSpace(modelStr)
	params["model"] = model
	isStream, _ := params["stream"].(bool)
	log.Printf("  responses: model=%s stream=%v", model, isStream)

	chat := responsesToChat(params)
	chatModel, _ := chat["model"].(string)
	route := routeModel(chatModel)
	if route == "reject" {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": map[string]string{"message": fmt.Sprintf("model %q is a paid zen model; only free zen models are proxied", chatModel), "type": "invalid_request_error"},
		})
		return
	}
	if route == "zen" {
		zm, ok := resolveZenFreeModel(chatModel)
		if !ok {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": map[string]string{"message": fmt.Sprintf("model %q is not a free zen model", chatModel), "type": "invalid_request_error"},
			})
			return
		}
		sid := requestSessionID(chat, r.Header)
		out := maybeCompact(chat, zm, sid)
		if out.changed {
			log.Printf("  responses zen: %s", out.note)
		}
		resp, _, err := callZenAPI(chat, isStream)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{
				"error": map[string]string{"message": err.Error(), "type": "api_error"},
			})
			return
		}
		defer resp.Body.Close()
		if isStream {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.WriteHeader(http.StatusOK)
			chatStreamToResponses(w, resp, nil, params)
			return
		}
		// aggregated path (both SSE and JSON share same decoder/state)
		state := newResponseState(params)
		dec, err := newResponsesDecoder(resp)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		var aggErr error
		for {
			delta, err := dec.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				aggErr = err
				break
			}
			state.Apply(delta, nil, nil)
		}
		if aggErr != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": aggErr.Error()})
			return
		}
		state.FinalizePendingTools(nil)
		now := time.Now().Unix()
		snap := state.Snapshot("completed", true, &now)
		writeJSON(w, http.StatusOK, snap)
		return
	}

	// cline 上游
	stream := isStream
	if !isStream && modelNeedsStream(normalizeRequestModel(chatModel)) {
		stream = true
	}
	up, acc, err := callClineAPI(chat, stream)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error": map[string]string{"message": err.Error(), "type": "api_error"},
		})
		return
	}
	defer up.Body.Close()

	usageFn := accountUsageFn(acc, chat)
	if isStream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.WriteHeader(http.StatusOK)
		chatStreamToResponses(w, up, usageFn, params)
		return
	}
	// aggregated (SSE or JSON)
	state := newResponseState(params)
	dec, err := newResponsesDecoder(up)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	var aggErr error
	for {
		delta, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			aggErr = err
			break
		}
		state.Apply(delta, nil, usageFn)
	}
	if aggErr != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": aggErr.Error()})
		return
	}
	state.FinalizePendingTools(nil)
	now := time.Now().Unix()
	snap := state.Snapshot("completed", true, &now)
	writeJSON(w, http.StatusOK, snap)
}
