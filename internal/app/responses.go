package app

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// ============================================================================
// OpenAI Responses API (/v1/responses) -> chat/completions 转换
// 支持 Cursor 等客户端直连反代,无需 opencode CLI
// ============================================================================

// responsesToChat 将 Responses 请求体转换为 chat.completions 请求体
func responsesToChat(body map[string]any) map[string]any {
	out := map[string]any{}
	if m, ok := body["model"].(string); ok {
		out["model"] = m
	}
	if s, ok := body["stream"].(bool); ok {
		out["stream"] = s
	}
	if mt, ok := body["max_output_tokens"].(float64); ok {
		out["max_tokens"] = int(mt)
	}
	for _, k := range []string{"temperature", "top_p", "stop", "seed", "user", "metadata", "logit_bias"} {
		if v, ok := body[k]; ok {
			out[k] = v
		}
	}
	if instr, ok := body["instructions"].(string); ok && instr != "" {
		out["messages"] = append([]any{map[string]any{"role": "system", "content": instr}}, responsesInputToMessages(body["input"])...)
	} else {
		out["messages"] = responsesInputToMessages(body["input"])
	}
	if tools, ok := body["tools"].([]any); ok {
		out["tools"] = responsesToolsToChat(tools)
	}
	if tc, ok := body["tool_choice"]; ok {
		out["tool_choice"] = tc
	}
	return out
}

func responsesInputToMessages(input any) []any {
	var msgs []any
	switch v := input.(type) {
	case string:
		msgs = append(msgs, map[string]any{"role": "user", "content": v})
	case []any:
		for _, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			switch m["type"] {
			case "message":
				role, _ := m["role"].(string)
				if role == "" {
					role = "user"
				}
				msgs = append(msgs, map[string]any{"role": role, "content": stringifyResponsesContent(m["content"])})
			case "function_call":
				callID, _ := m["call_id"].(string)
				if callID == "" {
					callID, _ = m["id"].(string)
				}
				name, _ := m["name"].(string)
				args := ""
				switch a := m["arguments"].(type) {
				case string:
					args = a
				case map[string]any:
					if b, err := json.Marshal(a); err == nil {
						args = string(b)
					}
				}
				msgs = append(msgs, map[string]any{
					"role":       "assistant",
					"content":    "",
					"tool_calls": []any{map[string]any{"id": callID, "type": "function", "function": map[string]any{"name": name, "arguments": args}}},
				})
			case "function_call_output":
				callID, _ := m["call_id"].(string)
				if callID == "" {
					callID, _ = m["id"].(string)
				}
				output := ""
				switch o := m["output"].(type) {
				case string:
					output = o
				case map[string]any:
					if b, err := json.Marshal(o); err == nil {
						output = string(b)
					}
				}
				msgs = append(msgs, map[string]any{"role": "tool", "content": output, "tool_call_id": callID})
			case "reasoning":
				// 忽略 Reasoning 输入项(无法映射到 chat 输入)
			}
		}
	}
	return msgs
}

func stringifyResponsesContent(content any) string {
	switch v := content.(type) {
	case string:
		return v
	case []any:
		parts := []string{}
		for _, block := range v {
			if b, ok := block.(map[string]any); ok {
				if t, ok := b["text"].(string); ok {
					parts = append(parts, t)
				}
			}
		}
		return strings.Join(parts, "\n")
	}
	return ""
}

func responsesToolsToChat(tools []any) []any {
	out := make([]any, 0, len(tools))
	for _, t := range tools {
		tm, ok := t.(map[string]any)
		if !ok {
			continue
		}
		if tm["type"] == "function" {
			fn := map[string]any{}
			if n, ok := tm["name"].(string); ok {
				fn["name"] = n
			}
			if d, ok := tm["description"].(string); ok {
				fn["description"] = d
			}
			if p, ok := tm["parameters"].(map[string]any); ok {
				fn["parameters"] = p
			}
			out = append(out, map[string]any{"type": "function", "function": fn})
		}
	}
	return out
}

// ============ 非流式响应转换 ============

// chatToResponses chat.completions 响应 -> Responses 响应
func chatToResponses(chat map[string]any) map[string]any {
	resp := map[string]any{
		"id":          "resp_" + fmt.Sprintf("%x", time.Now().UnixMilli()),
		"object":      "response",
		"created_at":  time.Now().Unix(),
		"status":      "completed",
		"model":       chat["model"],
		"output":      []any{},
		"output_text": "",
	}
	choices, _ := chat["choices"].([]any)
	outputs := []any{}
	var outputText strings.Builder
	if len(choices) > 0 {
		if ch, ok := choices[0].(map[string]any); ok {
			msg, _ := ch["message"].(map[string]any)
			if msg == nil {
				msg, _ = ch["delta"].(map[string]any)
			}
			content := []any{}
			if c, ok := msg["content"].(string); ok && c != "" {
				outputText.WriteString(c)
				content = append(content, map[string]any{"type": "output_text", "text": c, "annotations": []any{}})
			}
			msgOut := map[string]any{
				"type":        "message",
				"id":          "msg_" + fmt.Sprintf("%x", time.Now().UnixMilli()),
				"status":      "completed",
				"role":        "assistant",
				"content":     content,
				"output_text": outputText.String(),
			}
			outputs = append(outputs, msgOut)

			if tc, ok := msg["tool_calls"].([]any); ok {
				for _, c := range tc {
					if cm, ok := c.(map[string]any); ok {
						fn, _ := cm["function"].(map[string]any)
						callID, _ := cm["id"].(string)
						if callID == "" {
							callID = fmt.Sprintf("fc_%x", time.Now().UnixNano())
						}
						name := ""
						args := ""
						if fn != nil {
							name, _ = fn["name"].(string)
							if a, ok := fn["arguments"].(string); ok {
								args = a
							}
						}
						outputs = append(outputs, map[string]any{
							"type":      "function_call",
							"id":        "fc_" + fmt.Sprintf("%x", time.Now().UnixMilli()),
							"call_id":   callID,
							"name":      name,
							"arguments": args,
							"status":    "completed",
						})
					}
				}
			}
		}
	}
	// 始终含一个 message output：空文本/无 choices 时 content 可为空、output_text 为 ""。
	if len(outputs) == 0 {
		outputs = append(outputs, map[string]any{
			"type":        "message",
			"id":          "msg_" + fmt.Sprintf("%x", time.Now().UnixMilli()),
			"status":      "completed",
			"role":        "assistant",
			"content":     []any{},
			"output_text": "",
		})
	}
	resp["output"] = outputs
	resp["output_text"] = outputText.String()
	if u, ok := chat["usage"].(map[string]any); ok {
		details := map[string]any{}
		if pd, ok := u["prompt_tokens_details"].(map[string]any); ok {
			details["cached_tokens"] = pd["cached_tokens"]
		}
		od := map[string]any{}
		if rd, ok := u["reasoning_tokens"]; ok {
			od["reasoning_tokens"] = rd
		}
		resp["usage"] = map[string]any{
			"input_tokens":          u["prompt_tokens"],
			"input_tokens_details":  details,
			"output_tokens":         u["completion_tokens"],
			"output_tokens_details": od,
			"total_tokens":          u["total_tokens"],
		}
	}
	return resp
}

// ============ 流式响应转换 (Responses SSE) ============

type responsesSSEWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
	msgID   string
	respID  string
}

func newResponsesSSE(w http.ResponseWriter) *responsesSSEWriter {
	f, _ := w.(http.Flusher)
	return &responsesSSEWriter{w: w, flusher: f, msgID: "msg_" + fmt.Sprintf("%x", time.Now().UnixMilli()), respID: "resp_" + fmt.Sprintf("%x", time.Now().UnixMilli())}
}

func (s *responsesSSEWriter) event(event string, data any) {
	b, _ := json.Marshal(data)
	fmt.Fprintf(s.w, "event: %s\ndata: %s\n\n", event, string(b))
	if s.flusher != nil {
		s.flusher.Flush()
	}
}

// chatStreamToResponses 将上游 chat.completions SSE 流转换为 Responses SSE 流
func chatStreamToResponses(w http.ResponseWriter, upstream *http.Response, onUsage func(map[string]any)) {
	model := ""
	// 开场
	s := newResponsesSSE(w)
	s.event("response.created", map[string]any{
		"type": "response.created",
		"response": map[string]any{
			"id":         s.respID,
			"object":     "response",
			"created_at": time.Now().Unix(),
			"status":     "in_progress",
			"model":      "",
			"output":     []any{},
		},
	})
	s.event("response.in_progress", map[string]any{"type": "response.in_progress", "response": map[string]any{"id": s.respID}})

	textEmitted := false
	type respToolCall struct {
		idx         int
		callID      string
		name        string
		args        strings.Builder
		added       bool
		fcID        string
		outputIndex int
	}
	toolByIndex := map[int]*respToolCall{}
	var toolOrder []*respToolCall
	respFCID := func(tc *respToolCall) string {
		if tc.fcID != "" {
			return tc.fcID
		}
		if tc.name != "" {
			tc.fcID = "fc_" + tc.name
		} else if tc.callID != "" {
			tc.fcID = "fc_" + tc.callID
		} else {
			tc.fcID = "fc_unknown"
		}
		return tc.fcID
	}
	var outText strings.Builder

	reader := bufio.NewReader(upstream.Body)
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			line = strings.TrimRight(line, "\r\n")
			if strings.HasPrefix(line, "data:") {
				payload := strings.TrimSpace(line[5:])
				if payload == "" || payload == "[DONE]" {
					continue
				}
				var obj map[string]any
				if json.Unmarshal([]byte(payload), &obj) != nil {
					continue
				}
				if data, ok := obj["data"]; ok {
					if d, ok := data.(map[string]any); ok {
						obj = d
					}
				}
				if m, ok := obj["model"].(string); ok && m != "" {
					model = m
				}
				if onUsage != nil {
					if u, ok := obj["usage"].(map[string]any); ok && len(u) > 0 {
						onUsage(u)
					}
				}
				choices, _ := obj["choices"].([]any)
				if len(choices) == 0 {
					continue
				}
				ch, _ := choices[0].(map[string]any)
				if ch == nil {
					continue
				}
				delta := streamDeltaFromChoice(ch)
				// 文本
				if c, ok := delta["content"].(string); ok && c != "" {
					if !textEmitted {
						textEmitted = true
						s.event("response.output_item.added", map[string]any{
							"type":         "response.output_item.added",
							"output_index": 0,
							"item":         map[string]any{"id": s.msgID, "type": "message", "role": "assistant", "status": "in_progress", "content": []any{}},
						})
						s.event("response.content_part.added", map[string]any{
							"type":          "response.content_part.added",
							"item_id":       s.msgID,
							"output_index":  0,
							"content_index": 0,
							"part":          map[string]any{"type": "output_text", "text": "", "annotations": []any{}},
						})
					}
					outText.WriteString(c)
					s.event("response.output_text.delta", map[string]any{
						"type":          "response.output_text.delta",
						"item_id":       s.msgID,
						"output_index":  0,
						"content_index": 0,
						"delta":         c,
					})
				}
				// 推理
				if r, ok := delta["reasoning_content"].(string); ok && r != "" {
					s.event("response.reasoning_summary_text.delta", map[string]any{
						"type":          "response.reasoning_summary_text.delta",
						"item_id":       s.msgID,
						"output_index":  0,
						"content_index": 0,
						"delta":         r,
					})
				}
				// 工具调用：按 tool_calls[].index 分别累积 id/call_id/name/arguments，
				// 支持多调用与交错参数分片；各调用的 added/delta/done 必须对应正确调用。
				if tc, ok := delta["tool_calls"].([]any); ok {
					for _, c := range tc {
						cm, ok := c.(map[string]any)
						if !ok {
							continue
						}
						idx := 0
						if f, ok := cm["index"].(float64); ok {
							idx = int(f)
						}
						st, ok := toolByIndex[idx]
						if !ok {
							st = &respToolCall{idx: idx, outputIndex: 1 + len(toolOrder)}
							toolByIndex[idx] = st
							toolOrder = append(toolOrder, st)
						}
						if id, ok := cm["id"].(string); ok && id != "" {
							st.callID = id
						}
						var frag string
						fn, _ := cm["function"].(map[string]any)
						if fn != nil {
							if n, ok := fn["name"].(string); ok && n != "" {
								st.name = n
							}
							if a, ok := fn["arguments"].(string); ok && a != "" {
								frag = a
								st.args.WriteString(a)
							}
						}
						if !st.added && st.name != "" {
							st.added = true
							fcID := respFCID(st)
							s.event("response.output_item.added", map[string]any{
								"type":         "response.output_item.added",
								"output_index": st.outputIndex,
								"item": map[string]any{
									"type":      "function_call",
									"id":        fcID,
									"call_id":   st.callID,
									"name":      st.name,
									"arguments": "",
									"status":    "in_progress",
								},
							})
						}
						if frag != "" && st.added {
							s.event("response.function_call_arguments.delta", map[string]any{
								"type":         "response.function_call_arguments.delta",
								"item_id":      respFCID(st),
								"output_index": st.outputIndex,
								"delta":        frag,
							})
						}
					}
				}
			}
		}
		if err != nil {
			break
		}
	}

	// 收尾
	if textEmitted {
		s.event("response.output_text.done", map[string]any{"type": "response.output_text.done", "item_id": s.msgID, "output_index": 0, "content_index": 0, "text": outText.String()})
		s.event("response.content_part.done", map[string]any{"type": "response.content_part.done", "item_id": s.msgID, "output_index": 0, "content_index": 0, "part": map[string]any{"type": "output_text", "text": outText.String(), "annotations": []any{}}})
		s.event("response.output_item.done", map[string]any{"type": "response.output_item.done", "output_index": 0, "item": map[string]any{"id": s.msgID, "type": "message", "role": "assistant", "status": "completed", "content": []any{map[string]any{"type": "output_text", "text": outText.String(), "annotations": []any{}}}}})
	}
	for _, st := range toolOrder {
		if !st.added {
			st.added = true
			fcID := respFCID(st)
			s.event("response.output_item.added", map[string]any{
				"type":         "response.output_item.added",
				"output_index": st.outputIndex,
				"item": map[string]any{
					"type":      "function_call",
					"id":        fcID,
					"call_id":   st.callID,
					"name":      st.name,
					"arguments": "",
					"status":    "in_progress",
				},
			})
		}
		fcID := respFCID(st)
		s.event("response.function_call_arguments.done", map[string]any{"type": "response.function_call_arguments.done", "item_id": fcID, "output_index": st.outputIndex, "arguments": st.args.String()})
		s.event("response.output_item.done", map[string]any{"type": "response.output_item.done", "output_index": st.outputIndex, "item": map[string]any{"type": "function_call", "id": fcID, "call_id": st.callID, "name": st.name, "arguments": st.args.String(), "status": "completed"}})
	}
	// 回填本次累积的 output：始终先放一个 message（空文本时 content 可为空、output_text 为 ""），
	// 然后按上游顺序附加 function_call outputs，与非流式 chatToResponses 结构对齐。
	outputs := []any{}
	textContent := []any{}
	if outText.String() != "" {
		textContent = append(textContent, map[string]any{"type": "output_text", "text": outText.String(), "annotations": []any{}})
	}
	outputs = append(outputs, map[string]any{
		"type":        "message",
		"id":          s.msgID,
		"status":      "completed",
		"role":        "assistant",
		"content":     textContent,
		"output_text": outText.String(),
	})
	for _, st := range toolOrder {
		outputs = append(outputs, map[string]any{
			"type":      "function_call",
			"id":        respFCID(st),
			"call_id":   st.callID,
			"name":      st.name,
			"arguments": st.args.String(),
			"status":    "completed",
		})
	}
	s.event("response.completed", map[string]any{
		"type": "response.completed",
		"response": map[string]any{
			"id":          s.respID,
			"object":      "response",
			"created_at":  time.Now().Unix(),
			"status":      "completed",
			"model":       model,
			"output":      outputs,
			"output_text": outText.String(),
			"usage":       map[string]any{"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
		},
	})
}

// ============ /v1/responses 入口 ============

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
	model, _ := params["model"].(string)
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
			chatStreamToResponses(w, resp, nil)
			return
		}
		var raw map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, chatToResponses(raw))
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
		chatStreamToResponses(w, up, usageFn)
		return
	}
	if stream {
		out, err := collectStreamResponse(up)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		if u, ok := out["usage"].(map[string]any); ok && len(u) > 0 {
			usageFn(u)
		}
		writeJSON(w, http.StatusOK, chatToResponses(out))
		return
	}
	var raw map[string]any
	if err := json.NewDecoder(up.Body).Decode(&raw); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	if u, ok := raw["usage"].(map[string]any); ok && len(u) > 0 {
		usageFn(u)
	}
	out := raw
	if data, ok := raw["data"]; ok {
		if d, ok := data.(map[string]any); ok {
			out = d
		}
	}
	writeJSON(w, http.StatusOK, chatToResponses(out))
}
