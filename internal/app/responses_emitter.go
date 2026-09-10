package app

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type responsesEmitter struct {
	w   http.ResponseWriter
	rc  *http.ResponseController
	seq int
}

func newResponsesEmitter(w http.ResponseWriter) *responsesEmitter {
	rc := http.NewResponseController(w)
	return &responsesEmitter{w: w, rc: rc, seq: 0}
}

func (e *responsesEmitter) emit(event string, data map[string]any) {
	data["type"] = event
	data["sequence_number"] = e.seq
	e.seq++
	b, _ := json.Marshal(data)
	fmt.Fprintf(e.w, "event: %s\ndata: %s\n\n", event, string(b))
	_ = e.rc.Flush()
}

func (e *responsesEmitter) emitCreatedAndInProgress(state *ResponseState, model string) {
	// model override already in state, but snapshot will use state.model if empty we pass model
	// ensure state model reflects current if empty
	if state.model == "" && model != "" {
		state.model = model
	}
	if state.model != "" {
		state.modelFrozen = true
	}
	createdResp := state.Snapshot("in_progress", false, nil)
	e.emit("response.created", map[string]any{
		"response": createdResp,
	})
	inProgResp := state.Snapshot("in_progress", false, nil)
	e.emit("response.in_progress", map[string]any{
		"response": inProgResp,
	})
}
