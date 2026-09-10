package app

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testUpstream(body string) *http.Response {
	return &http.Response{Body: io.NopCloser(strings.NewReader(body))}
}

func chatPayloads(t *testing.T, body string) []string {
	t.Helper()
	frames := strings.Split(body, "\n\n")
	var out []string
	for _, f := range frames {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if !strings.HasPrefix(f, "data:") {
			continue
		}
		out = append(out, strings.TrimSpace(strings.TrimPrefix(f, "data:")))
	}
	return out
}

func TestChatStreamStandardDelta(t *testing.T) {
	up := testUpstream("data: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\ndata: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\n")
	rec := httptest.NewRecorder()
	handleStreamResponseWithUsage(rec, up, nil)
	body := rec.Body.String()
	if !strings.Contains(body, "Hello") || !strings.Contains(body, " world") {
		t.Fatalf("standard delta not forwarded: %q", body)
	}
	if got := strings.Count(body, "[DONE]"); got != 1 {
		t.Fatalf("expected exactly 1 [DONE], got %d: %q", got, body)
	}
	if !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Fatalf("expected complete DONE frame suffix, got %q", body)
	}
	for _, p := range chatPayloads(t, body) {
		if p == "[DONE]" || p == "" {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal([]byte(p), &obj); err != nil {
			t.Fatalf("forwarded data payload is not JSON: %q err=%v", p, err)
		}
	}
}

func TestChatStreamMessageFallback(t *testing.T) {
	up := testUpstream("data: {\"id\":\"1\",\"choices\":[{\"index\":0,\"message\":{\"role\":\"assistant\",\"content\":\"Hi\"},\"finish_reason\":\"stop\"}]}\n\n")
	rec := httptest.NewRecorder()
	handleStreamResponseWithUsage(rec, up, nil)
	body := rec.Body.String()
	payloads := chatPayloads(t, body)
	found := false
	for _, p := range payloads {
		if p == "[DONE]" || p == "" {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal([]byte(p), &obj); err != nil {
			t.Fatalf("bad JSON: %v", err)
		}
		choices, _ := obj["choices"].([]any)
		if len(choices) == 0 {
			continue
		}
		ch, _ := choices[0].(map[string]any)
		d, _ := ch["delta"].(map[string]any)
		if d == nil {
			t.Fatalf("message shape not normalized to delta: %q", p)
		}
		if c, _ := d["content"].(string); c == "Hi" {
			found = true
		}
	}
	if !found {
		t.Fatalf("message fallback content not found: %q", body)
	}
}

func TestChatStreamChoiceBodyFallback(t *testing.T) {
	up := testUpstream("data: {\"id\":\"1\",\"choices\":[{\"index\":0,\"content\":\"Hey\",\"finish_reason\":\"stop\"}]}\n\n")
	rec := httptest.NewRecorder()
	handleStreamResponseWithUsage(rec, up, nil)
	body := rec.Body.String()
	payloads := chatPayloads(t, body)
	found := false
	for _, p := range payloads {
		if p == "[DONE]" || p == "" {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal([]byte(p), &obj); err != nil {
			t.Fatalf("bad JSON: %v", err)
		}
		ch, _ := obj["choices"].([]any)[0].(map[string]any)
		d, _ := ch["delta"].(map[string]any)
		if d == nil {
			t.Fatalf("choice-body not normalized to delta: %q", p)
		}
		if c, _ := d["content"].(string); c == "Hey" {
			found = true
		}
	}
	if !found {
		t.Fatalf("choice-body fallback content not found: %q", body)
	}
}

func TestChatStreamUpstreamDoneNotDuplicated(t *testing.T) {
	up := testUpstream("data: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\ndata: [DONE]\n\n")
	rec := httptest.NewRecorder()
	handleStreamResponseWithUsage(rec, up, nil)
	body := rec.Body.String()
	if got := strings.Count(body, "[DONE]"); got != 1 {
		t.Fatalf("expected exactly 1 [DONE], got %d: %q", got, body)
	}
}

func TestChatStreamHeartbeatNotParsedAsJSON(t *testing.T) {
	up := testUpstream(": ping\n\ndata: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"y\"}}]}\n\n")
	rec := httptest.NewRecorder()
	handleStreamResponseWithUsage(rec, up, nil)
	body := rec.Body.String()
	if !strings.Contains(body, ": ping") {
		t.Fatalf("heartbeat comment not forwarded: %q", body)
	}
	if !strings.Contains(body, "\"content\":\"y\"") && !strings.Contains(body, "\"content\": \"y\"") {
		t.Fatalf("delta after heartbeat not forwarded: %q", body)
	}
	if got := strings.Count(body, "[DONE]"); got != 1 {
		t.Fatalf("expected exactly 1 [DONE], got %d: %q", got, body)
	}
}

type responsesEvent struct {
	event string
	data  map[string]any
}

func parseResponsesEvents(t *testing.T, body string) []responsesEvent {
	t.Helper()
	frames := strings.Split(body, "\n\n")
	var out []responsesEvent
	for _, f := range frames {
		f = strings.TrimRight(f, "\r\n")
		if strings.TrimSpace(f) == "" {
			continue
		}
		lines := strings.Split(f, "\n")
		var ev, ds string
		for _, ln := range lines {
			if strings.HasPrefix(ln, "event:") {
				ev = strings.TrimSpace(strings.TrimPrefix(ln, "event:"))
			}
			if strings.HasPrefix(ln, "data:") {
				ds = strings.TrimSpace(strings.TrimPrefix(ln, "data:"))
			}
		}
		if ev == "" {
			continue
		}
		var data map[string]any
		if err := json.Unmarshal([]byte(ds), &data); err != nil {
			t.Fatalf("responses event %s data not JSON: %q err=%v", ev, ds, err)
		}
		out = append(out, responsesEvent{event: ev, data: data})
	}
	return out
}

func TestResponsesCompletedOutputMatchesDeltas(t *testing.T) {
	up := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil)
	events := parseResponsesEvents(t, rec.Body.String())
	var deltas strings.Builder
	var completed map[string]any
	for _, e := range events {
		if e.event == "response.output_text.delta" {
			if d, _ := e.data["delta"].(string); d != "" {
				deltas.WriteString(d)
			}
		}
		if e.event == "response.completed" {
			if r, ok := e.data["response"].(map[string]any); ok {
				completed = r
			}
		}
	}
	if completed == nil {
		t.Fatalf("missing response.completed")
	}
	outputs, _ := completed["output"].([]any)
	if len(outputs) == 0 {
		t.Fatalf("completed.output is empty")
	}
	msg, _ := outputs[0].(map[string]any)
	content, _ := msg["content"].([]any)
	if len(content) == 0 {
		t.Fatalf("completed message content empty: %v", msg)
	}
	block, _ := content[0].(map[string]any)
	text, _ := block["text"].(string)
	if text != deltas.String() || text != "Hello world" {
		t.Fatalf("completed text %q != deltas %q", text, deltas.String())
	}
	if ot, _ := completed["output_text"].(string); ot != "Hello world" {
		t.Fatalf("completed output_text %q", ot)
	}
}

func TestResponsesCompletedOutputMessageShape(t *testing.T) {
	up := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"message\":{\"content\":\"Hi there\"},\"finish_reason\":\"stop\"}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil)
	events := parseResponsesEvents(t, rec.Body.String())
	var completed map[string]any
	for _, e := range events {
		if e.event == "response.completed" {
			if r, ok := e.data["response"].(map[string]any); ok {
				completed = r
			}
		}
	}
	if completed == nil {
		t.Fatalf("missing response.completed")
	}
	outputs, _ := completed["output"].([]any)
	if len(outputs) == 0 {
		t.Fatalf("message-shape completed.output is empty")
	}
}

func TestChatStreamMultiDataLinesJoined(t *testing.T) {
	up := testUpstream("data: {\"id\":\"1\",\ndata: \"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n")
	rec := httptest.NewRecorder()
	handleStreamResponseWithUsage(rec, up, nil)
	body := rec.Body.String()
	payloads := chatPayloads(t, body)
	dataCount := 0
	for _, p := range payloads {
		if p == "[DONE]" || p == "" {
			continue
		}
		dataCount++
		var obj map[string]any
		if err := json.Unmarshal([]byte(p), &obj); err != nil {
			t.Fatalf("multi-data joined payload not JSON: %q err=%v", p, err)
		}
	}
	if dataCount != 1 {
		t.Fatalf("expected multi-data lines joined into 1 event, got %d: %q", dataCount, body)
	}
	if !strings.Contains(body, "Hi") {
		t.Fatalf("joined content missing: %q", body)
	}
	if got := strings.Count(body, "[DONE]"); got != 1 {
		t.Fatalf("expected 1 [DONE], got %d: %q", got, body)
	}
}

func TestChatStreamSameEventCommentFraming(t *testing.T) {
	up := testUpstream(": note-same-event\ndata: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n")
	rec := httptest.NewRecorder()
	handleStreamResponseWithUsage(rec, up, nil)
	body := rec.Body.String()
	if !strings.Contains(body, ": note-same-event") {
		t.Fatalf("same-event comment not preserved: %q", body)
	}
	frames := strings.Split(body, "\n\n")
	found := false
	for _, f := range frames {
		if strings.Contains(f, ": note-same-event") && strings.Contains(f, "Hi") {
			found = true
		}
	}
	if !found {
		t.Fatalf("comment and data not in same output event: %q", body)
	}
	if got := strings.Count(body, "[DONE]"); got != 1 {
		t.Fatalf("expected 1 [DONE], got %d: %q", got, body)
	}
}

func TestChatStreamEventFieldsPreserved(t *testing.T) {
	up := testUpstream("event: message\nid: 7\ndata: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}\n\n")
	rec := httptest.NewRecorder()
	handleStreamResponseWithUsage(rec, up, nil)
	body := rec.Body.String()
	if !strings.Contains(body, "event: message") || !strings.Contains(body, "id: 7") {
		t.Fatalf("event/id fields not preserved: %q", body)
	}
	frames := strings.Split(body, "\n\n")
	found := false
	for _, f := range frames {
		if strings.Contains(f, "event: message") && strings.Contains(f, "Hi") {
			found = true
		}
	}
	if !found {
		t.Fatalf("event association broken: %q", body)
	}
}

func TestChatStreamEOFUnclosedEvent(t *testing.T) {
	up := testUpstream("data: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"tail\"}}]}")
	rec := httptest.NewRecorder()
	handleStreamResponseWithUsage(rec, up, nil)
	body := rec.Body.String()
	if !strings.Contains(body, "tail") {
		t.Fatalf("EOF unclosed event not forwarded: %q", body)
	}
	if got := strings.Count(body, "[DONE]"); got != 1 {
		t.Fatalf("expected 1 [DONE] after EOF, got %d: %q", got, body)
	}
	if !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Fatalf("expected DONE suffix, got %q", body)
	}
}

func TestChatStreamNonJSONDataPassthrough(t *testing.T) {
	up := testUpstream("data: not-json-payload\n\n")
	rec := httptest.NewRecorder()
	handleStreamResponseWithUsage(rec, up, nil)
	body := rec.Body.String()
	if !strings.Contains(body, "data: not-json-payload") {
		t.Fatalf("non-JSON data not passed through: %q", body)
	}
	if got := strings.Count(body, "[DONE]"); got != 1 {
		t.Fatalf("expected 1 [DONE], got %d: %q", got, body)
	}
}

func completedOf(t *testing.T, body string) map[string]any {
	t.Helper()
	events := parseResponsesEvents(t, body)
	for _, e := range events {
		if e.event == "response.completed" {
			if r, ok := e.data["response"].(map[string]any); ok {
				return r
			}
		}
	}
	t.Fatalf("missing response.completed: %q", body)
	return nil
}

func TestResponsesEmptyStillHasMessage(t *testing.T) {
	up := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil)
	completed := completedOf(t, rec.Body.String())
	outputs, _ := completed["output"].([]any)
	if len(outputs) != 0 {
		t.Fatalf("empty should have 0 outputs (no fake message), got %d: %v", len(outputs), completed["output"])
	}
	if ot, _ := completed["output_text"].(string); ot != "" {
		t.Fatalf("empty output_text should be \"\", got %q", ot)
	}
}

func TestResponsesToolOnlyStillHasMessage(t *testing.T) {
	up := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"get_weather\",\"arguments\":\"{\\\"city\\\":\\\"SF\\\"}\"}}]}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"finish_reason\":\"tool_calls\"}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil)
	completed := completedOf(t, rec.Body.String())
	outputs, _ := completed["output"].([]any)
	if len(outputs) != 1 {
		t.Fatalf("tool-only should have 1 function_call at index 0 (no fake message), got %d: %v", len(outputs), completed["output"])
	}
	fc, _ := outputs[0].(map[string]any)
	if fc["type"] != "function_call" || fc["name"] != "get_weather" {
		t.Fatalf("single output should be function_call get_weather: %v", fc)
	}
	if idx := getOutputIndex(t, rec.Body.String(), fc["id"].(string)); idx != 0 {
		t.Fatalf("tool-only first index should be 0, got %d", idx)
	}
}

// helper to find output_index for a given item id from SSE
func getOutputIndex(t *testing.T, raw string, itemID string) int {
	t.Helper()
	evs := parseResponsesEvents(t, raw)
	for _, e := range evs {
		if e.event == "response.output_item.added" {
			if item, ok := e.data["item"].(map[string]any); ok {
				if id, _ := item["id"].(string); id == itemID {
					if oi, ok := e.data["output_index"].(float64); ok {
						return int(oi)
					}
				}
			}
		}
	}
	return -1
}

func TestResponsesToolCallDeltaStandard(t *testing.T) {
	up := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\"}}]}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"x\\\"}\"}}]}}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil)
	body := rec.Body.String()
	events := parseResponsesEvents(t, body)
	deltaCount := 0
	for _, e := range events {
		if e.event == "response.function_call_arguments.delta" {
			deltaCount++
			if _, ok := e.data["delta"].(string); !ok {
				t.Fatalf("delta missing string delta: %v", e.data)
			}
		}
	}
	if deltaCount == 0 {
		t.Fatalf("expected at least one function_call_arguments.delta: %q", body)
	}
	completed := completedOf(t, body)
	outputs, _ := completed["output"].([]any)
	if len(outputs) != 1 {
		t.Fatalf("expected 1 call (no fake message), got %v", completed["output"])
	}
	if fc, _ := outputs[0].(map[string]any); fc["type"] != "function_call" {
		t.Fatalf("expected function_call, got %v", outputs[0])
	}
}

func TestResponsesTwoInterleavedToolCalls(t *testing.T) {
	up := testUpstream(
		"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_A\",\"function\":{\"name\":\"funcA\",\"arguments\":\"{\\\"a\\\":\"}}]}}]}\n\n" +
			"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":1,\"id\":\"call_B\",\"function\":{\"name\":\"funcB\",\"arguments\":\"{\\\"b\\\":\"}}]}}]}\n\n" +
			"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"1}\"}}]}}]}\n\n" +
			"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":1,\"function\":{\"arguments\":\"2}\"}}]}}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil)
	body := rec.Body.String()
	events := parseResponsesEvents(t, body)
	completed := completedOf(t, body)
	outputs, _ := completed["output"].([]any)
	if len(outputs) != 2 {
		t.Fatalf("expected 2 function_calls (no fake message), got %d: %v", len(outputs), completed["output"])
	}
	fcA, _ := outputs[0].(map[string]any)
	fcB, _ := outputs[1].(map[string]any)
	if fcA["type"] != "function_call" || fcB["type"] != "function_call" {
		t.Fatalf("outputs should both be function_call: %v %v", fcA, fcB)
	}
	// order by output_index should preserve creation order (0->funcA,1->funcB)
	if fcA["name"] != "funcA" || fcB["name"] != "funcB" {
		t.Fatalf("function order/names wrong: %v %v", fcA, fcB)
	}
	if a, _ := fcA["arguments"].(string); a != "{\"a\":1}" {
		t.Fatalf("funcA arguments crossed: %q", a)
	}
	if b, _ := fcB["arguments"].(string); b != "{\"b\":2}" {
		t.Fatalf("funcB arguments crossed: %q", b)
	}
	if c, _ := fcA["call_id"].(string); c != "call_A" {
		t.Fatalf("funcA call_id wrong: %q", c)
	}
	if c, _ := fcB["call_id"].(string); c != "call_B" {
		t.Fatalf("funcB call_id wrong: %q", c)
	}
	// added/delta/done 必须对应正确调用、不能合并, now indices 0 and 1
	addedIdx := map[float64]int{}
	deltaIdx := map[float64]int{}
	doneIdx := map[float64]int{}
	for _, e := range events {
		switch e.event {
		case "response.output_item.added":
			if item, ok := e.data["item"].(map[string]any); ok && item["type"] == "function_call" {
				oi, _ := e.data["output_index"].(float64)
				addedIdx[oi]++
			}
		case "response.function_call_arguments.delta":
			oi, _ := e.data["output_index"].(float64)
			deltaIdx[oi]++
		case "response.function_call_arguments.done":
			oi, _ := e.data["output_index"].(float64)
			doneIdx[oi]++
		}
	}
	if addedIdx[0] != 1 || addedIdx[1] != 1 {
		t.Fatalf("added should be one per call (idx0, idx1): %v", addedIdx)
	}
	if len(deltaIdx) != 2 || deltaIdx[0] == 0 || deltaIdx[1] == 0 {
		t.Fatalf("delta should exist for both calls separately: %v", deltaIdx)
	}
	if doneIdx[0] != 1 || doneIdx[1] != 1 {
		t.Fatalf("done should be one per call: %v", doneIdx)
	}
}
