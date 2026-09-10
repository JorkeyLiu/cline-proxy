package app

import (
	"bytes"
	"cline-go-proxy/internal/kit"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// helper already defined in stream_fix_test.go: testUpstream, parseResponsesEvents, completedOf, chatPayloads

func snapshotFromSSE(t *testing.T, body string, reqModel string) map[string]any {
	t.Helper()
	dec, err := newResponsesDecoder(testUpstream(body))
	if err != nil {
		t.Fatalf("decoder init err: %v", err)
	}
	state := newResponseState(map[string]any{"model": reqModel})
	for {
		d, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decoder err: %v", err)
		}
		state.Apply(d, nil, nil)
	}
	state.FinalizePendingTools(nil)
	now := time.Now().Unix()
	return state.Snapshot("completed", true, &now)
}

func TestCollectResponsesStream_ReasoningOnly(t *testing.T) {
	body := "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"think\"}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\" more\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3,\"total_tokens\":5}}\n\n"
	out := snapshotFromSSE(t, body, "m")
	outputs, _ := out["output"].([]any)
	if len(outputs) != 1 {
		t.Fatalf("reasoning-only should have 1 output, got %d: %v", len(outputs), out["output"])
	}
	if typ, _ := outputs[0].(map[string]any)["type"].(string); typ != "reasoning" {
		t.Fatalf("expected reasoning, got %v", outputs[0])
	}
	summary, _ := outputs[0].(map[string]any)["summary"].([]any)
	if len(summary) == 0 {
		t.Fatalf("reasoning summary empty")
	}
	txt, _ := summary[0].(map[string]any)["text"].(string)
	if txt != "think more" {
		t.Fatalf("reasoning text want 'think more' got %q", txt)
	}
	if txt == "" {
		t.Fatalf("reasoning item must be non-empty")
	}
	if usage, ok := out["usage"].(map[string]any); ok && usage != nil {
		if v, _ := usage["input_tokens"].(float64); int(v) != 2 {
			if vi, ok := usage["input_tokens"].(int); !ok || vi != 2 {
				t.Fatalf("input_tokens want 2 got %v", usage["input_tokens"])
			}
		}
	} else {
		if out["usage"] == nil {
			t.Fatalf("usage should be present, got nil")
		}
	}
}

func TestCollectResponsesStream_ReasoningAndContent(t *testing.T) {
	body := "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"r1\"}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello \"}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"world\",\"reasoning_content\":\" r2\"}}]}\n\n"
	out := snapshotFromSSE(t, body, "m")
	outputs, _ := out["output"].([]any)
	if len(outputs) != 2 {
		t.Fatalf("reasoning+content should have 2 outputs, got %d: %v", len(outputs), out["output"])
	}
	hasReasoning, hasMessage := false, false
	for _, o := range outputs {
		m, _ := o.(map[string]any)
		if m["type"] == "reasoning" {
			hasReasoning = true
			summary, _ := m["summary"].([]any)
			if len(summary) == 0 || summary[0].(map[string]any)["text"] == "" {
				t.Fatalf("reasoning should be non-empty")
			}
		}
		if m["type"] == "message" {
			hasMessage = true
			content, _ := m["content"].([]any)
			if len(content) == 0 {
				t.Fatalf("message content empty")
			}
		}
	}
	if !hasReasoning || !hasMessage {
		t.Fatalf("need both reasoning and message, got %v", outputs)
	}
	if typ, _ := outputs[0].(map[string]any)["type"].(string); typ != "reasoning" {
		t.Fatalf("stable order want reasoning first, got %v", outputs[0])
	}
	if typ, _ := outputs[1].(map[string]any)["type"].(string); typ != "message" {
		t.Fatalf("stable order want message second, got %v", outputs[1])
	}
	if ot, _ := out["output_text"].(string); ot != "hello world" {
		t.Fatalf("output_text want 'hello world' got %q", ot)
	}
}

func TestCollectResponsesStream_ReasoningAlias(t *testing.T) {
	body := "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning\":\"alias think\"}}]}\n\n"
	out := snapshotFromSSE(t, body, "m")
	outputs, _ := out["output"].([]any)
	if len(outputs) != 1 || outputs[0].(map[string]any)["type"] != "reasoning" {
		t.Fatalf("alias reasoning should produce reasoning output, got %v", out["output"])
	}
	summary, _ := outputs[0].(map[string]any)["summary"].([]any)
	if txt, _ := summary[0].(map[string]any)["text"].(string); txt != "alias think" {
		t.Fatalf("alias text wrong %q", txt)
	}
	choice := map[string]any{"delta": map[string]any{"content": "hi", "reasoning_content": "should-ignore", "reasoning": "also-ignore"}}
	delta := streamDeltaFromChoice(choice)
	if _, has := delta["reasoning_content"]; has {
		t.Fatalf("Chat delta must not contain reasoning_content")
	}
	if _, has := delta["reasoning"]; has {
		t.Fatalf("Chat delta must not contain reasoning")
	}
	rdelta := responsesDeltaFromChoice(choice)
	if r, _ := rdelta["reasoning_content"].(string); r != "should-ignore" {
		t.Fatalf("Responses delta must capture reasoning_content, got %v", rdelta)
	}
}

func TestCollectResponsesStream_ToolCallsNoRegression(t *testing.T) {
	body := "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"fn\",\"arguments\":\"{\\\"a\\\":1\"}}]}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\",\\\"b\\\":2}\"}}]}}]}\n\n"
	out := snapshotFromSSE(t, body, "m")
	outputs, _ := out["output"].([]any)
	found := false
	for _, o := range outputs {
		m, _ := o.(map[string]any)
		if m["type"] == "function_call" {
			found = true
			if m["name"] != "fn" {
				t.Fatalf("tool name wrong %v", m)
			}
			if args, _ := m["arguments"].(string); args != "{\"a\":1,\"b\":2}" {
				t.Fatalf("tool args want '{\"a\":1,\"b\":2}' got %q", args)
			}
			if cid, _ := m["call_id"].(string); cid != "call_1" {
				t.Fatalf("call_id wrong %q", cid)
			}
		}
	}
	if !found {
		t.Fatalf("tool_call missing in collected output %v", outputs)
	}
}

func TestCollectResponsesStream_OrderStable_Locked(t *testing.T) {
	body := "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"think\"}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"fn\",\"arguments\":\"{}\"}}]}}]}\n\n"
	dec, _ := newResponsesDecoder(testUpstream(body))
	state := newResponseState(map[string]any{"model": "m"})
	for {
		d, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decoder err: %v", err)
		}
		state.Apply(d, nil, nil)
	}
	state.FinalizePendingTools(nil)
	now := time.Now().Unix()
	snap := state.Snapshot("completed", true, &now)
	outputs, _ := snap["output"].([]any)
	if len(outputs) != 3 {
		t.Fatalf("aggregated should have 3 outputs, got %v", outputs)
	}
	if typ, _ := outputs[0].(map[string]any)["type"].(string); typ != "message" {
		t.Fatalf("aggregated first should be message (first appearance content), got %v", outputs[0])
	}
	if typ, _ := outputs[1].(map[string]any)["type"].(string); typ != "reasoning" {
		t.Fatalf("aggregated second should be reasoning, got %v", outputs[1])
	}
	if typ, _ := outputs[2].(map[string]any)["type"].(string); typ != "function_call" {
		t.Fatalf("aggregated third should be function_call, got %v", outputs[2])
	}
	hidden := "_" + "responsesFirstOrder"
	if _, has := snap[hidden]; has {
		t.Fatalf("snapshot must not contain hidden order hint")
	}
	up2 := testUpstream(body)
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up2, nil, map[string]any{"model": "m"})
	completed := completedOf(t, rec.Body.String())
	out2, _ := completed["output"].([]any)
	if len(out2) != 3 {
		t.Fatalf("streaming should have 3 outputs, got %v", out2)
	}
	if typ, _ := out2[0].(map[string]any)["type"].(string); typ != "message" {
		t.Fatalf("streaming first should be message, got %v", out2[0])
	}
	if typ, _ := out2[1].(map[string]any)["type"].(string); typ != "reasoning" {
		t.Fatalf("streaming second should be reasoning, got %v", out2[1])
	}
	if typ, _ := out2[2].(map[string]any)["type"].(string); typ != "function_call" {
		t.Fatalf("streaming third should be function_call, got %v", out2[2])
	}
	for i := range outputs {
		typ1, _ := outputs[i].(map[string]any)["type"].(string)
		typ2, _ := out2[i].(map[string]any)["type"].(string)
		if typ1 != typ2 {
			t.Fatalf("stream vs aggregated mismatch at %d: %s vs %s", i, typ1, typ2)
		}
	}
}

func TestHandleResponses_ForcedStream_Cline_Reasoning(t *testing.T) {
	modelID := "deepseek/deepseek-v4-flash"
	initModelsCache()
	modelsMu.Lock()
	if m, ok := modelsCache[modelID]; ok {
		m.RequiresStream = true
	} else {
		modelsCache[modelID] = &ModelInfo{ID: modelID, Source: "free", Provider: "deepseek", Cost: "free", RequiresStream: true, Status: ModelActive}
	}
	modelsMu.Unlock()

	poolMu.Lock()
	oldPool := pool
	oldPoolCopy := &AccountPool{}
	if pool != nil {
		*oldPoolCopy = *pool
		oldPoolCopy.Accounts = append([]*Account(nil), pool.Accounts...)
		oldPoolCopy.Keys = append([]string(nil), pool.Keys...)
	}
	if pool == nil {
		pool = &AccountPool{Accounts: []*Account{}, Keys: []string{}}
	}
	pool.Keys = []string{"test-key-forced"}
	acc := &Account{
		AccountID:    "acc_test_forced",
		Email:        "test@example.com",
		RefreshToken: "dummy-refresh",
		AccessToken:  "dummy-access",
		ExpiresAt:    time.Now().Add(time.Hour).UnixMilli(),
		Status:       "active",
		CreatedAt:    time.Now(),
	}
	pool.Accounts = []*Account{acc}
	poolMu.Unlock()
	defer func() {
		poolMu.Lock()
		if oldPool != nil {
			pool = oldPool
			if oldPoolCopy != nil && oldPoolCopy.Accounts != nil {
				pool.Accounts = oldPoolCopy.Accounts
				pool.Keys = oldPoolCopy.Keys
			}
		} else {
			pool = nil
		}
		poolMu.Unlock()
	}()

	sseBody := "data: {\"model\":\"" + modelID + "\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"think step\"}}]}\n\n" +
		"data: {\"model\":\"" + modelID + "\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello world\"}}]}\n\n" +
		"data: {\"model\":\"" + modelID + "\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"fn\",\"arguments\":\"{\\\"x\\\":1}\"}}]}}]}\n\n" +
		"data: {\"model\":\"" + modelID + "\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":20,\"total_tokens\":30}}\n\n"
	oldClient := kit.HTTPClient
	kit.HTTPClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if strings.Contains(req.URL.String(), "/chat/completions") {
				return &http.Response{
					StatusCode: 200,
					Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
					Body:       io.NopCloser(strings.NewReader(sseBody)),
				}, nil
			}
			return nil, io.EOF
		}),
	}
	defer func() { kit.HTTPClient = oldClient }()

	reqLogsMu.Lock()
	oldLogs := append([]RequestLog(nil), reqLogs...)
	reqLogs = nil
	reqLogsMu.Unlock()
	oldReqLogsFile := reqLogsFile
	tmpDir := t.TempDir()
	reqLogsFile = tmpDir + "/requests.jsonl"
	defer func() {
		time.Sleep(100 * time.Millisecond)
		reqLogsFile = oldReqLogsFile
		reqLogsMu.Lock()
		reqLogs = oldLogs
		reqLogsMu.Unlock()
	}()

	payload := map[string]any{
		"model":  modelID,
		"input":  "hi",
		"stream": false,
	}
	b, _ := json.Marshal(payload)
	req := httptest.NewRequest("POST", "/v1/responses", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", "test-key-forced")
	rec := httptest.NewRecorder()
	handler := apiKeyMiddleware(handleResponses)
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("handleResponses forced stream want 200 got %d body %q", rec.Code, rec.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response not json: %v body %q", err, rec.Body.String())
	}
	outputs, _ := resp["output"].([]any)
	if len(outputs) == 0 {
		t.Fatalf("forced stream aggregated output empty, want reasoning/message/tool")
	}
	hasReasoning, hasMessage, hasTool := false, false, false
	for _, o := range outputs {
		m, _ := o.(map[string]any)
		switch m["type"] {
		case "reasoning":
			hasReasoning = true
			summary, _ := m["summary"].([]any)
			if len(summary) == 0 {
				t.Fatalf("reasoning summary empty")
			}
			if txt, _ := summary[0].(map[string]any)["text"].(string); txt != "think step" {
				t.Fatalf("reasoning text want 'think step' got %q", txt)
			}
		case "message":
			hasMessage = true
			if ot, _ := resp["output_text"].(string); ot != "hello world" {
				t.Fatalf("output_text want 'hello world' got %q", ot)
			}
		case "function_call":
			hasTool = true
		}
	}
	if !hasReasoning {
		t.Fatalf("forced stream missing reasoning, outputs %v", outputs)
	}
	if !hasMessage {
		t.Fatalf("forced stream missing message, outputs %v", outputs)
	}
	if !hasTool {
		t.Fatalf("forced stream missing tool_call, outputs %v", outputs)
	}
	if usage, ok := resp["usage"].(map[string]any); !ok || usage == nil {
		t.Fatalf("usage missing in forced stream response %v", resp["usage"])
	} else {
		if v, _ := usage["input_tokens"].(float64); int(v) != 10 {
			if vi, ok := usage["input_tokens"].(int); !ok || vi != 10 {
				t.Fatalf("input_tokens want 10 got %v", usage["input_tokens"])
			}
		}
	}
	if typ, _ := outputs[0].(map[string]any)["type"].(string); typ != "reasoning" {
		t.Fatalf("forced stream order: first should be reasoning, got %v", outputs[0])
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
