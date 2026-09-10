package app

import (
	"bytes"
	"cline-go-proxy/internal/kit"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// Task 1: model freeze alias vs canonical
func TestResponses_ModelFrozen_AliasVsCanonical(t *testing.T) {
	reqModel := "alias-model-xyz"
	canonical := "canonical-model-xyz"
	body := "data: {\"model\":\"" + canonical + "\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"model\":\"" + canonical + "\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" there\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n"
	up := testUpstream(body)
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil, map[string]any{"model": reqModel})
	raw := rec.Body.String()
	events := parseResponsesEvents(t, raw)
	var models []string
	for _, e := range events {
		if e.event == "response.created" || e.event == "response.in_progress" || e.event == "response.completed" {
			if r, ok := e.data["response"].(map[string]any); ok {
				m, _ := r["model"].(string)
				models = append(models, e.event+":"+m)
				if m != reqModel {
					t.Fatalf("stream %s model want %q got %q (canonical %q must be frozen)", e.event, reqModel, m, canonical)
				}
				if e.event == "response.completed" {
					if s, _ := r["status"].(string); s != "completed" {
						t.Fatalf("completed status want completed got %q", s)
					}
				} else {
					if s, _ := r["status"].(string); s != "in_progress" {
						t.Fatalf("%s status want in_progress got %q", e.event, s)
					}
				}
			}
		}
	}
	if len(models) != 3 {
		t.Fatalf("expected 3 model snapshots, got %v", models)
	}
	if fails := strictValidateResponsesContract(t, raw); len(fails) > 0 {
		t.Fatalf("model frozen stream strict failed: %v", fails)
	}
	dec, _ := newResponsesDecoder(testUpstream(body))
	st := newResponseState(map[string]any{"model": reqModel})
	for {
		d, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decoder err: %v", err)
		}
		st.Apply(d, nil, nil)
	}
	st.FinalizePendingTools(nil)
	now := time.Now().Unix()
	snap := st.Snapshot("completed", true, &now)
	if m, _ := snap["model"].(string); m != reqModel {
		t.Fatalf("non-stream snapshot model want %q got %q", reqModel, m)
	}
	if s, _ := snap["status"].(string); s != "completed" {
		t.Fatalf("non-stream status want completed got %q", s)
	}
	dec2, _ := newResponsesDecoder(testUpstream(body))
	st2 := newResponseState(map[string]any{"model": ""})
	for {
		d, err := dec2.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("decoder err2: %v", err)
		}
		st2.Apply(d, nil, nil)
	}
	st2.FinalizePendingTools(nil)
	now2 := time.Now().Unix()
	snap2 := st2.Snapshot("completed", true, &now2)
	if m, _ := snap2["model"].(string); m != canonical {
		t.Fatalf("helper empty model should adopt canonical %q got %q", canonical, m)
	}
}

// Task 2: tool identity freeze
func TestResponses_ToolIdentity_LateID_Frozen(t *testing.T) {
	body := "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"myfn\",\"arguments\":\"{\\\"a\\\":1}\"}}]}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_real\",\"function\":{\"name\":\"myfn\",\"arguments\":\"\"}}]}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\",\\\"b\\\":2}\"}}]}}]}\n\n"
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, testUpstream(body), nil, map[string]any{"model": "m"})
	raw := rec.Body.String()
	if fails := strictValidateResponsesContract(t, raw); len(fails) > 0 {
		t.Fatalf("lateID strict failed: %v", fails)
	}
	events := parseResponsesEvents(t, raw)
	var addedID, addedCallID, addedName string
	var addedOI int
	for _, e := range events {
		if e.event == "response.output_item.added" {
			if item, _ := e.data["item"].(map[string]any); item["type"] == "function_call" {
				addedID, _ = item["id"].(string)
				addedCallID, _ = item["call_id"].(string)
				addedName, _ = item["name"].(string)
				addedOI = int(e.data["output_index"].(float64))
			}
		}
	}
	if addedID == "" || addedCallID == "" {
		t.Fatalf("missing added tool identity")
	}
	if addedCallID == "call_real" {
		t.Fatalf("late ID should be ignored, added call_id must not be call_real, got %q", addedCallID)
	}
	if addedName != "myfn" {
		t.Fatalf("added name want myfn got %q", addedName)
	}
	var doneID, doneCallID, doneName string
	for _, e := range events {
		if e.event == "response.output_item.done" {
			if item, _ := e.data["item"].(map[string]any); item["type"] == "function_call" {
				if oi, _ := e.data["output_index"].(float64); int(oi) == addedOI {
					doneID, _ = item["id"].(string)
					doneCallID, _ = item["call_id"].(string)
					doneName, _ = item["name"].(string)
				}
			}
		}
	}
	if doneID != addedID || doneCallID != addedCallID || doneName != addedName {
		t.Fatalf("done identity mismatch: added id=%q call=%q name=%q vs done id=%q call=%q name=%q", addedID, addedCallID, addedName, doneID, doneCallID, doneName)
	}
	completed := completedOf(t, raw)
	outputs, _ := completed["output"].([]any)
	var compID, compCallID, compName string
	for _, o := range outputs {
		if m, _ := o.(map[string]any); m["type"] == "function_call" {
			compID, _ = m["id"].(string)
			compCallID, _ = m["call_id"].(string)
			compName, _ = m["name"].(string)
		}
	}
	if compID != addedID || compCallID != addedCallID || compName != addedName {
		t.Fatalf("completed identity mismatch: added %q %q %q vs completed %q %q %q", addedID, addedCallID, addedName, compID, compCallID, compName)
	}
}

func TestResponses_ToolIdentity_ConflictingName_Frozen(t *testing.T) {
	body := "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"first\",\"arguments\":\"{}\"}}]}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"second\"}}]}}]}\n\n"
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, testUpstream(body), nil, map[string]any{"model": "m"})
	raw := rec.Body.String()
	if fails := strictValidateResponsesContract(t, raw); len(fails) > 0 {
		t.Fatalf("conflicting name strict failed: %v", fails)
	}
	events := parseResponsesEvents(t, raw)
	var addedName string
	var addedID string
	var addedOI int
	for _, e := range events {
		if e.event == "response.output_item.added" {
			if item, _ := e.data["item"].(map[string]any); item["type"] == "function_call" {
				addedName, _ = item["name"].(string)
				addedID, _ = item["id"].(string)
				addedOI = int(e.data["output_index"].(float64))
			}
		}
	}
	if addedName != "first" {
		t.Fatalf("conflicting name: added should be first, got %q", addedName)
	}
	var doneName string
	var doneID string
	for _, e := range events {
		if e.event == "response.output_item.done" {
			if item, _ := e.data["item"].(map[string]any); item["type"] == "function_call" && int(e.data["output_index"].(float64)) == addedOI {
				doneName, _ = item["name"].(string)
				doneID, _ = item["id"].(string)
			}
		}
	}
	if doneName != "first" || doneID != addedID {
		t.Fatalf("done identity should remain first, got name %q id %q vs added %q %q", doneName, doneID, addedName, addedID)
	}
	completed := completedOf(t, raw)
	outputs, _ := completed["output"].([]any)
	for _, o := range outputs {
		if m, _ := o.(map[string]any); m["type"] == "function_call" {
			if n, _ := m["name"].(string); n != "first" {
				t.Fatalf("completed name should be first, got %q", n)
			}
			if id, _ := m["id"].(string); id != addedID {
				t.Fatalf("completed id mismatch")
			}
		}
	}
}

// Task 5: decoder multi-data
func TestResponsesDecoder_MultiDataEvent(t *testing.T) {
	payloadPart1 := "{\"model\":\"m\","
	payloadPart2 := "\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"}}]}"
	rawSSE := "data: " + payloadPart1 + "\n" + "data: " + payloadPart2 + "\n\n"
	up := &http.Response{
		Body:   io.NopCloser(strings.NewReader(rawSSE)),
		Header: http.Header{"Content-Type": []string{"text/event-stream"}},
	}
	dec, err := newResponsesDecoder(up)
	if err != nil {
		t.Fatalf("decoder init err: %v", err)
	}
	d, err := dec.Next()
	if err != nil {
		t.Fatalf("multi-data Next err: %v", err)
	}
	if d.Content != "Hi" {
		t.Fatalf("multi-data content want Hi got %q", d.Content)
	}
	if d.Model != "m" {
		t.Fatalf("multi-data model want m got %q", d.Model)
	}
	if _, err := dec.Next(); err != io.EOF {
		t.Fatalf("expected EOF after single event, got %v", err)
	}
}

func TestHandleResponses_MalformedUpstream_HTTPError(t *testing.T) {
	modelID := "malformed-test-model"
	initModelsCache()
	modelsMu.Lock()
	modelsCache[modelID] = &ModelInfo{ID: modelID, Source: "free", Provider: "test", Cost: "free", RequiresStream: false, Status: ModelActive}
	modelsMu.Unlock()
	poolMu.Lock()
	oldPool := pool
	oldKeys := []string(nil)
	if oldPool != nil {
		oldKeys = append([]string(nil), oldPool.Keys...)
	}
	if pool == nil {
		pool = &AccountPool{Accounts: []*Account{}, Keys: []string{}}
	}
	pool.Keys = []string{"test-key-malformed"}
	acc := &Account{
		AccountID:    "acc_malformed",
		Email:        "malformed@test.com",
		RefreshToken: "dummy",
		AccessToken:  "dummy-access",
		ExpiresAt:    time.Now().Add(time.Hour).UnixMilli(),
		Status:       "active",
		CreatedAt:    time.Now(),
	}
	oldAccounts := pool.Accounts
	pool.Accounts = []*Account{acc}
	poolMu.Unlock()
	defer func() {
		poolMu.Lock()
		if oldPool != nil {
			pool.Keys = oldKeys
			pool.Accounts = oldAccounts
		} else {
			pool = nil
		}
		poolMu.Unlock()
		modelsMu.Lock()
		delete(modelsCache, modelID)
		modelsMu.Unlock()
	}()
	oldClient := kit.HTTPClient
	kit.HTTPClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			body := "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}"
			return &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(body)),
			}, nil
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
		time.Sleep(50 * time.Millisecond)
		reqLogsFile = oldReqLogsFile
		reqLogsMu.Lock()
		reqLogs = oldLogs
		reqLogsMu.Unlock()
	}()
	payload := map[string]any{"model": modelID, "input": "hi", "stream": false}
	b, _ := json.Marshal(payload)
	req := httptest.NewRequest("POST", "/v1/responses", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", "test-key-malformed")
	rec := httptest.NewRecorder()
	apiKeyMiddleware(handleResponses)(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatalf("malformed upstream should not return 200, got %d body %q", rec.Code, rec.Body.String())
	}
	if rec.Code != http.StatusBadGateway && rec.Code != http.StatusInternalServerError && rec.Code != http.StatusBadRequest {
		t.Fatalf("expected error status for malformed upstream, got %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "\"status\":\"completed\"") && strings.Contains(rec.Body.String(), "\"output\"") {
		t.Fatalf("malformed upstream should not produce success completed snapshot, got %q", rec.Body.String())
	}
	rec2 := httptest.NewRecorder()
	chatStreamToResponses(rec2, testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\ndata: not-json\n\n"), nil, map[string]any{"model": "m"})
	raw := rec2.Body.String()
	if strings.Contains(raw, "response.completed") {
		events := parseResponsesEvents(t, raw)
		for _, e := range events {
			if e.event == "response.completed" {
				t.Fatalf("malformed stream must not emit response.completed, got %q", raw)
			}
		}
	}
}

// Task 4: permanent Cline TCP route test
func TestResponses_Cline_FullTCPRoute_JSONSnapshot(t *testing.T) {
	oldZenConfig := *getZenConfig()
	oldProxies := append([]string(nil), oldZenConfig.Proxies...)
	zenTransportMu.Lock()
	oldZenClient := zenHTTPClient
	zenTransportMu.Unlock()
	zenStateMu.Lock()
	oldFailCount := zenFailCount
	oldFailUntil := zenFailUntil
	oldSem := zenSem
	zenStateMu.Unlock()
	poolMu.Lock()
	oldPool := pool
	var oldKeys []string
	var oldAccounts []*Account
	if oldPool != nil {
		oldKeys = append([]string(nil), oldPool.Keys...)
		oldAccounts = append([]*Account(nil), oldPool.Accounts...)
	}
	if pool == nil {
		pool = &AccountPool{Accounts: []*Account{}, Keys: []string{}}
	}
	pool.Keys = []string{"test-cline-tcp-key"}
	acc := &Account{
		AccountID:    "acc_cline_tcp",
		Email:        "cline-tcp@test.com",
		RefreshToken: "dummy-refresh",
		AccessToken:  "dummy-access",
		ExpiresAt:    time.Now().Add(time.Hour).UnixMilli(),
		Status:       "active",
		CreatedAt:    time.Now(),
	}
	pool.Accounts = []*Account{acc}
	poolMu.Unlock()
	reqLogsMu.Lock()
	oldLogs := append([]RequestLog(nil), reqLogs...)
	reqLogs = nil
	reqLogsMu.Unlock()
	oldReqLogsFile := reqLogsFile
	tmpDir := t.TempDir()
	reqLogsFile = tmpDir + "/requests.jsonl"
	modelID := "cline-tcp-model"
	aliasModel := "alias-cline-tcp"
	initModelsCache()
	modelsMu.Lock()
	modelsCache[aliasModel] = &ModelInfo{ID: aliasModel, Source: "free", Provider: "test", Cost: "free", RequiresStream: true, Status: ModelActive}
	modelsCache[modelID] = &ModelInfo{ID: modelID, Source: "free", Provider: "test", Cost: "free", RequiresStream: true, Status: ModelActive}
	modelsMu.Unlock()
	defer func() {
		time.Sleep(200 * time.Millisecond)
		reqLogsFile = oldReqLogsFile
		reqLogsMu.Lock()
		reqLogs = oldLogs
		reqLogsMu.Unlock()
		poolMu.Lock()
		if oldPool != nil {
			pool.Keys = oldKeys
			pool.Accounts = oldAccounts
		} else {
			pool = nil
		}
		poolMu.Unlock()
		zenConfigMu.Lock()
		*zenConfig = oldZenConfig
		zenConfig.Proxies = oldProxies
		zenConfigMu.Unlock()
		rebuildZenTransport()
		zenTransportMu.Lock()
		zenHTTPClient = oldZenClient
		zenTransportMu.Unlock()
		zenStateMu.Lock()
		zenFailCount = oldFailCount
		zenFailUntil = oldFailUntil
		if oldSem != nil {
			zenSem = oldSem
		}
		zenStateMu.Unlock()
		modelsMu.Lock()
		delete(modelsCache, aliasModel)
		delete(modelsCache, modelID)
		modelsMu.Unlock()
	}()
	zenStateMu.Lock()
	zenFailCount = 0
	zenFailUntil = time.Time{}
	zenStateMu.Unlock()
	zenConfigMu.Lock()
	zenConfig.Enabled = false
	zenConfigMu.Unlock()
	rebuildZenTransport()
	zenStateMu.Lock()
	zenSem = make(chan struct{}, 8)
	zenStateMu.Unlock()
	canonical := modelID
	sseBody := "data: {\"model\":\"" + canonical + "\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"think step\"}}]}\n\n" +
		"data: {\"model\":\"" + canonical + "\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello world\"}}]}\n\n" +
		"data: {\"model\":\"" + canonical + "\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"fn\",\"arguments\":\"{\\\"x\\\":1}\"}}]}}]}\n\n" +
		"data: {\"model\":\"" + canonical + "\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":20,\"total_tokens\":30,\"prompt_tokens_details\":{\"cached_tokens\":1,\"cache_write_tokens\":2},\"completion_tokens_details\":{\"reasoning_tokens\":5}}}\n\n"
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
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/responses", apiKeyMiddleware(handleResponses))
	mux.HandleFunc("/responses", apiKeyMiddleware(handleResponses))
	srv := httptest.NewServer(requestLogMiddleware(mux))
	defer srv.Close()
	payload := map[string]any{
		"model":  aliasModel,
		"input":  "hi",
		"stream": false,
	}
	bodyBytes, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", srv.URL+"/v1/responses", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", "test-cline-tcp-key")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d body %s", resp.StatusCode, string(b))
	}
	var out map[string]any
	b, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("response not json: %v body %q", err, string(b))
	}
	if m, _ := out["model"].(string); m != aliasModel {
		t.Fatalf("cline TCP route model must be alias %q got %q (canonical %q)", aliasModel, m, canonical)
	}
	if s, _ := out["status"].(string); s != "completed" {
		t.Fatalf("status want completed got %q", s)
	}
	outputs, _ := out["output"].([]any)
	if len(outputs) != 3 {
		t.Fatalf("expected 3 outputs reasoning/message/tool, got %d: %v", len(outputs), out["output"])
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
				t.Fatalf("reasoning text want think step got %q", txt)
			}
		case "message":
			hasMessage = true
			content, _ := m["content"].([]any)
			if len(content) == 0 {
				t.Fatalf("message content empty")
			}
			if txt, _ := content[0].(map[string]any)["text"].(string); txt != "hello world" {
				t.Fatalf("message text want hello world got %q", txt)
			}
			if ot, _ := out["output_text"].(string); ot != "hello world" {
				t.Fatalf("output_text want hello world got %q", ot)
			}
		case "function_call":
			hasTool = true
			if n, _ := m["name"].(string); n != "fn" {
				t.Fatalf("tool name want fn got %q", n)
			}
			if a, _ := m["arguments"].(string); a != "{\"x\":1}" {
				t.Fatalf("tool args want {\"x\":1} got %q", a)
			}
		}
	}
	if !hasReasoning || !hasMessage || !hasTool {
		t.Fatalf("missing types reasoning=%v message=%v tool=%v", hasReasoning, hasMessage, hasTool)
	}
	if usage, ok := out["usage"].(map[string]any); !ok || usage == nil {
		t.Fatalf("usage missing")
	} else {
		if v, _ := usage["input_tokens"].(float64); int(v) != 10 {
			t.Fatalf("input_tokens want 10 got %v", usage["input_tokens"])
		}
		if itd, ok := usage["input_tokens_details"].(map[string]any); ok {
			if _, hasAlias := itd["cache_creation_input_tokens"]; hasAlias {
				t.Fatalf("usage must not contain alias cache_creation_input_tokens")
			}
		}
	}
	time.Sleep(150 * time.Millisecond)
	var rawLog []byte
	for i := 0; i < 20; i++ {
		rawLog, _ = os.ReadFile(reqLogsFile)
		if rawLog != nil && strings.Contains(string(rawLog), "/v1/responses") {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !strings.Contains(string(rawLog), "/v1/responses") {
		t.Fatalf("request log isolation failed: temp file does not contain /v1/responses: %q", string(rawLog))
	}
}
