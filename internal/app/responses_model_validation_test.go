package app

import (
	"bytes"
	"cline-go-proxy/internal/kit"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestResponses_ModelValidation_MissingEmptyNonString(t *testing.T) {
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
	pool.Keys = []string{"test-model-validation-key"}
	pool.Accounts = []*Account{
		{AccountID: "acc_validation", Email: "validation@test.com", RefreshToken: "dummy", AccessToken: "dummy-access", ExpiresAt: time.Now().Add(time.Hour).UnixMilli(), Status: "active", CreatedAt: time.Now()},
	}
	poolMu.Unlock()
	reqLogsMu.Lock()
	oldLogs := append([]RequestLog(nil), reqLogs...)
	reqLogs = nil
	reqLogsMu.Unlock()
	oldReqLogsFile := reqLogsFile
	tmpDir := t.TempDir()
	reqLogsFile = tmpDir + "/requests.jsonl"

	var clineCalls atomic.Int64
	var zenCalls atomic.Int64
	oldKitClient := kit.HTTPClient
	kit.HTTPClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			clineCalls.Add(1)
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(""))}, nil
		}),
	}
	zenTransportMu.Lock()
	zenHTTPClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			zenCalls.Add(1)
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"choices":[{"message":{"content":"hi"}}]}`))}, nil
		}),
	}
	zenTransportMu.Unlock()

	defer func() {
		time.Sleep(50 * time.Millisecond)
		reqLogsFile = oldReqLogsFile
		reqLogsMu.Lock()
		reqLogs = oldLogs
		reqLogsMu.Unlock()
		kit.HTTPClient = oldKitClient
		zenTransportMu.Lock()
		zenHTTPClient = oldZenClient
		zenTransportMu.Unlock()
		zenConfigMu.Lock()
		*zenConfig = oldZenConfig
		zenConfig.Proxies = oldProxies
		zenConfigMu.Unlock()
		rebuildZenTransport()
		zenStateMu.Lock()
		zenFailCount = oldFailCount
		zenFailUntil = oldFailUntil
		if oldSem != nil {
			zenSem = oldSem
		}
		zenStateMu.Unlock()
		poolMu.Lock()
		if oldPool != nil {
			pool.Keys = oldKeys
			pool.Accounts = oldAccounts
		} else {
			pool = nil
		}
		poolMu.Unlock()
	}()

	zenStateMu.Lock()
	zenFailCount = 0
	zenFailUntil = time.Time{}
	zenStateMu.Unlock()
	zenConfigMu.Lock()
	zenConfig.Enabled = true
	zenConfig.Failover = false
	zenConfigMu.Unlock()
	rebuildZenTransport()
	zenStateMu.Lock()
	zenSem = make(chan struct{}, 8)
	zenStateMu.Unlock()

	initModelsCache()
	modelsMu.Lock()
	// ensure default model exists
	modelsMu.Unlock()

	cases := []struct {
		name    string
		payload map[string]any
	}{
		{"missing", map[string]any{"input": "hi", "stream": false}},
		{"empty", map[string]any{"model": "", "input": "hi"}},
		{"whitespace", map[string]any{"model": "   ", "input": "hi"}},
		{"non-string-number", map[string]any{"model": 123, "input": "hi"}},
		{"non-string-null", map[string]any{"model": nil, "input": "hi"}},
		{"non-string-object", map[string]any{"model": map[string]any{"id": "x"}, "input": "hi"}},
	}
	for _, tc := range cases {
		clineCalls.Store(0)
		zenCalls.Store(0)
		body, _ := json.Marshal(tc.payload)
		req := httptest.NewRequest("POST", "/v1/responses", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-api-key", "test-model-validation-key")
		rec := httptest.NewRecorder()
		apiKeyMiddleware(handleResponses)(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("[%s] expected 400, got %d body %q", tc.name, rec.Code, rec.Body.String())
		}
		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("[%s] response not json: %v body %q", tc.name, err, rec.Body.String())
		}
		errObj, ok := resp["error"].(map[string]any)
		if !ok {
			t.Fatalf("[%s] missing error object: %q", tc.name, rec.Body.String())
		}
		typ, _ := errObj["type"].(string)
		if typ != "invalid_request_error" {
			t.Fatalf("[%s] error type want invalid_request_error got %q body %q", tc.name, typ, rec.Body.String())
		}
		msg, _ := errObj["message"].(string)
		if strings.TrimSpace(msg) == "" {
			t.Fatalf("[%s] error message empty: %q", tc.name, rec.Body.String())
		}
		if clineCalls.Load() != 0 {
			t.Fatalf("[%s] cline upstream should not be called, got %d", tc.name, clineCalls.Load())
		}
		if zenCalls.Load() != 0 {
			t.Fatalf("[%s] zen upstream should not be called, got %d", tc.name, zenCalls.Load())
		}
	}

	// valid trimmed model should pass through to upstream (not 400). Use a real zen free model with mocked success.
	clineCalls.Store(0)
	zenCalls.Store(0)
	zenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		// ensure model present and not fallback unknown
		if m, _ := b["model"].(string); m == "" || m == "unknown" {
			http.Error(w, `{"error":"bad model"}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"big-pickle","choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer zenSrv.Close()
	zenConfigMu.Lock()
	zenConfig.BaseURL = zenSrv.URL
	zenConfig.Key = "public"
	zenConfig.Proxies = nil
	zenConfigMu.Unlock()
	rebuildZenTransport()
	zenStateMu.Lock()
	zenSem = make(chan struct{}, 8)
	zenStateMu.Unlock()

	payload := map[string]any{"model": "  big-pickle  ", "input": "hi", "stream": false}
	bb, _ := json.Marshal(payload)
	req := httptest.NewRequest("POST", "/v1/responses", bytes.NewReader(bb))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", "test-model-validation-key")
	rec := httptest.NewRecorder()
	apiKeyMiddleware(handleResponses)(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("trimmed valid model should succeed, got %d body %q", rec.Code, rec.Body.String())
	}
	var okResp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &okResp); err != nil {
		t.Fatalf("valid response not json: %v", err)
	}
	if m, _ := okResp["model"].(string); m != "big-pickle" {
		t.Fatalf("trimmed model response want big-pickle got %q", m)
	}
}
