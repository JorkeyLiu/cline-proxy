package app

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestResponses_Zen_NonStreamRoute_JSONAndStreamFlag(t *testing.T) {
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
	if oldPool != nil {
		oldKeys = append([]string(nil), oldPool.Keys...)
	}
	if pool == nil {
		pool = &AccountPool{Accounts: []*Account{}, Keys: []string{}}
	}
	pool.Keys = []string{"test-zen-nonstream-key"}
	poolMu.Unlock()
	reqLogsMu.Lock()
	oldLogs := append([]RequestLog(nil), reqLogs...)
	reqLogs = nil
	reqLogsMu.Unlock()
	oldReqLogsFile := reqLogsFile
	tmpDir := t.TempDir()
	reqLogsFile = tmpDir + "/requests.jsonl"

	defer func() {
		time.Sleep(200 * time.Millisecond)
		reqLogsFile = oldReqLogsFile
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
		poolMu.Lock()
		if oldPool != nil {
			pool.Keys = oldKeys
		} else {
			pool = nil
		}
		poolMu.Unlock()
		reqLogsMu.Lock()
		reqLogs = oldLogs
		reqLogsMu.Unlock()
	}()

	zenStateMu.Lock()
	zenFailCount = 0
	zenFailUntil = time.Time{}
	zenStateMu.Unlock()

	const expectedUpstreamModel = "big-pickle"
	var mu sync.Mutex
	var seenStream any
	var seenModel string
	var mismatch string
	zenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			http.NotFound(w, r)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		seenModel, _ = body["model"].(string)
		seenStream = body["stream"]
		if seenModel != expectedUpstreamModel {
			mismatch = "expected upstream model " + expectedUpstreamModel + " got " + seenModel
		}
		if v, ok := body["stream"].(bool); ok && v {
			mismatch = "zen upstream stream must not be true for client stream:false, got true"
		}
		mu.Unlock()
		// Return non-stream JSON (not SSE)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"model":"big-pickle","choices":[{"message":{"content":"Hello JSON"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":7,"total_tokens":12,"prompt_tokens_details":{"cached_tokens":1,"cache_write_tokens":2},"completion_tokens_details":{"reasoning_tokens":0}}}`)
	}))
	defer zenSrv.Close()

	zenConfigMu.Lock()
	zenConfig.Enabled = true
	zenConfig.BaseURL = zenSrv.URL
	zenConfig.Key = "public"
	zenConfig.Proxies = nil
	zenConfig.MaxConcurrency = 8
	zenConfig.Retries = 3
	zenConfig.Failover = false
	zenConfigMu.Unlock()
	rebuildZenTransport()
	zenStateMu.Lock()
	zenSem = make(chan struct{}, 8)
	zenStateMu.Unlock()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/responses", apiKeyMiddleware(handleResponses))
	mux.HandleFunc("/responses", apiKeyMiddleware(handleResponses))
	srv := httptest.NewServer(requestLogMiddleware(mux))
	defer srv.Close()

	payload := map[string]any{
		"model":  "big-pickle",
		"input":  "hi",
		"stream": false,
	}
	bodyBytes, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", srv.URL+"/v1/responses", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", "test-zen-nonstream-key")
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
	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Fatalf("expected Content-Type application/json for non-stream, got %q", ct)
	}
	rawBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(rawBytes) == 0 {
		t.Fatalf("response body empty")
	}
	if strings.Contains(string(rawBytes), "data:") || strings.Contains(string(rawBytes), "event:") {
		t.Fatalf("non-stream response must be JSON, not SSE, got %q", string(rawBytes))
	}
	var out map[string]any
	if err := json.Unmarshal(rawBytes, &out); err != nil {
		t.Fatalf("response not JSON: %v body %q", err, string(rawBytes))
	}
	if st, _ := out["status"].(string); st != "completed" {
		t.Fatalf("status want completed got %q body %q", st, string(rawBytes))
	}
	outputs, _ := out["output"].([]any)
	if len(outputs) == 0 {
		t.Fatalf("output empty: %q", string(rawBytes))
	}
	foundText := ""
	for _, o := range outputs {
		if om, _ := o.(map[string]any); om["type"] == "message" {
			if c, ok := om["content"].([]any); ok && len(c) > 0 {
				if block, ok := c[0].(map[string]any); ok {
					foundText, _ = block["text"].(string)
				}
			}
		}
	}
	if foundText != "Hello JSON" {
		t.Fatalf("completed text want 'Hello JSON' got %q body %q", foundText, string(rawBytes))
	}
	if m, _ := out["model"].(string); m != "big-pickle" {
		t.Fatalf("model want big-pickle got %q", m)
	}
	if usage, ok := out["usage"].(map[string]any); !ok || usage == nil {
		t.Fatalf("usage missing: %v", out["usage"])
	}
	mu.Lock()
	mis := mismatch
	streamVal := seenStream
	modelSeen := seenModel
	mu.Unlock()
	if mis != "" {
		t.Fatalf("zen upstream check failed: %s (model %q stream %v)", mis, modelSeen, streamVal)
	}
	if b, ok := streamVal.(bool); ok && b {
		t.Fatalf("zen upstream stream must not be true for client stream:false, got %v", streamVal)
	}
	if modelSeen != expectedUpstreamModel {
		t.Fatalf("zen upstream model want %q got %q", expectedUpstreamModel, modelSeen)
	}
}
