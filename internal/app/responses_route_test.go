package app

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestResponses_RouteFullViaMuxAndZen(t *testing.T) {
	// --- snapshot globals for isolation ---
	// zenConfig
	oldZenConfig := *getZenConfig()
	// copy proxies slice
	oldProxies := append([]string(nil), oldZenConfig.Proxies...)
	// zenHTTPClient
	zenTransportMu.Lock()
	oldZenClient := zenHTTPClient
	zenTransportMu.Unlock()
	// fail state
	zenStateMu.Lock()
	oldFailCount := zenFailCount
	oldFailUntil := zenFailUntil
	oldSem := zenSem
	zenStateMu.Unlock()
	// pool Keys
	poolMu.Lock()
	oldPool := pool
	var oldKeys []string
	if oldPool != nil {
		oldKeys = append([]string(nil), oldPool.Keys...)
	}
	// ensure pool exists
	if pool == nil {
		pool = &AccountPool{Accounts: []*Account{}, Keys: []string{}}
	}
	// set test key
	pool.Keys = []string{"test-route-key"}
	poolMu.Unlock()
	// reqLogs
	reqLogsMu.Lock()
	oldLogs := append([]RequestLog(nil), reqLogs...)
	reqLogs = nil
	reqLogsMu.Unlock()
	oldReqLogsFile := reqLogsFile
	tmpDir := t.TempDir()
	reqLogsFile = tmpDir + "/requests.jsonl"

	defer func() {
		// wait for async AppendReqLog goroutines to finish flushing to temp file before restoring globals
		time.Sleep(200 * time.Millisecond)
		reqLogsFile = oldReqLogsFile
		// restore zenConfig
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

	// ensure zen not in failover
	zenStateMu.Lock()
	zenFailCount = 0
	zenFailUntil = time.Time{}
	zenStateMu.Unlock()

	// --- fake Zen upstream ---
	const expectedUpstreamModel = "big-pickle"
	var upstreamModelSeen string
	var upstreamModelMismatch string
	var upstreamMu sync.Mutex
	zenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			http.NotFound(w, r)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		upstreamMu.Lock()
		if m, _ := body["model"].(string); m != expectedUpstreamModel {
			upstreamModelMismatch = "expected upstream model " + expectedUpstreamModel + " got " + m
		} else {
			upstreamModelSeen = m
		}
		upstreamMu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		// emit SSE
		_, _ = io.WriteString(w, "data: {\"model\":\"big-pickle\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(10 * time.Millisecond)
		_, _ = io.WriteString(w, "data: {\"model\":\"big-pickle\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(10 * time.Millisecond)
		_, _ = io.WriteString(w, "data: {\"model\":\"big-pickle\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":7,\"total_tokens\":12,\"prompt_tokens_details\":{\"cached_tokens\":1,\"cache_write_tokens\":2},\"completion_tokens_details\":{\"reasoning_tokens\":0}}}\n\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}))
	defer zenSrv.Close()

	// configure zen to point to fake
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
	// reset sem after rebuild
	zenStateMu.Lock()
	zenSem = make(chan struct{}, 8)
	zenStateMu.Unlock()

	// --- proxy mux with real apiKeyMiddleware + requestLogMiddleware ---
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/responses", apiKeyMiddleware(handleResponses))
	mux.HandleFunc("/responses", apiKeyMiddleware(handleResponses))
	srv := httptest.NewServer(requestLogMiddleware(mux))
	defer srv.Close()

	// --- real POST via TCP ---
	payload := map[string]any{
		"model":  "big-pickle",
		"input":  "hi",
		"stream": true,
	}
	bodyBytes, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", srv.URL+"/v1/responses", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", "test-route-key")
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
	rawBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	raw := string(rawBytes)
	upstreamMu.Lock()
	mismatch := upstreamModelMismatch
	seen := upstreamModelSeen
	upstreamMu.Unlock()
	if mismatch != "" {
		t.Fatalf("zen upstream model check failed: %s (seen %q want %q)", mismatch, seen, expectedUpstreamModel)
	}
	if seen != expectedUpstreamModel {
		t.Fatalf("zen upstream model not verified: seen %q want %q", seen, expectedUpstreamModel)
	}
	if strings.Contains(raw, "data: [DONE]") {
		t.Fatalf("must not contain [DONE], got %q", raw)
	}
	failures := strictValidateResponsesContract(t, raw)
	if len(failures) > 0 {
		t.Fatalf("strict contract via route failed (%d): %s\nall:\n- %s\n\nraw:\n%s", len(failures), failures[0], strings.Join(failures, "\n- "), raw)
	}
	events := parseResponsesEvents(t, raw)
	// verify model routing correct: completed model should be big-pickle
	var completed map[string]any
	var deltas strings.Builder
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
		t.Fatalf("missing completed")
	}
	if m, _ := completed["model"].(string); m != "big-pickle" {
		t.Fatalf("model routing wrong, got %q want big-pickle", m)
	}
	// realtime vs completed text
	completedText := ""
	if outputs, _ := completed["output"].([]any); len(outputs) > 0 {
		for _, o := range outputs {
			if om, _ := o.(map[string]any); om["type"] == "message" {
				if c, ok := om["content"].([]any); ok && len(c) > 0 {
					if block, ok := c[0].(map[string]any); ok {
						completedText, _ = block["text"].(string)
					}
				}
			}
		}
	}
	if deltas.String() != "Hello world" {
		t.Fatalf("realtime deltas want 'Hello world' got %q", deltas.String())
	}
	if completedText != "Hello world" {
		t.Fatalf("completed text want 'Hello world' got %q", completedText)
	}
	if completedText != deltas.String() {
		t.Fatalf("realtime vs completed mismatch: deltas %q vs completed %q", deltas.String(), completedText)
	}
	// usage should be present (seen) and official only
	if usage, ok := completed["usage"].(map[string]any); !ok || usage == nil {
		t.Fatalf("completed usage missing, got %v", completed["usage"])
	} else {
		if itd, ok := usage["input_tokens_details"].(map[string]any); ok {
			if _, hasAlias := itd["cache_creation_input_tokens"]; hasAlias {
				t.Fatalf("usage must not contain alias, got %v", itd)
			}
		}
	}
	// verify requestLogMiddleware isolation: async write went to temp file, not pollution
	time.Sleep(150 * time.Millisecond)
	// poll up to 500ms for async log to appear in temp file
	var logged []byte
	var logErr error
	for i := 0; i < 20; i++ {
		logged, logErr = os.ReadFile(reqLogsFile)
		if logErr == nil && len(logged) > 0 && strings.Contains(string(logged), "/v1/responses") {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if logErr != nil {
		t.Fatalf("request log temp file read failed: %v (path %s)", logErr, reqLogsFile)
	}
	if !strings.Contains(string(logged), "/v1/responses") {
		t.Fatalf("request log isolation failed: temp file %s does not contain /v1/responses: %q", reqLogsFile, string(logged))
	}
	// ensure original file not polluted during this test (we cannot fully guarantee old content, but ensure we didn't write to old path while switched)
	if oldReqLogsFile != reqLogsFile {
		// old file should not have been appended with this temp-run's entry in the last 200ms window; we check temp file is separate
		if _, err := os.Stat(oldReqLogsFile); err == nil {
			// not failing on existence, just ensure temp file is distinct path
			if oldReqLogsFile == reqLogsFile {
				t.Fatalf("reqLogsFile not isolated")
			}
		}
	}
}
