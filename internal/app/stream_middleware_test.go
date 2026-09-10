package app

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestMiddlewarePreservesFlushViaController verifies that a handler wrapped by
// requestLogMiddleware can still obtain flush capability via http.ResponseController.
// On old HEAD (statusWriter without Unwrap), rc.Flush() returns "feature not supported"
// and the test fails, reproducing the production root cause.
func TestMiddlewarePreservesFlushViaController(t *testing.T) {
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		rc := http.NewResponseController(w)
		if err := rc.Flush(); err != nil {
			t.Errorf("ResponseController Flush failed through middleware: %v", err)
			return
		}
		called = true
		_, _ = w.Write([]byte("data: ok\n\n"))
		_ = rc.Flush()
	})
	srv := httptest.NewServer(requestLogMiddleware(inner))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "ok") {
		t.Fatalf("expected ok in body, got %q", string(body))
	}
	if !called {
		t.Fatalf("inner handler did not confirm flush")
	}
	// status code must be recorded as 200, not 0
	time.Sleep(50 * time.Millisecond) // allow async log
	logs := LoadRequestLogs()
	if len(logs) == 0 {
		t.Fatalf("no request logs")
	}
	last := logs[len(logs)-1]
	if last.Status != http.StatusOK {
		t.Fatalf("expected logged status 200, got %d", last.Status)
	}
}

// TestMiddlewareRealtimeFlushProvesNetworkFlush verifies that flushing is realtime,
// not buffered until handler return. The inner handler writes first SSE frame,
// flushes, then blocks on gate; client must receive first frame before gate release.
func TestMiddlewareRealtimeFlushProvesNetworkFlush(t *testing.T) {
	gate := make(chan struct{})
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusOK)
		rc := http.NewResponseController(w)
		if err := rc.Flush(); err != nil {
			t.Errorf("initial flush failed: %v", err)
			return
		}
		if _, err := w.Write([]byte("data: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"first\"}}]}\n\n")); err != nil {
			return
		}
		_ = rc.Flush()
		<-gate
		if _, err := w.Write([]byte("data: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"second\"}}]}\n\n")); err != nil {
			return
		}
		_ = rc.Flush()
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
		_ = rc.Flush()
	})

	srv := httptest.NewServer(requestLogMiddleware(inner))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	firstCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		var buf strings.Builder
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				errCh <- err
				return
			}
			buf.WriteString(line)
			if strings.Contains(buf.String(), "first") {
				firstCh <- buf.String()
				return
			}
			// also handle case where data arrives without newline? but we always have \n\n
			if line == "\n" && buf.Len() > 0 {
				if strings.Contains(buf.String(), "first") {
					firstCh <- buf.String()
					return
				}
			}
		}
	}()

	select {
	case got := <-firstCh:
		if !strings.Contains(got, "first") {
			t.Fatalf("first payload missing first: %q", got)
		}
		// success: first frame arrived before gate release -> proves flush is realtime
	case err := <-errCh:
		t.Fatalf("read error before first frame: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for first frame before gate release; flushing likely buffered (old bug)")
	}

	// now release gate and verify second frame arrives
	close(gate)
	// read remaining with timeout
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(reader)
		done <- string(b)
	}()
	select {
	case rest := <-done:
		if !strings.Contains(rest, "second") {
			t.Fatalf("second payload not received after gate: %q", rest)
		}
		if !strings.Contains(rest, "[DONE]") {
			t.Fatalf("missing [DONE] after second: %q", rest)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for second frame")
	}

	// verify status still recorded
	time.Sleep(50 * time.Millisecond)
	logs := LoadRequestLogs()
	if len(logs) == 0 {
		t.Fatalf("no logs")
	}
	last := logs[len(logs)-1]
	if last.Status != http.StatusOK {
		t.Fatalf("expected 200, got %d", last.Status)
	}
}

// TestHandleStreamResponseWithUsageViaMiddlewareNetwork verifies the real
// handleStreamResponseWithUsage through middleware+httptest.Server (no real upstream).
// Two fake upstream frames are forwarded and [DONE] is emitted exactly once.
func TestHandleStreamResponseWithUsageViaMiddlewareNetwork(t *testing.T) {
	upBody := "data: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\ndata: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\n"
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := testUpstream(upBody)
		handleStreamResponseWithUsage(w, up, nil)
	})
	srv := httptest.NewServer(requestLogMiddleware(inner))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(resp.Body)
	body := string(bodyBytes)
	if !strings.Contains(body, "Hello") || !strings.Contains(body, " world") {
		t.Fatalf("expected Hello world forwarded, got %q", body)
	}
	if got := strings.Count(body, "[DONE]"); got != 1 {
		t.Fatalf("expected exactly 1 [DONE], got %d: %q", got, body)
	}
	if !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Fatalf("expected DONE suffix, got %q", body)
	}
	// payloads must be JSON
	for _, p := range chatPayloads(t, body) {
		if p == "[DONE]" || p == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(p), &m); err != nil {
			t.Fatalf("payload not JSON: %q err=%v", p, err)
		}
	}
}

// ensure keep statusWriter recording after flush: write header 201 then flush
func TestStatusWriterKeepsStatusCodeAfterFlush(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		rc := http.NewResponseController(w)
		_ = rc.Flush()
		_, _ = w.Write([]byte("hello"))
		_ = rc.Flush()
	})
	srv := httptest.NewServer(requestLogMiddleware(inner))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	time.Sleep(50 * time.Millisecond)
	logs := LoadRequestLogs()
	if len(logs) == 0 {
		t.Fatalf("no logs")
	}
	last := logs[len(logs)-1]
	if last.Status != http.StatusCreated {
		t.Fatalf("expected logged 201, got %d", last.Status)
	}
}

// TestAnthropicStreamViaMiddlewareRealtimeFlush verifies Anthropic conversion
// through requestLogMiddleware + real TCP (httptest.NewServer). Handler builds
// a fake OpenAI upstream SSE body and calls handleAnthropicStreamWithUsage.
// Client reads via real HTTP, asserts message_start / text_delta / message_delta
// / message_stop all arrive, Content-Type is text/event-stream, and proves
// the first renderable event arrives before handler gate release (realtime flush).
func TestAnthropicStreamViaMiddlewareRealtimeFlush(t *testing.T) {
	gate := make(chan struct{})
	pr, pw := io.Pipe()

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream := &http.Response{Body: pr}
		handleAnthropicStreamWithUsage(w, upstream, "test-model", nil, nil)
	})

	srv := httptest.NewServer(requestLogMiddleware(inner))
	defer srv.Close()
	defer func() {
		_ = pr.Close()
		_ = pw.Close()
	}()

	// Upstream writer: first delta before gate, second after gate.
	go func() {
		defer pw.Close()
		if _, err := io.WriteString(pw, "data: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n"); err != nil {
			return
		}
		<-gate
		_, _ = io.WriteString(pw, "data: {\"id\":\"1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\n")
	}()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("expected Content-Type text/event-stream, got %q", ct)
	}

	reader := bufio.NewReader(resp.Body)
	type evt struct {
		event string
		data  string
	}
	evtCh := make(chan evt, 32)
	errCh := make(chan error, 1)
	go func() {
		var cur []string
		for {
			line, err := reader.ReadString('\n')
			if line != "" {
				trim := strings.TrimRight(line, "\r\n")
				if strings.TrimSpace(trim) == "" {
					if len(cur) > 0 {
						var ev, ds string
						for _, l := range cur {
							if strings.HasPrefix(l, "event:") {
								ev = strings.TrimSpace(strings.TrimPrefix(l, "event:"))
							}
							if strings.HasPrefix(l, "data:") {
								ds = strings.TrimSpace(strings.TrimPrefix(l, "data:"))
							}
						}
						if ev != "" {
							evtCh <- evt{ev, ds}
						}
						cur = nil
					}
				} else {
					cur = append(cur, trim)
				}
			}
			if err != nil {
				if err != io.EOF {
					errCh <- err
				}
				close(evtCh)
				return
			}
		}
	}()

	// Gate closer helper to avoid blocking on failure.
	closeGate := func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}
	defer closeGate()

	// Collect events with deadline coordination.
	var seenStart, seenDelta, seenMessageDelta, seenStop bool
	var deltaTexts []string
	firstRenderableCh := make(chan struct{}, 1)

	// We must prove first renderable event arrived before gate release.
	// So we wait for message_start or first text_delta before closing gate.
	deadline := time.After(3 * time.Second)
	gateClosed := false
	collected := []evt{}
waitLoop:
	for {
		select {
		case ev, ok := <-evtCh:
			if !ok {
				break waitLoop
			}
			collected = append(collected, ev)
			switch ev.event {
			case "message_start":
				seenStart = true
				var payload map[string]any
				if err := json.Unmarshal([]byte(ev.data), &payload); err != nil {
					t.Errorf("message_start data not JSON: %q err=%v", ev.data, err)
				}
				if payload["type"] != "message_start" {
					t.Errorf("message_start type wrong: %v", payload)
				}
				select {
				case firstRenderableCh <- struct{}{}:
				default:
				}
			case "content_block_delta":
				var payload map[string]any
				if err := json.Unmarshal([]byte(ev.data), &payload); err != nil {
					t.Errorf("content_block_delta not JSON: %q err=%v", ev.data, err)
					break
				}
				if d, ok := payload["delta"].(map[string]any); ok {
					if d["type"] == "text_delta" {
						if txt, ok := d["text"].(string); ok {
							deltaTexts = append(deltaTexts, txt)
							seenDelta = seenDelta || txt != ""
						}
					}
				}
				select {
				case firstRenderableCh <- struct{}{}:
				default:
				}
			case "message_delta":
				seenMessageDelta = true
			case "message_stop":
				seenStop = true
				// message_stop is terminal; break after drain
			}
			// Once we have seen the first renderable event, prove flush by closing gate exactly once.
			if !gateClosed {
				select {
				case <-firstRenderableCh:
					closeGate()
					gateClosed = true
				default:
				}
			}
			if seenStop {
				// keep draining until channel closed or timeout
			}
		case err := <-errCh:
			t.Fatalf("read error: %v", err)
		case <-deadline:
			closeGate()
			t.Fatalf("timeout waiting for anthropic stream; collected=%v deltaTexts=%v", collected, deltaTexts)
		}
		if gateClosed && seenStop {
			// Drain remaining with short timeout instead of permanent block.
			drainDeadline := time.After(2 * time.Second)
			for {
				select {
				case ev, ok := <-evtCh:
					if !ok {
						break waitLoop
					}
					collected = append(collected, ev)
					if ev.event == "message_delta" {
						seenMessageDelta = true
					}
					if ev.event == "message_stop" {
						seenStop = true
					}
				case <-drainDeadline:
					break waitLoop
				}
			}
		}
		if seenStop && gateClosed {
			break
		}
		// If gate not yet closed and we timed out waiting for first event, deadline above will fire.
	}

	if !gateClosed {
		closeGate()
	}

	// Final drain with timeout to avoid permanent block on truncated stream.
	drainDone := make(chan struct{})
	go func() {
		for range evtCh {
		}
		close(drainDone)
	}()
	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
	}

	if !seenStart {
		t.Fatalf("missing message_start; collected events: %v", collected)
	}
	if !seenDelta {
		t.Fatalf("missing text_delta content; deltaTexts=%v collected=%v", deltaTexts, collected)
	}
	joined := strings.Join(deltaTexts, "")
	if !strings.Contains(joined, "Hello") {
		t.Fatalf("deltaTexts missing Hello: %q collected=%v", joined, collected)
	}
	if !seenMessageDelta {
		t.Fatalf("missing message_delta; collected=%v", collected)
	}
	if !seenStop {
		t.Fatalf("missing message_stop; collected=%v", collected)
	}
	// Ensure world arrived (second delta after gate) to prove stream completed correctly.
	if !strings.Contains(joined, " world") {
		// Fallback: search in collected payloads
		found := false
		for _, ev := range collected {
			if strings.Contains(ev.data, "world") {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing second delta world after gate; joined=%q collected=%v", joined, collected)
		}
	}
}

// TestResponsesStreamViaMiddlewareRealtimeFlush verifies Responses SSE conversion
// through requestLogMiddleware + real TCP. Handler calls chatStreamToResponses
// (the production writer path) with a fake upstream split into two text deltas
// controlled by gate. Asserts response.created/in_progress, output_text.delta,
// response.completed arrival, final output_text correctness, and proves
// front events arrived before handler completion (realtime flush).
func TestResponsesStreamViaMiddlewareRealtimeFlush(t *testing.T) {
	gate := make(chan struct{})
	pr, pw := io.Pipe()

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		upstream := &http.Response{Body: pr}
		chatStreamToResponses(w, upstream, nil)
	})

	srv := httptest.NewServer(requestLogMiddleware(inner))
	defer srv.Close()
	defer func() {
		_ = pr.Close()
		_ = pw.Close()
	}()

	go func() {
		defer pw.Close()
		if _, err := io.WriteString(pw, "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n"); err != nil {
			return
		}
		<-gate
		_, _ = io.WriteString(pw, "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\n")
	}()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("expected Content-Type text/event-stream, got %q", ct)
	}

	reader := bufio.NewReader(resp.Body)
	type evt struct {
		event string
		data  string
	}
	evtCh := make(chan evt, 64)
	errCh := make(chan error, 1)
	go func() {
		var cur []string
		for {
			line, err := reader.ReadString('\n')
			if line != "" {
				trim := strings.TrimRight(line, "\r\n")
				if strings.TrimSpace(trim) == "" {
					if len(cur) > 0 {
						var ev, ds string
						for _, l := range cur {
							if strings.HasPrefix(l, "event:") {
								ev = strings.TrimSpace(strings.TrimPrefix(l, "event:"))
							}
							if strings.HasPrefix(l, "data:") {
								ds = strings.TrimSpace(strings.TrimPrefix(l, "data:"))
							}
						}
						if ev != "" {
							evtCh <- evt{ev, ds}
						}
						cur = nil
					}
				} else {
					cur = append(cur, trim)
				}
			}
			if err != nil {
				if err != io.EOF {
					errCh <- err
				}
				close(evtCh)
				return
			}
		}
	}()

	closeGate := func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}
	defer closeGate()

	var seenCreated, seenInProgress, seenDelta, seenCompleted bool
	var deltaTexts []string
	var completedPayload map[string]any
	firstCh := make(chan struct{}, 1)
	gateClosed := false
	collected := []evt{}
	deadline := time.After(3 * time.Second)

waitLoop2:
	for {
		select {
		case ev, ok := <-evtCh:
			if !ok {
				break waitLoop2
			}
			collected = append(collected, ev)
			switch ev.event {
			case "response.created":
				seenCreated = true
				var payload map[string]any
				if err := json.Unmarshal([]byte(ev.data), &payload); err != nil {
					t.Errorf("response.created data not JSON: %v", err)
				}
				select {
				case firstCh <- struct{}{}:
				default:
				}
			case "response.in_progress":
				seenInProgress = true
				select {
				case firstCh <- struct{}{}:
				default:
				}
			case "response.output_text.delta":
				seenDelta = true
				var payload map[string]any
				if err := json.Unmarshal([]byte(ev.data), &payload); err == nil {
					if d, ok := payload["delta"].(string); ok {
						deltaTexts = append(deltaTexts, d)
					}
				}
				select {
				case firstCh <- struct{}{}:
				default:
				}
			case "response.completed":
				seenCompleted = true
				var payload map[string]any
				if err := json.Unmarshal([]byte(ev.data), &payload); err == nil {
					completedPayload = payload
				}
			}
			if !gateClosed {
				select {
				case <-firstCh:
					closeGate()
					gateClosed = true
				default:
				}
			}
			if seenCompleted {
				// will drain then exit
			}
		case err := <-errCh:
			t.Fatalf("read error: %v", err)
		case <-deadline:
			closeGate()
			t.Fatalf("timeout waiting for responses stream; collected=%v delta=%v", collected, deltaTexts)
		}
		if gateClosed && seenCompleted {
			// drain remaining quickly
			drainDeadline := time.After(2 * time.Second)
			for {
				select {
				case ev, ok := <-evtCh:
					if !ok {
						break waitLoop2
					}
					collected = append(collected, ev)
					if ev.event == "response.completed" {
						seenCompleted = true
						var payload map[string]any
						if err := json.Unmarshal([]byte(ev.data), &payload); err == nil {
							completedPayload = payload
						}
					}
				case <-drainDeadline:
					break waitLoop2
				}
			}
		}
		if seenCompleted && gateClosed {
			break
		}
	}

	if !gateClosed {
		closeGate()
	}
	// ensure reader goroutine finished without permanent block
	drainDone := make(chan struct{})
	go func() {
		for range evtCh {
		}
		close(drainDone)
	}()
	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
	}

	if !seenCreated {
		t.Fatalf("missing response.created; collected=%v", collected)
	}
	if !seenInProgress {
		t.Fatalf("missing response.in_progress; collected=%v", collected)
	}
	if !seenDelta {
		t.Fatalf("missing response.output_text.delta; collected=%v", collected)
	}
	if !seenCompleted {
		t.Fatalf("missing response.completed; collected=%v", collected)
	}
	joined := strings.Join(deltaTexts, "")
	if joined != "Hello world" {
		t.Fatalf("delta joined %q != Hello world; collected=%v", joined, collected)
	}
	if completedPayload == nil {
		t.Fatalf("completed payload nil")
	}
	respObj, _ := completedPayload["response"].(map[string]any)
	if respObj == nil {
		t.Fatalf("completed missing response field: %v", completedPayload)
	}
	if ot, _ := respObj["output_text"].(string); ot != "Hello world" {
		t.Fatalf("completed output_text %q != Hello world", ot)
	}
	outputs, _ := respObj["output"].([]any)
	if len(outputs) == 0 {
		t.Fatalf("completed output empty: %v", respObj)
	}
	msg, _ := outputs[0].(map[string]any)
	if msg["type"] != "message" {
		t.Fatalf("first output should be message, got %v", msg)
	}
	content, _ := msg["content"].([]any)
	if len(content) == 0 {
		t.Fatalf("message content empty")
	}
	block, _ := content[0].(map[string]any)
	if block["type"] != "output_text" || block["text"] != "Hello world" {
		t.Fatalf("message output_text block wrong: %v", block)
	}
	// prove front events arrived before completion: gateClosed true implies first event arrived before gate, and we closed gate only after first event.
	if !gateClosed {
		t.Fatalf("gate was never closed; flush proof invalid")
	}
}
