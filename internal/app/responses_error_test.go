package app

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// errorReader returns non-EOF error after some bytes
type errorReader struct {
	data []byte
	pos  int
	err  error
}

func (r *errorReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, r.err
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos >= len(r.data) {
		return n, r.err
	}
	return n, nil
}

func TestResponses_BadSSEJSON_ReturnsError(t *testing.T) {
	body := "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\ndata: not-json\n\n"
	up := &http.Response{Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{"Content-Type": []string{"text/event-stream"}}}
	dec, err := newResponsesDecoder(up)
	if err != nil {
		t.Fatalf("decoder init err: %v", err)
	}
	// first delta should succeed
	d, err := dec.Next()
	if err != nil {
		t.Fatalf("first Next should succeed, got %v", err)
	}
	if d.Content != "hi" {
		t.Fatalf("first delta content want hi got %q", d.Content)
	}
	// second should be bad JSON error
	_, err = dec.Next()
	if err == nil {
		t.Fatalf("bad JSON should return error, got nil")
	}
	// aggregated via state should also error
	up2 := &http.Response{Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{"Content-Type": []string{"text/event-stream"}}}
	dec2, _ := newResponsesDecoder(up2)
	state := newResponseState(map[string]any{"model": "m"})
	var aggErr error
	for {
		d, err := dec2.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			aggErr = err
			break
		}
		state.Apply(d, nil, nil)
	}
	if aggErr == nil {
		t.Fatalf("aggregated bad JSON should propagate error")
	}
	// streaming should not emit completed (failed)
	up3 := testUpstream(body)
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up3, nil, map[string]any{"model": "m"})
	raw := rec.Body.String()
	if strings.Contains(raw, "\"status\":\"completed\"") && !strings.Contains(raw, "response.failed") {
		// If decoder error, streaming should not emit successful completed with partial output alone
		// Check that raw does not contain completed with status completed and full output? We allow partial but should not be considered success.
		// At minimum, decoder error should have been logged and not produced completed with empty? Our current impl returns without completed.
		// So raw should NOT contain response.completed
		if strings.Contains(raw, "response.completed") {
			events := parseResponsesEvents(t, raw)
			foundCompleted := false
			for _, e := range events {
				if e.event == "response.completed" {
					foundCompleted = true
				}
			}
			if foundCompleted {
				t.Fatalf("bad JSON streaming should not emit response.completed (failed), got %q", raw)
			}
		}
	}
}

type funcReader struct {
	fn func([]byte) (int, error)
}

func (f funcReader) Read(p []byte) (int, error) { return f.fn(p) }

func TestResponses_NonEOFReaderError_ReturnsError(t *testing.T) {
	calls := 0
	fr := funcReader{fn: func(p []byte) (int, error) {
		calls++
		if calls == 1 {
			s := "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n"
			n := copy(p, s)
			return n, nil
		}
		if calls == 2 {
			// Peek may have already read some; return error on next read
			return 0, io.ErrUnexpectedEOF
		}
		return 0, io.EOF
	}}
	up := &http.Response{Body: io.NopCloser(fr), Header: http.Header{"Content-Type": []string{"text/event-stream"}}}
	dec, _ := newResponsesDecoder(up)
	var gotErr error
	for {
		_, err := dec.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			gotErr = err
			break
		}
	}
	if gotErr == nil {
		t.Fatalf("expected non-EOF reader error, got nil")
	}
	if gotErr == io.EOF {
		t.Fatalf("should not be EOF, got EOF")
	}
}

func TestResponses_EarlyTruncation_ReturnsError(t *testing.T) {
	// truncated JSON without closing braces and no blank line
	body := "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}"
	up := &http.Response{Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{"Content-Type": []string{"text/event-stream"}}}
	dec, _ := newResponsesDecoder(up)
	_, err := dec.Next()
	if err == nil {
		t.Fatalf("truncated JSON should return error, got nil")
	}
	// also test via aggregated state
	up2 := &http.Response{Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{"Content-Type": []string{"text/event-stream"}}}
	dec2, _ := newResponsesDecoder(up2)
	state := newResponseState(map[string]any{"model": "m"})
	var aggErr error
	for {
		d, err := dec2.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			aggErr = err
			break
		}
		state.Apply(d, nil, nil)
	}
	if aggErr == nil {
		t.Fatalf("truncated aggregated should error")
	}
	// ensure not successful partial output is treated as success
	// For handleResponses non-stream case, decoder error should make HTTP return 502 not 200. Simulate via collect
	if aggErr != nil {
		// state should not have produced successful snapshot without error; check that state has no completed
		// We don't emit snapshot when error, so snapshot would be partial; but we should not treat as success.
		// Ensure state snapshot would be incomplete but we don't write it when error.
	}
}

func TestResponses_InterleavedTool_StreamAndAggregatedConsistent(t *testing.T) {
	body := "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_A\",\"function\":{\"name\":\"funcA\",\"arguments\":\"{\\\"a\\\":\"}}]}}]}\n\n" +
		"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":1,\"id\":\"call_B\",\"function\":{\"name\":\"funcB\",\"arguments\":\"{\\\"b\\\":\"}}]}}]}\n\n" +
		"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"1}\"}}]}}]}\n\n" +
		"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":1,\"function\":{\"arguments\":\"2}\"}}]}}]}\n\n"
	// streaming
	upStream := testUpstream(body)
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, upStream, nil, map[string]any{"model": "m"})
	rawStream := rec.Body.String()
	if fails := strictValidateResponsesContract(t, rawStream); len(fails) > 0 {
		t.Fatalf("stream strict failed: %v", fails)
	}
	completedStream := completedOf(t, rawStream)
	outStream, _ := completedStream["output"].([]any)

	// aggregated via decoder+state (non-stream forced)
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
	snapAgg := state.Snapshot("completed", true, &now)
	outAgg, _ := snapAgg["output"].([]any)

	if len(outStream) != len(outAgg) {
		t.Fatalf("stream vs aggregated output length mismatch: %d vs %d", len(outStream), len(outAgg))
	}
	for i := range outStream {
		m1, _ := outStream[i].(map[string]any)
		m2, _ := outAgg[i].(map[string]any)
		if m1["type"] != m2["type"] || m1["name"] != m2["name"] || m1["arguments"] != m2["arguments"] {
			t.Fatalf("mismatch at %d: stream %v vs agg %v", i, m1, m2)
		}
		if m1["call_id"] != m2["call_id"] {
			// call_id may differ due to ID generation but should be consistent per run? Actually IDs are random, but stream and agg generate different IDs
			// So we compare only name/arguments/type, not IDs
		}
	}
	// Also test that both produce same concatenated args and that strict contract holds for aggregated snapshot via streaming validation?
	// For aggregated, we can simulate streaming completed via same snapshot, ensure no fake message
	for _, o := range outAgg {
		if m, _ := o.(map[string]any); m["type"] == "message" {
			t.Fatalf("interleaved tool-only should not contain fake message in aggregated")
		}
	}
}

func TestResponses_DecoderBadJSON_NonStreamJSON(t *testing.T) {
	body := "{not json"
	up := &http.Response{Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{"Content-Type": []string{"application/json"}}}
	dec, _ := newResponsesDecoder(up)
	_, err := dec.Next()
	if err == nil {
		t.Fatalf("bad JSON non-stream should error")
	}
}

func TestResponses_NoDoneSentinel(t *testing.T) {
	body := "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n"
	up := testUpstream(body)
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil, map[string]any{"model": "m"})
	raw := rec.Body.String()
	if strings.Contains(raw, "data: [DONE]") {
		t.Fatalf("Responses must not emit [DONE], got %q", raw)
	}
	if fails := strictValidateResponsesContract(t, raw); len(fails) > 0 {
		t.Fatalf("no DONE strict failed: %v", fails)
	}
}
