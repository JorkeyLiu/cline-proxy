package app

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestResponsesStrictContract_ToolLifecycle(t *testing.T) {
	up := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"get_weather\",\"arguments\":\"{\\\"city\\\":\\\"SF\\\"}\"}}]}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"finish_reason\":\"tool_calls\"}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil, map[string]any{"model": "m"})
	raw := rec.Body.String()
	failures := strictValidateResponsesContract(t, raw)
	if len(failures) > 0 {
		t.Fatalf("strict contract tool FAILED (%d) — first: %s\nall:\n- %s\n\nraw:\n%s", len(failures), failures[0], strings.Join(failures, "\n- "), raw)
	}
	completed := completedOf(t, raw)
	outputs, _ := completed["output"].([]any)
	if len(outputs) != 1 {
		t.Fatalf("tool-only completed output must have 1 item, got %v", completed["output"])
	}
	if m, _ := outputs[0].(map[string]any); m["type"] != "function_call" {
		t.Fatalf("tool-only output type must be function_call, got %v", outputs[0])
	}
	events := parseResponsesEvents(t, raw)
	for _, e := range events {
		if e.event == "response.output_item.added" {
			if item, ok := e.data["item"].(map[string]any); ok && item["type"] == "function_call" {
				if oi, _ := e.data["output_index"].(float64); int(oi) != 0 {
					t.Fatalf("tool-only output_index should be 0, got %v", e.data["output_index"])
				}
			}
		}
	}
}

func TestResponsesStrictContract_UsageMapping(t *testing.T) {
	up := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":20,\"total_tokens\":30,\"prompt_tokens_details\":{\"cached_tokens\":4,\"cache_creation_input_tokens\":1},\"completion_tokens_details\":{\"reasoning_tokens\":2}}}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil, map[string]any{"model": "m"})
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
		t.Fatalf("missing completed")
	}
	usage, _ := completed["usage"].(map[string]any)
	if usage == nil {
		t.Fatalf("missing usage")
	}
	if v, _ := usage["input_tokens"].(float64); int(v) != 10 {
		t.Fatalf("input_tokens want 10 got %v", usage["input_tokens"])
	}
	if v, _ := usage["output_tokens"].(float64); int(v) != 20 {
		t.Fatalf("output_tokens want 20 got %v", usage["output_tokens"])
	}
	if v, _ := usage["total_tokens"].(float64); int(v) != 30 {
		t.Fatalf("total_tokens want 30 got %v", usage["total_tokens"])
	}
	if itd, ok := usage["input_tokens_details"].(map[string]any); ok {
		if v, _ := itd["cached_tokens"].(float64); int(v) != 4 {
			t.Fatalf("cached_tokens want 4 got %v", itd["cached_tokens"])
		}
		// official output only cache_write_tokens, alias accepted on input but not emitted
		if _, hasAlias := itd["cache_creation_input_tokens"]; hasAlias {
			t.Fatalf("output must not contain alias cache_creation_input_tokens, got %v", itd)
		}
		if v, _ := itd["cache_write_tokens"].(float64); int(v) != 1 {
			t.Fatalf("cache_write_tokens want 1 got %v", itd["cache_write_tokens"])
		}
	} else {
		t.Fatalf("missing input_tokens_details")
	}
	if otd, ok := usage["output_tokens_details"].(map[string]any); ok {
		if v, _ := otd["reasoning_tokens"].(float64); int(v) != 2 {
			t.Fatalf("reasoning_tokens want 2 got %v", otd["reasoning_tokens"])
		}
	} else {
		t.Fatalf("missing output_tokens_details")
	}
	raw := rec.Body.String()
	if fails := strictValidateResponsesContract(t, raw); len(fails) > 0 {
		t.Fatalf("usage mapping strict contract failed: %v", fails)
	}
}

func TestResponsesStrictContract_NoUsageNil(t *testing.T) {
	up := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"},\"finish_reason\":\"stop\"}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil, map[string]any{"model": "m"})
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
		t.Fatalf("missing completed")
	}
	// absent usage must be nil/null, not forged zeros
	if usage, exists := completed["usage"]; !exists {
		t.Fatalf("completed missing usage field")
	} else if usage != nil {
		t.Fatalf("absent upstream usage must be nil, got %v", usage)
	}
	// validator must allow nil
	raw := rec.Body.String()
	if fails := strictValidateResponsesContract(t, raw); len(fails) > 0 {
		t.Fatalf("no-usage nil strict should pass, got %v", fails)
	}
	// seen-zero-values: explicit zero usage must be full structure with zeros
	up2 := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":0,\"total_tokens\":0,\"prompt_tokens_details\":{\"cached_tokens\":0,\"cache_write_tokens\":0},\"completion_tokens_details\":{\"reasoning_tokens\":0}}}\n\n")
	rec2 := httptest.NewRecorder()
	chatStreamToResponses(rec2, up2, nil, map[string]any{"model": "m"})
	evs2 := parseResponsesEvents(t, rec2.Body.String())
	var comp2 map[string]any
	for _, e := range evs2 {
		if e.event == "response.completed" {
			if r, ok := e.data["response"].(map[string]any); ok {
				comp2 = r
			}
		}
	}
	usage2, _ := comp2["usage"].(map[string]any)
	if usage2 == nil {
		t.Fatalf("seen-zero usage must be object, got nil")
	}
	for _, k := range []string{"input_tokens", "output_tokens", "total_tokens"} {
		if v, _ := usage2[k].(float64); int(v) != 0 {
			t.Fatalf("seen zero %s should be 0, got %v", k, usage2[k])
		}
	}
	if itd, ok := usage2["input_tokens_details"].(map[string]any); !ok || itd["cached_tokens"] == nil {
		t.Fatalf("seen zero missing cached_tokens")
	} else if _, ok := itd["cache_write_tokens"]; !ok {
		t.Fatalf("seen zero missing cache_write_tokens")
	}
	if otd, ok := usage2["output_tokens_details"].(map[string]any); !ok || otd["reasoning_tokens"] == nil {
		t.Fatalf("seen zero missing reasoning_tokens")
	}
	// alternative style: input_tokens_details.cached_tokens direct
	up3 := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"input_tokens\":5,\"output_tokens\":6,\"total_tokens\":11,\"input_tokens_details\":{\"cached_tokens\":2},\"output_tokens_details\":{\"reasoning_tokens\":3}}}\n\n")
	rec3 := httptest.NewRecorder()
	chatStreamToResponses(rec3, up3, nil, map[string]any{"model": "m"})
	evs3 := parseResponsesEvents(t, rec3.Body.String())
	var comp3 map[string]any
	for _, e := range evs3 {
		if e.event == "response.completed" {
			if r, ok := e.data["response"].(map[string]any); ok {
				comp3 = r
			}
		}
	}
	u3, _ := comp3["usage"].(map[string]any)
	if itd, ok := u3["input_tokens_details"].(map[string]any); ok {
		if v, _ := itd["cached_tokens"].(float64); int(v) != 2 {
			t.Fatalf("alt cached_tokens want 2 got %v", itd["cached_tokens"])
		}
		if _, hasAlias := itd["cache_creation_input_tokens"]; hasAlias {
			t.Fatalf("output must not contain alias, got %v", itd)
		}
	}
	if otd, ok := u3["output_tokens_details"].(map[string]any); ok {
		if v, _ := otd["reasoning_tokens"].(float64); int(v) != 3 {
			t.Fatalf("alt reasoning_tokens want 3 got %v", otd["reasoning_tokens"])
		}
	}
}

func TestResponsesStrictContract_NonStreamingUsageMapping(t *testing.T) {
	// simulate non-stream snapshot via direct state (no chatToResponses double helper)
	state := newResponseState(map[string]any{"model": "m", "parallel_tool_calls": true})
	// usage
	u := map[string]any{
		"prompt_tokens": 7, "completion_tokens": 8, "total_tokens": 15,
		"prompt_tokens_details":     map[string]any{"cached_tokens": 3, "cache_creation_input_tokens": 2},
		"completion_tokens_details": map[string]any{"reasoning_tokens": 5},
	}
	in, outt, total, cached, cw, reasoning := parseUsageValues(u)
	state.inTokens = in
	state.outTokens = outt
	state.totalTokens = total
	state.cachedTokens = cached
	state.cacheWriteTokens = cw
	state.reasoningTokens = reasoning
	state.usageSeen = true
	// message + reasoning + tool via typed delta
	delta := &upstreamDelta{
		Content:   "hello",
		Reasoning: "think",
		ToolFrags: []toolFrag{{Index: 0, ID: "call_1", Name: "fn", Args: "{}"}},
	}
	state.Apply(delta, nil, nil)
	state.FinalizePendingTools(nil)
	now := int64(1234567890)
	out := state.Snapshot("completed", true, &now)
	usage, _ := out["usage"].(map[string]any)
	if usage == nil {
		t.Fatalf("non-stream missing usage")
	}
	if v, _ := usage["input_tokens"].(float64); int(v) != 7 {
		if vi, ok := usage["input_tokens"].(int); !ok || vi != 7 {
			t.Fatalf("non-stream input_tokens want 7 got %v (%T)", usage["input_tokens"], usage["input_tokens"])
		}
	}
	if itd, ok := usage["input_tokens_details"].(map[string]any); ok {
		if itd["cached_tokens"] == nil || itd["cache_write_tokens"] == nil {
			t.Fatalf("non-stream missing cached/cache_write: %v", itd)
		}
		if _, hasAlias := itd["cache_creation_input_tokens"]; hasAlias {
			t.Fatalf("non-stream must not contain alias, got %v", itd)
		}
	} else {
		t.Fatalf("missing input_tokens_details")
	}
	if otd, ok := usage["output_tokens_details"].(map[string]any); !ok || otd["reasoning_tokens"] == nil {
		t.Fatalf("missing reasoning_tokens")
	}
	outputs, _ := out["output"].([]any)
	if len(outputs) != 3 {
		t.Fatalf("non-stream expected 3 outputs (reasoning,message,tool), got %d: %v", len(outputs), out["output"])
	}
	// absent usage -> nil via fresh state
	state2 := newResponseState(map[string]any{"model": "m"})
	delta2 := &upstreamDelta{Content: "hi"}
	state2.Apply(delta2, nil, nil)
	state2.FinalizePendingTools(nil)
	out2 := state2.Snapshot("completed", true, &now)
	if out2["usage"] != nil {
		t.Fatalf("non-stream absent usage must be nil, got %v", out2["usage"])
	}
}

func TestResponses_NonStreamingNoUsageNil(t *testing.T) {
	state := newResponseState(map[string]any{"model": "m"})
	delta := &upstreamDelta{Content: "hi"}
	state.Apply(delta, nil, nil)
	state.FinalizePendingTools(nil)
	now := int64(1234567890)
	out := state.Snapshot("completed", true, &now)
	if out["usage"] != nil {
		t.Fatalf("expected nil usage for absent, got %v", out["usage"])
	}
}

func TestResponses_ReasoningOnly(t *testing.T) {
	up := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"think\"}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\" more\"}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil, map[string]any{"model": "m"})
	raw := rec.Body.String()
	if fails := strictValidateResponsesContract(t, raw); len(fails) > 0 {
		t.Fatalf("reasoning-only strict failed: %v\nraw:%s", fails, raw)
	}
	completed := completedOf(t, raw)
	outputs, _ := completed["output"].([]any)
	if len(outputs) != 1 {
		t.Fatalf("reasoning-only should have 1 reasoning output, got %d: %v", len(outputs), completed["output"])
	}
	if typ, _ := outputs[0].(map[string]any)["type"].(string); typ != "reasoning" {
		t.Fatalf("reasoning-only output type must be reasoning, got %v", outputs[0])
	}
	for _, o := range outputs {
		if m, _ := o.(map[string]any); m["type"] == "message" {
			t.Fatalf("reasoning-only must not contain fake message")
		}
	}
	events := parseResponsesEvents(t, raw)
	hasAdded, hasPartAdded, hasDelta, hasTextDone, hasPartDone, hasItemDone := false, false, false, false, false, false
	for _, e := range events {
		switch e.event {
		case "response.output_item.added":
			if item, _ := e.data["item"].(map[string]any); item["type"] == "reasoning" {
				hasAdded = true
				if oi, _ := e.data["output_index"].(float64); int(oi) != 0 {
					t.Fatalf("reasoning output_index should be 0, got %v", e.data["output_index"])
				}
			}
		case "response.reasoning_summary_part.added":
			hasPartAdded = true
		case "response.reasoning_summary_text.delta":
			hasDelta = true
		case "response.reasoning_summary_text.done":
			hasTextDone = true
		case "response.reasoning_summary_part.done":
			hasPartDone = true
		case "response.output_item.done":
			if item, _ := e.data["item"].(map[string]any); item["type"] == "reasoning" {
				hasItemDone = true
			}
		}
	}
	if !hasAdded || !hasPartAdded || !hasDelta || !hasTextDone || !hasPartDone || !hasItemDone {
		t.Fatalf("reasoning lifecycle incomplete: added=%v partAdded=%v delta=%v textDone=%v partDone=%v itemDone=%v", hasAdded, hasPartAdded, hasDelta, hasTextDone, hasPartDone, hasItemDone)
	}
	if rs, _ := outputs[0].(map[string]any); true {
		summary, _ := rs["summary"].([]any)
		if len(summary) != 1 {
			t.Fatalf("reasoning summary len want 1 got %v", summary)
		}
		if txt, _ := summary[0].(map[string]any)["text"].(string); txt != "think more" {
			t.Fatalf("reasoning summary text want 'think more' got %q", txt)
		}
	}
	if strings.Contains(raw, "data: [DONE]") {
		t.Fatalf("reasoning-only must not emit [DONE]")
	}
}

func TestResponses_ReasoningTextInterleaved(t *testing.T) {
	up := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"r1\"}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"t1\"}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"r2\"}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"t2\"}}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil, map[string]any{"model": "m"})
	raw := rec.Body.String()
	if fails := strictValidateResponsesContract(t, raw); len(fails) > 0 {
		t.Fatalf("interleaved strict failed: %v", fails)
	}
	events := parseResponsesEvents(t, raw)
	var rsIdx, msgIdx *int
	for _, e := range events {
		if e.event == "response.output_item.added" {
			if item, _ := e.data["item"].(map[string]any); item["type"] == "reasoning" {
				v := int(e.data["output_index"].(float64))
				rsIdx = &v
			}
			if item, _ := e.data["item"].(map[string]any); item["type"] == "message" {
				v := int(e.data["output_index"].(float64))
				msgIdx = &v
			}
		}
	}
	if rsIdx == nil || msgIdx == nil {
		t.Fatalf("interleaved missing reasoning or message added: rsIdx=%v msgIdx=%v", rsIdx, msgIdx)
	}
	if *rsIdx != 0 || *msgIdx != 1 {
		t.Fatalf("interleaved indices wrong: reasoning %d message %d want 0,1", *rsIdx, *msgIdx)
	}
	var rsID, msgID string
	for _, e := range events {
		if e.event == "response.output_item.added" {
			if item, _ := e.data["item"].(map[string]any); item["type"] == "reasoning" {
				rsID, _ = item["id"].(string)
			}
			if item, _ := e.data["item"].(map[string]any); item["type"] == "message" {
				msgID, _ = item["id"].(string)
			}
		}
	}
	for _, e := range events {
		if e.event == "response.reasoning_summary_text.delta" {
			if id, _ := e.data["item_id"].(string); id != rsID {
				t.Fatalf("reasoning delta item_id mismatch %q vs %q", id, rsID)
			}
			if oi, _ := e.data["output_index"].(float64); int(oi) != *rsIdx {
				t.Fatalf("reasoning delta output_index mismatch")
			}
		}
		if e.event == "response.output_text.delta" {
			if id, _ := e.data["item_id"].(string); id != msgID {
				t.Fatalf("text delta item_id mismatch %q vs %q", id, msgID)
			}
			if oi, _ := e.data["output_index"].(float64); int(oi) != *msgIdx {
				t.Fatalf("text delta output_index mismatch")
			}
		}
	}
	completed := completedOf(t, raw)
	outputs, _ := completed["output"].([]any)
	if len(outputs) != 2 {
		t.Fatalf("interleaved completed should have 2 outputs, got %d", len(outputs))
	}
	if typ, _ := outputs[0].(map[string]any)["type"].(string); typ != "reasoning" {
		t.Fatalf("first output should be reasoning, got %v", outputs[0])
	}
	if typ, _ := outputs[1].(map[string]any)["type"].(string); typ != "message" {
		t.Fatalf("second output should be message, got %v", outputs[1])
	}
}

func TestResponses_ReasoningAliasDeltaReasoning(t *testing.T) {
	up := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning\":\"alias think\"}}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil, map[string]any{"model": "m"})
	raw := rec.Body.String()
	if fails := strictValidateResponsesContract(t, raw); len(fails) > 0 {
		t.Fatalf("alias strict failed: %v", fails)
	}
	completed := completedOf(t, raw)
	outputs, _ := completed["output"].([]any)
	if len(outputs) != 1 || outputs[0].(map[string]any)["type"] != "reasoning" {
		t.Fatalf("alias reasoning should produce reasoning output, got %v", completed["output"])
	}
	summary, _ := outputs[0].(map[string]any)["summary"].([]any)
	if len(summary) == 0 || summary[0].(map[string]any)["text"] != "alias think" {
		t.Fatalf("alias text wrong: %v", summary)
	}
}

// Chat must not output reasoning alias, Responses must.
func TestResponses_ReasoningAliasIsolation(t *testing.T) {
	// Chat path uses streamDeltaFromChoice which must ignore reasoning
	choice := map[string]any{
		"delta": map[string]any{
			"content":           "hi",
			"reasoning_content": "should-be-ignored-in-chat",
			"reasoning":         "also-ignored",
		},
	}
	delta := streamDeltaFromChoice(choice)
	if _, has := delta["reasoning_content"]; has {
		t.Fatalf("Chat streamDeltaFromChoice must not contain reasoning_content, got %v", delta)
	}
	if _, has := delta["reasoning"]; has {
		t.Fatalf("Chat streamDeltaFromChoice must not contain reasoning, got %v", delta)
	}
	if c, _ := delta["content"].(string); c != "hi" {
		t.Fatalf("Chat delta content lost, got %v", delta)
	}
	// Responses path must capture reasoning even when same chunk has content
	choice2 := map[string]any{
		"delta": map[string]any{
			"content":           "hello",
			"reasoning_content": "think",
		},
	}
	rdelta := responsesDeltaFromChoice(choice2)
	if r, _ := rdelta["reasoning_content"].(string); r != "think" {
		t.Fatalf("Responses delta must capture reasoning_content, got %v", rdelta)
	}
	if c, _ := rdelta["content"].(string); c != "hello" {
		t.Fatalf("Responses delta must keep content when reasoning present, got %v", rdelta)
	}
	// also test that chatStreamToResponses actually emits both reasoning and content from same chunk
	up := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"t1\",\"reasoning_content\":\"r1\"}}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil, map[string]any{"model": "m"})
	raw := rec.Body.String()
	if fails := strictValidateResponsesContract(t, raw); len(fails) > 0 {
		t.Fatalf("same-chunk reasoning+content strict failed: %v", fails)
	}
	completed := completedOf(t, raw)
	outputs, _ := completed["output"].([]any)
	foundReasoning, foundMessage := false, false
	for _, o := range outputs {
		if m, _ := o.(map[string]any); m["type"] == "reasoning" {
			foundReasoning = true
			summary, _ := m["summary"].([]any)
			if len(summary) > 0 {
				if txt, _ := summary[0].(map[string]any)["text"].(string); txt != "r1" {
					t.Fatalf("same-chunk reasoning text want r1 got %q", txt)
				}
			}
		}
		if m, _ := o.(map[string]any); m["type"] == "message" {
			foundMessage = true
		}
	}
	if !foundReasoning || !foundMessage {
		t.Fatalf("same-chunk must produce both reasoning and message, got foundReasoning=%v foundMessage=%v outputs=%v", foundReasoning, foundMessage, outputs)
	}
}

func TestResponses_SameNameDoubleToolUniqueID(t *testing.T) {
	up := testUpstream(
		"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_A\",\"function\":{\"name\":\"same\",\"arguments\":\"{\\\"a\\\":1}\"}}]}}]}\n\n" +
			"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":1,\"id\":\"call_B\",\"function\":{\"name\":\"same\",\"arguments\":\"{\\\"b\\\":2}\"}}]}}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil, map[string]any{"model": "m"})
	raw := rec.Body.String()
	if fails := strictValidateResponsesContract(t, raw); len(fails) > 0 {
		t.Fatalf("same-name strict failed: %v", fails)
	}
	events := parseResponsesEvents(t, raw)
	ids := map[string]bool{}
	for _, e := range events {
		if e.event == "response.output_item.added" {
			if item, _ := e.data["item"].(map[string]any); item["type"] == "function_call" {
				id, _ := item["id"].(string)
				if ids[id] {
					t.Fatalf("duplicate function_call id for same name: %q", id)
				}
				ids[id] = true
			}
		}
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 unique function_call ids, got %d", len(ids))
	}
	completed := completedOf(t, raw)
	outputs, _ := completed["output"].([]any)
	if len(outputs) != 2 {
		t.Fatalf("expected 2 outputs, got %d", len(outputs))
	}
	cids := map[string]bool{}
	for _, o := range outputs {
		if m, _ := o.(map[string]any); m["type"] == "function_call" {
			id, _ := m["id"].(string)
			if cids[id] {
				t.Fatalf("duplicate completed id %q", id)
			}
			cids[id] = true
			if cid, _ := m["call_id"].(string); cid == "" {
				t.Fatalf("call_id must be non-empty")
			}
		}
	}
}

func TestResponses_ToolCallMissingCallIDGeneratesUnique(t *testing.T) {
	up := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"fn\",\"arguments\":\"{}\"}}]}}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil, map[string]any{"model": "m"})
	raw := rec.Body.String()
	if fails := strictValidateResponsesContract(t, raw); len(fails) > 0 {
		t.Fatalf("missing call_id strict failed: %v", fails)
	}
	completed := completedOf(t, raw)
	outputs, _ := completed["output"].([]any)
	if len(outputs) != 1 {
		t.Fatalf("expected 1 tool output, got %d", len(outputs))
	}
	if fc, _ := outputs[0].(map[string]any); fc["call_id"] == "" {
		t.Fatalf("generated call_id should be non-empty")
	}
}

func TestResponses_ToolArgsBeforeName(t *testing.T) {
	// args arrive before name: first chunk has arguments only, second has name
	up := testUpstream(
		"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"a\\\":1\"}}]}}]}\n\n" +
			"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"myfunc\",\"arguments\":\"}\"}}]}}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil, map[string]any{"model": "m"})
	raw := rec.Body.String()
	if fails := strictValidateResponsesContract(t, raw); len(fails) > 0 {
		t.Fatalf("args-before-name strict failed: %v", fails)
	}
	events := parseResponsesEvents(t, raw)
	// find added and deltas for the tool
	var addedOI *int
	var addedID string
	var deltas []string
	for _, e := range events {
		if e.event == "response.output_item.added" {
			if item, _ := e.data["item"].(map[string]any); item["type"] == "function_call" {
				v := int(e.data["output_index"].(float64))
				addedOI = &v
				addedID, _ = item["id"].(string)
			}
		}
		if e.event == "response.function_call_arguments.delta" {
			deltas = append(deltas, e.data["delta"].(string))
		}
	}
	if addedOI == nil {
		t.Fatalf("tool added not found")
	}
	if len(deltas) == 0 {
		t.Fatalf("expected at least one delta for args-before-name")
	}
	// after fix, first delta should be merged args from both fragments: {"a":1} plus "}"
	// we split into two fragments: first {"a":1  (no name), second "}\", merged should be {"a":1}
	// total accumulated must equal completed arguments
	completed := completedOf(t, raw)
	outputs, _ := completed["output"].([]any)
	if len(outputs) != 1 {
		t.Fatalf("expected 1 output, got %v", outputs)
	}
	args, _ := outputs[0].(map[string]any)["arguments"].(string)
	joined := strings.Join(deltas, "")
	if joined != args {
		t.Fatalf("delta拼接 must equal arguments: deltas %q joined %q != args %q", deltas, joined, args)
	}
	if args != "{\"a\":1}" {
		t.Fatalf("args want {\"a\":1} got %q", args)
	}
	// check that no added was emitted before name: output_index should be 0 and there should be exactly 1 delta that contains the merged content (or at least first delta contains merged)
	if len(deltas) != 1 {
		t.Fatalf("args-before-name should produce single merged delta after name, got %d deltas %v", len(deltas), deltas)
	}
	if addedID == "" {
		t.Fatalf("missing added id")
	}
	// ensure added output_index is 0 (since no other items)
	if *addedOI != 0 {
		t.Fatalf("tool output_index should be 0, got %d", *addedOI)
	}
}

func TestResponses_ToolArgsBeforeName_WithInterleavedMessageReasoning(t *testing.T) {
	// args before name interleaved with message and reasoning
	up := testUpstream(
		"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"{\\\"x\\\":1\"}}]}}]}\n\n" +
			"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\n" +
			"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"think\"}}]}\n\n" +
			"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"fn\",\"arguments\":\"}\"}}]}}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil, map[string]any{"model": "m"})
	raw := rec.Body.String()
	if fails := strictValidateResponsesContract(t, raw); len(fails) > 0 {
		t.Fatalf("interleaved args-before-name strict failed: %v", fails)
	}
	events := parseResponsesEvents(t, raw)
	// ensure order: message should be index 0, reasoning index 1, tool index 2 (since tool name appears last)
	var order []string
	for _, e := range events {
		if e.event == "response.output_item.added" {
			if item, _ := e.data["item"].(map[string]any); item != nil {
				if typ, _ := item["type"].(string); typ != "" {
					order = append(order, typ+":"+itoa(int(e.data["output_index"].(float64))))
				}
			}
		}
	}
	// expected tool last
	if len(order) != 3 {
		t.Fatalf("expected 3 added items, got %v", order)
	}
	if order[0] != "message:0" || order[1] != "reasoning:1" || order[2] != "function_call:2" {
		t.Fatalf("interleaved order wrong, got %v want [message:0 reasoning:1 function_call:2]", order)
	}
	// verify tool args merged
	completed := completedOf(t, raw)
	outputs, _ := completed["output"].([]any)
	var toolArgs string
	for _, o := range outputs {
		if m, _ := o.(map[string]any); m["type"] == "function_call" {
			toolArgs, _ = m["arguments"].(string)
		}
	}
	if toolArgs != "{\"x\":1}" {
		t.Fatalf("tool args want {\"x\":1} got %q", toolArgs)
	}
	// verify deltas for tool is single merged after name
	var deltas []string
	for _, e := range events {
		if e.event == "response.function_call_arguments.delta" {
			deltas = append(deltas, e.data["delta"].(string))
		}
	}
	if len(deltas) != 1 || deltas[0] != "{\"x\":1}" {
		t.Fatalf("merged delta wrong, got %v", deltas)
	}
}

func TestResponses_IDConcurrentUnique(t *testing.T) {
	const n = 1000
	ids := make([]string, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			ids[idx] = responsesNewID("resp_")
		}(i)
	}
	wg.Wait()
	seen := map[string]bool{}
	for _, id := range ids {
		if id == "" {
			t.Fatalf("empty id generated")
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
	// multi-request uniqueness: generate via two chatStreamToResponses calls
	up1 := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n")
	rec1 := httptest.NewRecorder()
	chatStreamToResponses(rec1, up1, nil, map[string]any{"model": "m"})
	up2 := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n")
	rec2 := httptest.NewRecorder()
	chatStreamToResponses(rec2, up2, nil, map[string]any{"model": "m"})
	id1 := completedOf(t, rec1.Body.String())["id"].(string)
	id2 := completedOf(t, rec2.Body.String())["id"].(string)
	if id1 == id2 {
		t.Fatalf("multi-request resp IDs must be unique, both %q", id1)
	}
	// also ensure msg IDs unique per request
	events1 := parseResponsesEvents(t, rec1.Body.String())
	events2 := parseResponsesEvents(t, rec2.Body.String())
	var msg1, msg2 string
	for _, e := range events1 {
		if e.event == "response.output_item.added" {
			if item, _ := e.data["item"].(map[string]any); item["type"] == "message" {
				msg1, _ = item["id"].(string)
			}
		}
	}
	for _, e := range events2 {
		if e.event == "response.output_item.added" {
			if item, _ := e.data["item"].(map[string]any); item["type"] == "message" {
				msg2, _ = item["id"].(string)
			}
		}
	}
	if msg1 != "" && msg2 != "" && msg1 == msg2 {
		t.Fatalf("multi-request msg IDs must be unique, both %q", msg1)
	}
}

func TestResponses_MultiRequestToolIDsUnique(t *testing.T) {
	up := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"fn\",\"arguments\":\"{}\"}}]}}]}\n\n")
	rec1 := httptest.NewRecorder()
	chatStreamToResponses(rec1, up, nil, map[string]any{"model": "m"})
	rec2 := httptest.NewRecorder()
	up2 := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"fn\",\"arguments\":\"{}\"}}]}}]}\n\n")
	chatStreamToResponses(rec2, up2, nil, map[string]any{"model": "m"})
	c1 := completedOf(t, rec1.Body.String())
	c2 := completedOf(t, rec2.Body.String())
	o1, _ := c1["output"].([]any)
	o2, _ := c2["output"].([]any)
	if len(o1) == 0 || len(o2) == 0 {
		t.Fatalf("missing tool output")
	}
	id1, _ := o1[0].(map[string]any)["id"].(string)
	id2, _ := o2[0].(map[string]any)["id"].(string)
	if id1 == id2 {
		t.Fatalf("tool fc IDs across requests must be unique, both %q", id1)
	}
}

func TestResponses_TextFirstReasoningLater(t *testing.T) {
	// text-first → reasoning-later: ensures message index 0, reasoning index 1 and done order matches
	up := testUpstream(
		"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\n" +
			"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"think later\"}}]}\n\n" +
			"data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"}}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil, map[string]any{"model": "m"})
	raw := rec.Body.String()
	if fails := strictValidateResponsesContract(t, raw); len(fails) > 0 {
		t.Fatalf("text-first strict failed: %v\nraw:%s", fails, raw)
	}
	events := parseResponsesEvents(t, raw)
	var msgIdx, rsIdx *int
	var msgID, rsID string
	for _, e := range events {
		if e.event == "response.output_item.added" {
			if item, _ := e.data["item"].(map[string]any); item["type"] == "message" {
				v := int(e.data["output_index"].(float64))
				msgIdx = &v
				msgID, _ = item["id"].(string)
			}
			if item, _ := e.data["item"].(map[string]any); item["type"] == "reasoning" {
				v := int(e.data["output_index"].(float64))
				rsIdx = &v
				rsID, _ = item["id"].(string)
			}
		}
	}
	if msgIdx == nil || rsIdx == nil {
		t.Fatalf("text-first missing message or reasoning added: msgIdx=%v rsIdx=%v", msgIdx, rsIdx)
	}
	if *msgIdx != 0 || *rsIdx != 1 {
		t.Fatalf("text-first indices wrong: message %d reasoning %d want 0,1", *msgIdx, *rsIdx)
	}
	// done order must be message then reasoning (0 then 1)
	var doneOrder []int
	for _, e := range events {
		if e.event == "response.output_item.done" {
			doneOrder = append(doneOrder, int(e.data["output_index"].(float64)))
		}
	}
	if len(doneOrder) != 2 || doneOrder[0] != 0 || doneOrder[1] != 1 {
		t.Fatalf("text-first done order must be [0 1], got %v", doneOrder)
	}
	// completed output must be sorted same
	completed := completedOf(t, raw)
	outputs, _ := completed["output"].([]any)
	if len(outputs) != 2 {
		t.Fatalf("text-first completed should have 2 outputs, got %d", len(outputs))
	}
	if typ, _ := outputs[0].(map[string]any)["type"].(string); typ != "message" {
		t.Fatalf("first completed should be message, got %v", outputs[0])
	}
	if typ, _ := outputs[1].(map[string]any)["type"].(string); typ != "reasoning" {
		t.Fatalf("second completed should be reasoning, got %v", outputs[1])
	}
	_ = msgID
	_ = rsID
	// also ensure per-class subevent order preserved via validator (already strict)
}
