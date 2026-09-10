package app

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
)

// strict contract validator for OpenAI Responses SSE.
//
// Covers the task's minimum requirements:
// - every listed lifecycle event carries integer sequence_number strictly increasing from 0 contiguous;
// - response.created / in_progress / completed carry full Response base fields
//   (id,object,created_at,error,incomplete_details,instructions,model,tools,output,
//    parallel_tool_calls,metadata,tool_choice,temperature,top_p plus status;
//    completed additionally completed_at+usage with details);
// - output_text.delta / output_text.done carry logprobs array;
// - reasoning lifecycle added/part/delta/done consistent;
// - added/delta/done item_id/output_index/content_index/summary_index consistent;
// - data.type equals SSE event line;
// - forbidden [DONE] sentinel.

var strictRequiredResponseFields = []string{
	"id", "object", "created_at", "error", "incomplete_details", "instructions",
	"model", "tools", "output", "parallel_tool_calls", "metadata", "tool_choice",
	"temperature", "top_p", "status",
}

var strictLifecycleEvents = map[string]bool{
	"response.created":                       true,
	"response.in_progress":                   true,
	"response.output_item.added":             true,
	"response.content_part.added":            true,
	"response.output_text.delta":             true,
	"response.output_text.done":              true,
	"response.content_part.done":             true,
	"response.output_item.done":              true,
	"response.completed":                     true,
	"response.function_call_arguments.delta": true,
	"response.function_call_arguments.done":  true,
	"response.reasoning_summary_text.delta":  true,
	"response.reasoning_summary_text.done":   true,
	"response.reasoning_summary_part.added":  true,
	"response.reasoning_summary_part.done":   true,
}

func strictValidateResponsesContract(t *testing.T, raw string) []string {
	t.Helper()
	var failures []string

	if strings.Contains(raw, "data: [DONE]") || strings.Contains(raw, "[DONE]") && strings.Contains(raw, "data:") {
		for _, line := range strings.Split(raw, "\n") {
			if strings.TrimSpace(line) == "data: [DONE]" {
				failures = append(failures, "forbidden sentinel: Responses stream must not emit `data: [DONE]` (terminate via response.completed)")
				break
			}
		}
	}

	events := parseResponsesEvents(t, raw)
	if len(events) == 0 {
		failures = append(failures, "no SSE events parsed (expected at least response.created/completed)")
		return failures
	}

	// event -> data.type consistency + sequence_number monotonic contiguous from 0
	prevSeq := -1
	seenSeq := false
	for idx, ev := range events {
		dtype, _ := ev.data["type"].(string)
		if dtype == "" {
			failures = append(failures, "event["+ev.event+"] index "+itoa(idx)+" missing data.type")
		} else if dtype != ev.event {
			failures = append(failures, "event["+ev.event+"] data.type mismatch: got "+dtype)
		}
		if strictLifecycleEvents[ev.event] {
			seqRaw, ok := ev.data["sequence_number"]
			if !ok {
				failures = append(failures, "event["+ev.event+"] index "+itoa(idx)+" missing integer sequence_number")
				continue
			}
			f, ok := seqRaw.(float64)
			if !ok {
				failures = append(failures, "event["+ev.event+"] index "+itoa(idx)+" sequence_number not numeric: "+typeName(seqRaw))
				continue
			}
			if f != float64(int(f)) {
				failures = append(failures, "event["+ev.event+"] index "+itoa(idx)+" sequence_number not integer: "+jsonVal(seqRaw))
				continue
			}
			seq := int(f)
			if !seenSeq {
				if seq != 0 {
					failures = append(failures, "event["+ev.event+"] index "+itoa(idx)+" sequence_number must start at 0, got "+itoa(seq))
				}
			} else {
				if seq != prevSeq+1 {
					failures = append(failures, "event["+ev.event+"] index "+itoa(idx)+" sequence_number not contiguous: expected "+itoa(prevSeq+1)+" got "+itoa(seq))
				}
			}
			prevSeq = seq
			seenSeq = true
		}
	}

	// response.created / in_progress / completed base fields + type/value checks
	for _, ev := range events {
		if ev.event == "response.created" || ev.event == "response.in_progress" || ev.event == "response.completed" {
			respRaw, ok := ev.data["response"]
			if !ok {
				failures = append(failures, "event["+ev.event+"] missing response object")
				continue
			}
			resp, ok := respRaw.(map[string]any)
			if !ok {
				failures = append(failures, "event["+ev.event+"] response not object")
				continue
			}
			for _, field := range strictRequiredResponseFields {
				if _, exists := resp[field]; !exists {
					failures = append(failures, "event["+ev.event+"] response missing required field: "+field)
				}
			}
			// type/value checks
			if id, _ := resp["id"].(string); id == "" {
				failures = append(failures, "event["+ev.event+"] response.id must be non-empty string")
			}
			if obj, _ := resp["object"].(string); obj != "response" {
				failures = append(failures, "event["+ev.event+"] response.object must be 'response', got "+jsonVal(resp["object"]))
			}
			if ca, ok := resp["created_at"].(float64); !ok || ca <= 0 || ca != float64(int(ca)) {
				failures = append(failures, "event["+ev.event+"] response.created_at must be positive integer, got "+jsonVal(resp["created_at"]))
			}
			if m, _ := resp["model"].(string); m == "" {
				failures = append(failures, "event["+ev.event+"] response.model must be non-empty string (request model)")
			}
			if _, ok := resp["tools"].([]any); !ok {
				if resp["tools"] != nil {
					failures = append(failures, "event["+ev.event+"] response.tools must be array, got "+typeName(resp["tools"]))
				}
			}
			// allow metadata null or object
			if md := resp["metadata"]; md != nil {
				if _, ok := md.(map[string]any); !ok {
					failures = append(failures, "event["+ev.event+"] response.metadata must be null or object, got "+typeName(md))
				}
			}
			if _, ok := resp["parallel_tool_calls"].(bool); !ok {
				failures = append(failures, "event["+ev.event+"] response.parallel_tool_calls must be bool, got "+typeName(resp["parallel_tool_calls"]))
			}
			if out, ok := resp["output"].([]any); !ok {
				failures = append(failures, "event["+ev.event+"] response.output must be array, got "+typeName(resp["output"]))
			} else {
				_ = out
			}
			if ev.event == "response.created" || ev.event == "response.in_progress" {
				if st, _ := resp["status"].(string); st != "in_progress" {
					failures = append(failures, "event["+ev.event+"] response.status must be 'in_progress', got "+jsonVal(resp["status"]))
				}
				// created/in_progress may have no usage or nil usage – allow but if present validate details
				if u, exists := resp["usage"]; exists && u != nil {
					if _, ok := u.(map[string]any); !ok {
						failures = append(failures, "event["+ev.event+"] response.usage must be object or nil")
					}
				}
			}
			if ev.event == "response.completed" {
				if st, _ := resp["status"].(string); st != "completed" {
					failures = append(failures, "event[response.completed] response.status must be 'completed', got "+jsonVal(resp["status"]))
				}
				if ca, ok := resp["completed_at"].(float64); !ok || ca <= 0 {
					failures = append(failures, "event[response.completed] response missing/invalid completed_at: "+jsonVal(resp["completed_at"]))
				}
				if _, exists := resp["usage"]; !exists {
					failures = append(failures, "event[response.completed] response missing required field: usage")
				} else {
					usageRaw := resp["usage"]
					if usageRaw == nil {
						// allowed nil when no upstream usage
					} else {
						usage, ok := usageRaw.(map[string]any)
						if !ok {
							failures = append(failures, "event[response.completed] usage not object")
						} else {
							for _, uf := range []string{"input_tokens", "output_tokens", "total_tokens"} {
								if _, exists := usage[uf]; !exists {
									failures = append(failures, "event[response.completed] usage missing field: "+uf)
								} else {
									if _, ok := usage[uf].(float64); !ok {
										failures = append(failures, "event[response.completed] usage."+uf+" must be number")
									}
								}
							}
							// details: official only cache_write_tokens
							if itd, ok := usage["input_tokens_details"].(map[string]any); !ok {
								failures = append(failures, "event[response.completed] usage missing input_tokens_details object")
							} else {
								if _, ok := itd["cached_tokens"]; !ok {
									failures = append(failures, "event[response.completed] usage.input_tokens_details missing cached_tokens")
								} else if _, ok := itd["cached_tokens"].(float64); !ok {
									failures = append(failures, "event[response.completed] usage.input_tokens_details.cached_tokens must be number")
								}
								if _, ok := itd["cache_write_tokens"]; !ok {
									failures = append(failures, "event[response.completed] usage.input_tokens_details missing cache_write_tokens")
								} else if _, ok := itd["cache_write_tokens"].(float64); !ok {
									failures = append(failures, "event[response.completed] usage.input_tokens_details.cache_write_tokens must be number")
								}
								if _, hasAlias := itd["cache_creation_input_tokens"]; hasAlias {
									failures = append(failures, "event[response.completed] usage.input_tokens_details must not contain alias cache_creation_input_tokens (use cache_write_tokens only)")
								}
							}
							if otd, ok := usage["output_tokens_details"].(map[string]any); !ok {
								failures = append(failures, "event[response.completed] usage missing output_tokens_details object")
							} else {
								if _, ok := otd["reasoning_tokens"]; !ok {
									failures = append(failures, "event[response.completed] usage.output_tokens_details missing reasoning_tokens")
								} else if _, ok := otd["reasoning_tokens"].(float64); !ok {
									failures = append(failures, "event[response.completed] usage.output_tokens_details.reasoning_tokens must be number")
								}
							}
						}
					}
				}
			}
		}
	}

	// output_text.delta/done must carry logprobs array
	for _, ev := range events {
		if ev.event == "response.output_text.delta" || ev.event == "response.output_text.done" {
			lp, ok := ev.data["logprobs"]
			if !ok {
				failures = append(failures, "event["+ev.event+"] missing logprobs array")
				continue
			}
			if _, ok := lp.([]any); !ok {
				if lp != nil {
					failures = append(failures, "event["+ev.event+"] logprobs not array: "+typeName(lp))
				} else {
					failures = append(failures, "event["+ev.event+"] logprobs is null, expected []")
				}
			}
		}
	}

	// --- lifecycle single-pass validation ---
	// Track added items in order, enforce continuity 0..n-1, and that any delta/part/done comes after added.
	outputIndexToItemID := map[int]string{}
	outputIndexToType := map[int]string{}
	addedOrder := []int{}
	addedSet := map[int]bool{}
	// per-item accumulation for reconstruction
	msgAccum := map[int]string{}
	rsAccum := map[int]string{}
	fcAccumByIndex := map[int]string{}
	fcIDByIndex := map[int]string{}
	// also track part added
	msgPartAddedByIndex := map[int]bool{}
	rsPartAddedByIndex := map[int]bool{}
	// map item_id -> output_index for delta validation
	itemIDToIndex := map[string]int{}

	var messageID string
	var messageOutputIndex *int
	var reasoningID string
	var reasoningOutputIndex *int

	for evIdx, ev := range events {
		switch ev.event {
		case "response.output_item.added":
			oiRaw, _ := ev.data["output_index"]
			oiFloat, ok := oiRaw.(float64)
			if !ok {
				failures = append(failures, "event[response.output_item.added] index "+itoa(evIdx)+" output_index not number")
				continue
			}
			oi := int(oiFloat)
			if addedSet[oi] {
				failures = append(failures, "duplicate output_index "+itoa(oi)+" in added")
			}
			addedSet[oi] = true
			addedOrder = append(addedOrder, oi)
			itemRaw, _ := ev.data["item"].(map[string]any)
			if itemRaw == nil {
				failures = append(failures, "event[response.output_item.added] missing item object")
				continue
			}
			id, _ := itemRaw["id"].(string)
			if id == "" {
				failures = append(failures, "event[response.output_item.added] item missing id")
				continue
			}
			itype, _ := itemRaw["type"].(string)
			if itype == "" {
				failures = append(failures, "event[response.output_item.added] item missing type")
			}
			if _, exists := outputIndexToItemID[oi]; exists {
				// already reported duplicate
			}
			outputIndexToItemID[oi] = id
			outputIndexToType[oi] = itype
			itemIDToIndex[id] = oi
			// check duplicate id across different indices
			for prevIdx, prevID := range outputIndexToItemID {
				if prevIdx != oi && prevID == id {
					failures = append(failures, "duplicate item id "+id+" at indices "+itoa(prevIdx)+" and "+itoa(oi))
				}
			}
			if itype == "message" {
				messageID = id
				cpy := oi
				messageOutputIndex = &cpy
				if status, _ := itemRaw["status"].(string); status != "in_progress" {
					failures = append(failures, "message added status must be in_progress")
				}
			}
			if itype == "reasoning" {
				reasoningID = id
				cpy := oi
				reasoningOutputIndex = &cpy
				if summary, ok := itemRaw["summary"].([]any); !ok || len(summary) != 0 {
					if summary != nil && len(summary) != 0 {
						failures = append(failures, "reasoning added summary must be [], got "+jsonVal(summary))
					}
				}
				if status, _ := itemRaw["status"].(string); status != "in_progress" {
					failures = append(failures, "reasoning added status must be in_progress")
				}
			}
			if itype == "function_call" {
				if _, ok := itemRaw["call_id"]; !ok {
					failures = append(failures, "function_call added missing call_id")
				} else if cid, _ := itemRaw["call_id"].(string); cid == "" {
					failures = append(failures, "function_call added call_id must be non-empty")
				}
				if _, ok := itemRaw["name"].(string); !ok {
					failures = append(failures, "function_call added missing name")
				}
				fcIDByIndex[oi] = id
				fcAccumByIndex[oi] = ""
			}
		case "response.content_part.added":
			oiRaw, _ := ev.data["output_index"]
			oiFloat, ok := oiRaw.(float64)
			if !ok {
				failures = append(failures, "event[response.content_part.added] index "+itoa(evIdx)+" output_index not number")
				continue
			}
			oi := int(oiFloat)
			if !addedSet[oi] {
				failures = append(failures, "event[response.content_part.added] orphan: output_index "+itoa(oi)+" not yet added (must be after added)")
			}
			itemID, _ := ev.data["item_id"].(string)
			ciRaw, _ := ev.data["content_index"]
			if messageID != "" && itemID != messageID {
				failures = append(failures, "event[response.content_part.added] item_id mismatch: got "+itemID+" want "+messageID)
			}
			if messageOutputIndex != nil {
				if oi2, ok := oiRaw.(float64); !ok || int(oi2) != *messageOutputIndex {
					failures = append(failures, "event[response.content_part.added] output_index mismatch: got "+jsonVal(oiRaw)+" want "+itoa(*messageOutputIndex))
				}
			}
			if ci, ok := ciRaw.(float64); !ok || int(ci) != 0 {
				failures = append(failures, "event[response.content_part.added] content_index mismatch: expected 0 got "+jsonVal(ciRaw))
			}
			if want, exists := outputIndexToItemID[oi]; exists && want != itemID {
				failures = append(failures, "event[response.content_part.added] output_index->item.id mismatch: index "+itoa(oi)+" maps to "+want+" but item_id "+itemID)
			}
			msgPartAddedByIndex[oi] = true
		case "response.output_text.delta":
			oiRaw, _ := ev.data["output_index"]
			oiFloat, ok := oiRaw.(float64)
			if !ok {
				failures = append(failures, "event[response.output_text.delta] index "+itoa(evIdx)+" output_index not number")
				continue
			}
			oi := int(oiFloat)
			if !addedSet[oi] {
				failures = append(failures, "event[response.output_text.delta] orphan delta: output_index "+itoa(oi)+" not yet added")
			}
			itemID, _ := ev.data["item_id"].(string)
			ciRaw, _ := ev.data["content_index"]
			if messageID != "" && itemID != messageID {
				failures = append(failures, "event[response.output_text.delta] item_id mismatch: got "+itemID+" want "+messageID)
			} else if messageID == "" {
				failures = append(failures, "event[response.output_text.delta] got text delta but no message item added")
			}
			if messageOutputIndex != nil {
				if oi2, ok := oiRaw.(float64); !ok || int(oi2) != *messageOutputIndex {
					failures = append(failures, "event[response.output_text.delta] output_index mismatch: got "+jsonVal(oiRaw)+" want "+itoa(*messageOutputIndex))
				}
			}
			if ci, ok := ciRaw.(float64); !ok || int(ci) != 0 {
				failures = append(failures, "event[response.output_text.delta] content_index mismatch: expected 0 got "+jsonVal(ciRaw))
			}
			// accumulate
			if d, ok := ev.data["delta"].(string); ok {
				msgAccum[oi] += d
			} else {
				failures = append(failures, "event[response.output_text.delta] missing delta string")
			}
		case "response.output_text.done":
			oiRaw, _ := ev.data["output_index"]
			oiFloat, ok := oiRaw.(float64)
			if !ok {
				failures = append(failures, "event[response.output_text.done] output_index not number")
				continue
			}
			oi := int(oiFloat)
			if !addedSet[oi] {
				failures = append(failures, "event[response.output_text.done] orphan: output_index "+itoa(oi)+" not yet added")
			}
			if txt, ok := ev.data["text"].(string); ok {
				if acc, exists := msgAccum[oi]; exists && txt != acc {
					failures = append(failures, "event[response.output_text.done] text mismatch: done "+jsonVal(txt)+" != accumulated deltas "+jsonVal(acc))
				}
			} else {
				failures = append(failures, "event[response.output_text.done] missing text string")
			}
			// reuse checks from earlier unified block
			itemID, _ := ev.data["item_id"].(string)
			ciRaw, _ := ev.data["content_index"]
			if messageID != "" && itemID != messageID {
				failures = append(failures, "event[response.output_text.done] item_id mismatch: got "+itemID+" want "+messageID)
			}
			if messageOutputIndex != nil {
				if oi2, ok := oiRaw.(float64); !ok || int(oi2) != *messageOutputIndex {
					failures = append(failures, "event[response.output_text.done] output_index mismatch: got "+jsonVal(oiRaw)+" want "+itoa(*messageOutputIndex))
				}
			}
			if ci, ok := ciRaw.(float64); !ok || int(ci) != 0 {
				failures = append(failures, "event[response.output_text.done] content_index mismatch: expected 0 got "+jsonVal(ciRaw))
			}
		case "response.content_part.done":
			oiRaw, _ := ev.data["output_index"]
			oiFloat, ok := oiRaw.(float64)
			if !ok {
				failures = append(failures, "event[response.content_part.done] output_index not number")
				continue
			}
			oi := int(oiFloat)
			if !addedSet[oi] {
				failures = append(failures, "event[response.content_part.done] orphan: output_index "+itoa(oi)+" not yet added")
			}
			itemID, _ := ev.data["item_id"].(string)
			ciRaw, _ := ev.data["content_index"]
			if messageID != "" && itemID != messageID {
				failures = append(failures, "event[response.content_part.done] item_id mismatch: got "+itemID+" want "+messageID)
			}
			if messageOutputIndex != nil {
				if oi2, ok := oiRaw.(float64); !ok || int(oi2) != *messageOutputIndex {
					failures = append(failures, "event[response.content_part.done] output_index mismatch: got "+jsonVal(oiRaw)+" want "+itoa(*messageOutputIndex))
				}
			}
			if ci, ok := ciRaw.(float64); !ok || int(ci) != 0 {
				failures = append(failures, "event[response.content_part.done] content_index mismatch: expected 0 got "+jsonVal(ciRaw))
			}
			// validate part text matches accumulated
			if part, ok := ev.data["part"].(map[string]any); ok {
				if ptxt, ok := part["text"].(string); ok {
					if acc, exists := msgAccum[oi]; exists && ptxt != acc {
						failures = append(failures, "event[response.content_part.done] part.text mismatch: got "+jsonVal(ptxt)+" want accumulated "+jsonVal(acc))
					}
				}
			}
		case "response.reasoning_summary_part.added":
			oiRaw, _ := ev.data["output_index"]
			oiFloat, ok := oiRaw.(float64)
			if !ok {
				failures = append(failures, "event[response.reasoning_summary_part.added] output_index not number")
				continue
			}
			oi := int(oiFloat)
			if !addedSet[oi] {
				failures = append(failures, "event[response.reasoning_summary_part.added] orphan: output_index "+itoa(oi)+" not yet added")
			}
			itemID, _ := ev.data["item_id"].(string)
			siRaw, _ := ev.data["summary_index"]
			if reasoningID != "" && itemID != reasoningID {
				failures = append(failures, "event[response.reasoning_summary_part.added] item_id mismatch: got "+itemID+" want "+reasoningID)
			} else if reasoningID == "" {
				failures = append(failures, "event[response.reasoning_summary_part.added] no reasoning item added")
			}
			if reasoningOutputIndex != nil {
				if oi2, ok := oiRaw.(float64); !ok || int(oi2) != *reasoningOutputIndex {
					failures = append(failures, "event[response.reasoning_summary_part.added] output_index mismatch: got "+jsonVal(oiRaw)+" want "+itoa(*reasoningOutputIndex))
				}
			}
			if si, ok := siRaw.(float64); !ok || int(si) != 0 {
				failures = append(failures, "event[response.reasoning_summary_part.added] summary_index must be 0, got "+jsonVal(siRaw))
			}
			if part, ok := ev.data["part"].(map[string]any); !ok || part["type"] != "summary_text" {
				failures = append(failures, "event[response.reasoning_summary_part.added] part must be summary_text")
			}
			rsPartAddedByIndex[oi] = true
		case "response.reasoning_summary_text.delta":
			oiRaw, _ := ev.data["output_index"]
			oiFloat, ok := oiRaw.(float64)
			if !ok {
				failures = append(failures, "event[response.reasoning_summary_text.delta] output_index not number")
				continue
			}
			oi := int(oiFloat)
			if !addedSet[oi] {
				failures = append(failures, "event[response.reasoning_summary_text.delta] orphan delta: output_index "+itoa(oi)+" not yet added")
			}
			itemID, _ := ev.data["item_id"].(string)
			siRaw, _ := ev.data["summary_index"]
			if _, ok := ev.data["delta"].(string); !ok {
				failures = append(failures, "event[response.reasoning_summary_text.delta] missing delta string")
			} else {
				if d, ok := ev.data["delta"].(string); ok {
					rsAccum[oi] += d
				}
			}
			if reasoningID != "" && itemID != reasoningID {
				failures = append(failures, "event[response.reasoning_summary_text.delta] item_id mismatch: got "+itemID+" want "+reasoningID)
			} else if reasoningID == "" {
				failures = append(failures, "event[response.reasoning_summary_text.delta] delta without reasoning added")
			}
			if reasoningOutputIndex != nil {
				if oi2, ok := oiRaw.(float64); !ok || int(oi2) != *reasoningOutputIndex {
					failures = append(failures, "event[response.reasoning_summary_text.delta] output_index mismatch: got "+jsonVal(oiRaw)+" want "+itoa(*reasoningOutputIndex))
				}
			}
			if si, ok := siRaw.(float64); !ok || int(si) != 0 {
				failures = append(failures, "event[response.reasoning_summary_text.delta] summary_index must be 0, got "+jsonVal(siRaw))
			}
		case "response.reasoning_summary_text.done":
			oiRaw, _ := ev.data["output_index"]
			oiFloat, ok := oiRaw.(float64)
			if !ok {
				failures = append(failures, "event[response.reasoning_summary_text.done] output_index not number")
				continue
			}
			oi := int(oiFloat)
			if !addedSet[oi] {
				failures = append(failures, "event[response.reasoning_summary_text.done] orphan: output_index "+itoa(oi)+" not yet added")
			}
			txt, ok := ev.data["text"].(string)
			if !ok {
				failures = append(failures, "event[response.reasoning_summary_text.done] missing text string")
			} else {
				if acc, exists := rsAccum[oi]; exists && txt != acc {
					failures = append(failures, "event[response.reasoning_summary_text.done] text mismatch: done "+jsonVal(txt)+" != accumulated "+jsonVal(acc))
				}
			}
			itemID, _ := ev.data["item_id"].(string)
			siRaw, _ := ev.data["summary_index"]
			if reasoningID != "" && itemID != reasoningID {
				failures = append(failures, "event[response.reasoning_summary_text.done] item_id mismatch")
			}
			if reasoningOutputIndex != nil {
				if oi2, ok := oiRaw.(float64); !ok || int(oi2) != *reasoningOutputIndex {
					failures = append(failures, "event[response.reasoning_summary_text.done] output_index mismatch")
				}
			}
			if si, ok := siRaw.(float64); !ok || int(si) != 0 {
				failures = append(failures, "event[response.reasoning_summary_text.done] summary_index must be 0")
			}
		case "response.reasoning_summary_part.done":
			oiRaw, _ := ev.data["output_index"]
			oiFloat, ok := oiRaw.(float64)
			if !ok {
				failures = append(failures, "event[response.reasoning_summary_part.done] output_index not number")
				continue
			}
			oi := int(oiFloat)
			if !addedSet[oi] {
				failures = append(failures, "event[response.reasoning_summary_part.done] orphan: output_index "+itoa(oi)+" not yet added")
			}
			itemID, _ := ev.data["item_id"].(string)
			siRaw, _ := ev.data["summary_index"]
			if reasoningID != "" && itemID != reasoningID {
				failures = append(failures, "event[response.reasoning_summary_part.done] item_id mismatch")
			}
			if reasoningOutputIndex != nil {
				if oi2, ok := oiRaw.(float64); !ok || int(oi2) != *reasoningOutputIndex {
					failures = append(failures, "event[response.reasoning_summary_part.done] output_index mismatch")
				}
			}
			if si, ok := siRaw.(float64); !ok || int(si) != 0 {
				failures = append(failures, "event[response.reasoning_summary_part.done] summary_index must be 0")
			}
			if part, ok := ev.data["part"].(map[string]any); ok {
				if ptxt, ok := part["text"].(string); ok {
					if acc, exists := rsAccum[oi]; exists && ptxt != acc {
						failures = append(failures, "event[response.reasoning_summary_part.done] part.text mismatch: got "+jsonVal(ptxt)+" want "+jsonVal(acc))
					}
				}
			}
		case "response.output_item.done":
			oiRaw, _ := ev.data["output_index"]
			oiFloat, ok := oiRaw.(float64)
			if !ok {
				continue
			}
			oi := int(oiFloat)
			if !addedSet[oi] {
				failures = append(failures, "event[response.output_item.done] orphan: output_index "+itoa(oi)+" not yet added")
			}
			itemRaw, _ := ev.data["item"].(map[string]any)
			if itemRaw == nil {
				continue
			}
			id, _ := itemRaw["id"].(string)
			if want, exists := outputIndexToItemID[oi]; exists && want != id {
				failures = append(failures, "event[response.output_item.done] output_index "+itoa(oi)+" item.id mismatch: added "+want+" vs done "+id)
			}
			if typ, exists := outputIndexToType[oi]; exists {
				if itype, _ := itemRaw["type"].(string); itype != typ {
					failures = append(failures, "event[response.output_item.done] output_index "+itoa(oi)+" type mismatch: added "+typ+" vs done "+itype)
				}
			}
			// per-type done content verification vs accumulated
			if typ, exists := outputIndexToType[oi]; exists {
				switch typ {
				case "message":
					if content, ok := itemRaw["content"].([]any); ok && len(content) > 0 {
						if block, ok := content[0].(map[string]any); ok {
							if txt, ok := block["text"].(string); ok {
								if acc, exists := msgAccum[oi]; exists && txt != acc {
									failures = append(failures, "event[response.output_item.done] message text mismatch: done "+jsonVal(txt)+" != accumulated "+jsonVal(acc))
								}
							}
						}
					} else {
						failures = append(failures, "event[response.output_item.done] message missing content")
					}
				case "reasoning":
					if summary, ok := itemRaw["summary"].([]any); ok && len(summary) > 0 {
						if block, ok := summary[0].(map[string]any); ok {
							if txt, ok := block["text"].(string); ok {
								if acc, exists := rsAccum[oi]; exists && txt != acc {
									failures = append(failures, "event[response.output_item.done] reasoning summary mismatch: done "+jsonVal(txt)+" != accumulated "+jsonVal(acc))
								}
							}
						}
					}
				case "function_call":
					if args, ok := itemRaw["arguments"].(string); ok {
						if acc, exists := fcAccumByIndex[oi]; exists && args != acc {
							failures = append(failures, "event[response.output_item.done] function_call arguments mismatch: done "+jsonVal(args)+" != accumulated "+jsonVal(acc))
						}
					}
				}
			}
		case "response.function_call_arguments.delta":
			oiRaw, _ := ev.data["output_index"]
			oiFloat, ok := oiRaw.(float64)
			if !ok {
				failures = append(failures, "event[response.function_call_arguments.delta] output_index not number")
				continue
			}
			oi := int(oiFloat)
			if !addedSet[oi] {
				failures = append(failures, "event[response.function_call_arguments.delta] orphan delta: output_index "+itoa(oi)+" not yet added")
			}
			itemID, _ := ev.data["item_id"].(string)
			if want, exists := outputIndexToItemID[oi]; exists && want != itemID {
				failures = append(failures, "event[response.function_call_arguments.delta] item_id/output_index mismatch: index "+itoa(oi)+" expects "+want+" got "+itemID)
			}
			if typ, exists := outputIndexToType[oi]; exists && typ != "function_call" {
				failures = append(failures, "event[response.function_call_arguments.delta] output_index "+itoa(oi)+" type must be function_call, got "+typ)
			}
			if _, ok := ev.data["delta"].(string); !ok {
				failures = append(failures, "event[response.function_call_arguments.delta] missing delta string")
			} else {
				if d, ok := ev.data["delta"].(string); ok {
					fcAccumByIndex[oi] += d
				}
			}
		case "response.function_call_arguments.done":
			oiRaw, _ := ev.data["output_index"]
			oiFloat, ok := oiRaw.(float64)
			if !ok {
				failures = append(failures, "event[response.function_call_arguments.done] output_index not number")
				continue
			}
			oi := int(oiFloat)
			if !addedSet[oi] {
				failures = append(failures, "event[response.function_call_arguments.done] orphan: output_index "+itoa(oi)+" not yet added")
			}
			itemID, _ := ev.data["item_id"].(string)
			if want, exists := outputIndexToItemID[oi]; exists && want != itemID {
				failures = append(failures, "event[response.function_call_arguments.done] item_id/output_index mismatch: index "+itoa(oi)+" expects "+want+" got "+itemID)
			}
			if typ, exists := outputIndexToType[oi]; exists && typ != "function_call" {
				failures = append(failures, "event[response.function_call_arguments.done] output_index "+itoa(oi)+" type must be function_call, got "+typ)
			}
			if args, ok := ev.data["arguments"].(string); !ok {
				failures = append(failures, "event[response.function_call_arguments.done] missing arguments string")
			} else {
				if acc, exists := fcAccumByIndex[oi]; exists && args != acc {
					failures = append(failures, "event[response.function_call_arguments.done] arguments mismatch: done "+jsonVal(args)+" != accumulated "+jsonVal(acc))
				}
				// ensure accumulated equals this done value for later completed check
				fcAccumByIndex[oi] = args
			}
		}
	}

	// verify output_index continuity 0..n-1 and done item order
	if len(addedOrder) > 0 {
		sorted := append([]int(nil), addedOrder...)
		sort.Ints(sorted)
		for i, v := range sorted {
			if v != i {
				failures = append(failures, "output_index not contiguous 0..n-1: expected "+itoa(i)+" got "+itoa(v)+" (sorted added indices "+jsonVal(sorted)+")")
				break
			}
		}
		// also verify addedOrder arrival order is already 0,1,2... (no gap interleaving would still be contiguous but out-of-order counts as gap? We already sorted; to enforce appearance order is contiguous we check that at time of added, nextOutputIndex increments by 1 – already covered via sorted check, but we also ensure addedOrder is sorted ascending)
		for i, v := range addedOrder {
			if v != i {
				failures = append(failures, "added output_index appearance order must be 0..n-1 in event order: at position "+itoa(i)+" got "+itoa(v)+" full order "+jsonVal(addedOrder))
				break
			}
		}
		// done lifecycle order must follow output_index creation order (0..n-1) and be contiguous
		var doneOrder []int
		for _, ev := range events {
			if ev.event == "response.output_item.done" {
				if oiRaw, ok := ev.data["output_index"]; ok {
					if f, ok := oiRaw.(float64); ok {
						doneOrder = append(doneOrder, int(f))
					}
				}
			}
		}
		if len(doneOrder) != len(addedOrder) {
			failures = append(failures, "done output_item count "+itoa(len(doneOrder))+" != added count "+itoa(len(addedOrder))+" (done order "+jsonVal(doneOrder)+" added "+jsonVal(addedOrder)+")")
		} else {
			for i, v := range doneOrder {
				if v != i {
					failures = append(failures, "done output_item order must be 0..n-1 by output_index in event order: at position "+itoa(i)+" got "+itoa(v)+" full done order "+jsonVal(doneOrder))
					break
				}
			}
			// each done must match added mapping
			for i, v := range doneOrder {
				if v != addedOrder[i] {
					failures = append(failures, "done order mismatch added order: done["+itoa(i)+"]="+itoa(v)+" vs added["+itoa(i)+"]="+itoa(addedOrder[i]))
					break
				}
			}
		}
	} else {
		// no added but should have no done
		for _, ev := range events {
			if ev.event == "response.output_item.done" {
				failures = append(failures, "unexpected response.output_item.done without any added")
				break
			}
		}
	}

	// check completed output sorted by output_index and matches added types, and content matches accumulated
	for _, ev := range events {
		if ev.event == "response.completed" {
			resp, _ := ev.data["response"].(map[string]any)
			if resp == nil {
				continue
			}
			output, _ := resp["output"].([]any)
			if len(output) != len(outputIndexToItemID) {
				if len(outputIndexToItemID) == 0 && len(output) == 0 {
					// ok
				} else if len(outputIndexToItemID) > 0 && len(output) != len(outputIndexToItemID) {
					failures = append(failures, "completed output length "+itoa(len(output))+" != added count "+itoa(len(outputIndexToItemID)))
				}
			}
			prevIdx := -1
			for i, itemRaw := range output {
				item, _ := itemRaw.(map[string]any)
				if item == nil {
					failures = append(failures, "completed output["+itoa(i)+"] not object")
					continue
				}
				id, _ := item["id"].(string)
				itype, _ := item["type"].(string)
				foundIdx := -1
				for oi, oid := range outputIndexToItemID {
					if oid == id {
						foundIdx = oi
						break
					}
				}
				if foundIdx == -1 && len(outputIndexToItemID) > 0 {
					failures = append(failures, "completed output["+itoa(i)+"] id "+id+" not found in added mapping")
				}
				if foundIdx != -1 {
					if foundIdx <= prevIdx {
						failures = append(failures, "completed output not sorted by output_index: index "+itoa(foundIdx)+" <= prev "+itoa(prevIdx))
					}
					prevIdx = foundIdx
					// type consistency
					if wantTyp, exists := outputIndexToType[foundIdx]; exists && itype != wantTyp {
						failures = append(failures, "completed output["+itoa(i)+"] type mismatch: added "+wantTyp+" vs completed "+itype)
					}
					// content verification vs accumulated
					switch itype {
					case "message":
						if content, ok := item["content"].([]any); ok && len(content) > 0 {
							if block, ok := content[0].(map[string]any); ok {
								if txt, ok := block["text"].(string); ok {
									if acc, exists := msgAccum[foundIdx]; exists && txt != acc {
										failures = append(failures, "completed output["+itoa(i)+"] message text mismatch: completed "+jsonVal(txt)+" != accumulated "+jsonVal(acc))
									}
								}
							}
						}
					case "reasoning":
						if summary, ok := item["summary"].([]any); ok && len(summary) > 0 {
							if block, ok := summary[0].(map[string]any); ok {
								if txt, ok := block["text"].(string); ok {
									if acc, exists := rsAccum[foundIdx]; exists && txt != acc {
										failures = append(failures, "completed output["+itoa(i)+"] reasoning summary mismatch: completed "+jsonVal(txt)+" != accumulated "+jsonVal(acc))
									}
								}
							}
						}
					case "function_call":
						if args, ok := item["arguments"].(string); ok {
							if acc, exists := fcAccumByIndex[foundIdx]; exists && args != acc {
								failures = append(failures, "completed output["+itoa(i)+"] function_call arguments mismatch: completed "+jsonVal(args)+" != accumulated "+jsonVal(acc))
							}
						}
					}
				}
			}
		}
	}

	return failures
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	buf := make([]byte, 0, 12)
	for i > 0 {
		buf = append([]byte{byte('0' + i%10)}, buf...)
		i /= 10
	}
	if neg {
		buf = append([]byte{'-'}, buf...)
	}
	return string(buf)
}

func typeName(v any) string {
	if v == nil {
		return "nil"
	}
	switch v.(type) {
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "bool"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return "unknown"
	}
}

func jsonVal(v any) string {
	b, _ := json.Marshal(v)
	if len(b) == 0 {
		return "<nil>"
	}
	return string(b)
}

// TestResponsesStrictContract_TextLifecycle validates the strict Responses SSE contract
// via direct chatStreamToResponses call (no network, no credentials).
func TestResponsesStrictContract_TextLifecycle(t *testing.T) {
	up := testUpstream("data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\ndata: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\n")
	rec := httptest.NewRecorder()
	chatStreamToResponses(rec, up, nil, map[string]any{"model": "m"})
	raw := rec.Body.String()
	failures := strictValidateResponsesContract(t, raw)
	if len(failures) > 0 {
		t.Fatalf("strict Responses contract FAILED (%d violations) — first: %s\nall:\n- %s\n\nraw SSE:\n%s", len(failures), failures[0], strings.Join(failures, "\n- "), raw)
	}
}

// TestResponsesStrictContract_ViaMiddlewareTCP validates the same contract when
// Responses SSE is produced through requestLogMiddleware + real TCP (httptest.Server),
// proving realtime flush path does not alter contract.
func TestResponsesStrictContract_ViaMiddlewareTCP(t *testing.T) {
	pr, pw := io.Pipe()
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		upstream := &http.Response{Body: pr}
		chatStreamToResponses(w, upstream, nil, map[string]any{"model": "m"})
	})
	srv := httptest.NewServer(requestLogMiddleware(inner))
	defer srv.Close()
	defer func() {
		_ = pr.Close()
		_ = pw.Close()
	}()

	go func() {
		defer pw.Close()
		_, _ = io.WriteString(pw, "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello\"}}]}\n\n")
		_, _ = io.WriteString(pw, "data: {\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\n")
	}()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	raw := string(body)
	failures := strictValidateResponsesContract(t, raw)
	if len(failures) > 0 {
		t.Fatalf("strict Responses contract via middleware TCP FAILED (%d violations) — first: %s\nall:\n- %s\n\nraw SSE:\n%s", len(failures), failures[0], strings.Join(failures, "\n- "), raw)
	}
}

// negative fixtures
func TestResponsesStrictContract_Negative_OrphanDelta(t *testing.T) {
	// craft SSE where delta appears before added
	raw := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"created_at\":1,\"error\":null,\"incomplete_details\":null,\"instructions\":null,\"model\":\"m\",\"tools\":[],\"output\":[],\"parallel_tool_calls\":true,\"metadata\":null,\"tool_choice\":\"auto\",\"temperature\":null,\"top_p\":null,\"status\":\"in_progress\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":1,\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"hi\",\"logprobs\":[]}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"created_at\":1,\"error\":null,\"incomplete_details\":null,\"instructions\":null,\"model\":\"m\",\"tools\":[],\"output\":[],\"parallel_tool_calls\":true,\"metadata\":null,\"tool_choice\":\"auto\",\"temperature\":null,\"top_p\":null,\"status\":\"completed\",\"completed_at\":2,\"usage\":null}}\n\n"
	failures := strictValidateResponsesContract(t, raw)
	if len(failures) == 0 {
		t.Fatalf("negative test expected failures for orphan delta, got none")
	}
	found := false
	for _, f := range failures {
		if strings.Contains(f, "orphan") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("orphan delta not detected, failures: %v", failures)
	}
}

func TestResponsesStrictContract_Negative_IndexGap(t *testing.T) {
	raw := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"created_at\":1,\"error\":null,\"incomplete_details\":null,\"instructions\":null,\"model\":\"m\",\"tools\":[],\"output\":[],\"parallel_tool_calls\":true,\"metadata\":null,\"tool_choice\":\"auto\",\"temperature\":null,\"top_p\":null,\"status\":\"in_progress\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":2,\"output_index\":2,\"item\":{\"id\":\"msg_2\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":3,\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"created_at\":1,\"error\":null,\"incomplete_details\":null,\"instructions\":null,\"model\":\"m\",\"tools\":[],\"output\":[],\"parallel_tool_calls\":true,\"metadata\":null,\"tool_choice\":\"auto\",\"temperature\":null,\"top_p\":null,\"status\":\"completed\",\"completed_at\":2,\"usage\":null}}\n\n"
	failures := strictValidateResponsesContract(t, raw)
	if len(failures) == 0 {
		t.Fatalf("negative test expected failures for index gap, got none")
	}
	found := false
	for _, f := range failures {
		if strings.Contains(f, "contiguous") || strings.Contains(f, "gap") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("index gap not detected, failures: %v", failures)
	}
}

func TestResponsesStrictContract_Negative_CompletedMismatch(t *testing.T) {
	// delta accumulates "hello" but completed output has "wrong"
	raw := "event: response.created\ndata: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"created_at\":1,\"error\":null,\"incomplete_details\":null,\"instructions\":null,\"model\":\"m\",\"tools\":[],\"output\":[],\"parallel_tool_calls\":true,\"metadata\":null,\"tool_choice\":\"auto\",\"temperature\":null,\"top_p\":null,\"status\":\"in_progress\"}}\n\n" +
		"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n" +
		"event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"sequence_number\":2,\"output_index\":0,\"item_id\":\"msg_1\",\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\",\"annotations\":[]}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":3,\"output_index\":0,\"item_id\":\"msg_1\",\"content_index\":0,\"delta\":\"hello\",\"logprobs\":[]}\n\n" +
		"event: response.output_text.done\ndata: {\"type\":\"response.output_text.done\",\"sequence_number\":4,\"output_index\":0,\"item_id\":\"msg_1\",\"content_index\":0,\"text\":\"hello\",\"logprobs\":[]}\n\n" +
		"event: response.content_part.done\ndata: {\"type\":\"response.content_part.done\",\"sequence_number\":5,\"output_index\":0,\"item_id\":\"msg_1\",\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"hello\",\"annotations\":[]}}\n\n" +
		"event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"sequence_number\":6,\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\",\"annotations\":[]}] }}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"sequence_number\":7,\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"created_at\":1,\"error\":null,\"incomplete_details\":null,\"instructions\":null,\"model\":\"m\",\"tools\":[],\"output\":[{\"type\":\"message\",\"id\":\"msg_1\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"wrong\",\"annotations\":[]}],\"output_text\":\"wrong\"}],\"parallel_tool_calls\":true,\"metadata\":null,\"tool_choice\":\"auto\",\"temperature\":null,\"top_p\":null,\"status\":\"completed\",\"completed_at\":2,\"usage\":null}}\n\n"
	failures := strictValidateResponsesContract(t, raw)
	if len(failures) == 0 {
		t.Fatalf("negative test expected failures for completed mismatch, got none")
	}
	found := false
	for _, f := range failures {
		if strings.Contains(f, "mismatch") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("completed mismatch not detected, failures: %v", failures)
	}
}
