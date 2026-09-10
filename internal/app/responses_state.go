package app

import (
	"sort"
	"strings"
	"time"
)

// ResponseState is the single source of truth for Responses protocol.
type ResponseState struct {
	respID      string
	msgID       string
	createdAt   int64
	model       string
	modelFrozen bool

	instructions      any
	tools             any
	parallelToolCalls any
	metadata          any
	toolChoice        any
	temperature       any
	topP              any

	usageSeen        bool
	inTokens         int
	outTokens        int
	totalTokens      int
	cachedTokens     int
	cacheWriteTokens int
	reasoningTokens  int

	nextIdx int

	msgIdx   *int
	msgAdded bool
	msgText  strings.Builder

	rsID    string
	rsIdx   *int
	rsAdded bool
	rsText  strings.Builder

	toolByIndex map[int]*stateTool
	toolOrder   []*stateTool
}

type stateTool struct {
	idx         int
	callID      string
	name        string
	args        strings.Builder
	added       bool
	fcID        string
	outputIndex int
}

func newResponseState(params map[string]any) *ResponseState {
	s := &ResponseState{
		respID:      responsesNewID("resp_"),
		msgID:       responsesNewID("msg_"),
		createdAt:   time.Now().Unix(),
		toolByIndex: make(map[int]*stateTool),
		nextIdx:     0,
	}
	if params != nil {
		if v, ok := params["model"].(string); ok {
			s.model = v
		}
		if s.model != "" {
			s.modelFrozen = true
		}
		if v, ok := params["instructions"]; ok {
			s.instructions = v
		} else {
			s.instructions = nil
		}
		if v, ok := params["tools"]; ok {
			if arr, ok := v.([]any); ok && arr != nil {
				s.tools = arr
			} else {
				s.tools = []any{}
			}
		} else {
			s.tools = []any{}
		}
		if v, ok := params["parallel_tool_calls"]; ok {
			s.parallelToolCalls = v
		} else {
			s.parallelToolCalls = true
		}
		if v, ok := params["metadata"]; ok {
			s.metadata = v
		} else {
			s.metadata = nil
		}
		if v, ok := params["tool_choice"]; ok {
			s.toolChoice = v
		} else {
			s.toolChoice = "auto"
		}
		if v, ok := params["temperature"]; ok {
			s.temperature = v
		} else {
			s.temperature = nil
		}
		if v, ok := params["top_p"]; ok {
			s.topP = v
		} else {
			s.topP = nil
		}
	} else {
		s.instructions = nil
		s.tools = []any{}
		s.parallelToolCalls = true
		s.metadata = nil
		s.toolChoice = "auto"
		s.temperature = nil
		s.topP = nil
	}
	return s
}

// Apply processes a typed delta, updating state and emitting SSE events via emitter.
// onUsage is invoked when usage is seen.
func (s *ResponseState) Apply(delta *upstreamDelta, emitter *responsesEmitter, onUsage func(map[string]any)) {
	if delta == nil {
		return
	}
	if delta.Model != "" && !s.modelFrozen {
		s.model = delta.Model
	}
	if delta.Usage != nil {
		in, out, total, cached, cw, reasoning := parseUsageValues(delta.Usage)
		s.inTokens = in
		s.outTokens = out
		s.totalTokens = total
		s.cachedTokens = cached
		s.cacheWriteTokens = cw
		s.reasoningTokens = reasoning
		s.usageSeen = true
		if onUsage != nil {
			onUsage(delta.Usage)
		}
	}
	if delta.Reasoning != "" {
		if s.rsIdx == nil {
			s.rsID = responsesNewID("rs_")
			idx := s.nextIdx
			s.nextIdx++
			s.rsIdx = &idx
			s.rsAdded = true
			if emitter != nil {
				emitter.emit("response.output_item.added", map[string]any{
					"output_index": idx,
					"item":         map[string]any{"id": s.rsID, "type": "reasoning", "summary": []any{}, "status": "in_progress"},
				})
				emitter.emit("response.reasoning_summary_part.added", map[string]any{
					"item_id":       s.rsID,
					"output_index":  idx,
					"summary_index": 0,
					"part":          map[string]any{"type": "summary_text", "text": ""},
				})
			}
		}
		s.rsText.WriteString(delta.Reasoning)
		if emitter != nil {
			emitter.emit("response.reasoning_summary_text.delta", map[string]any{
				"item_id":       s.rsID,
				"output_index":  *s.rsIdx,
				"summary_index": 0,
				"delta":         delta.Reasoning,
			})
		}
	}
	if delta.Content != "" {
		if s.msgIdx == nil {
			idx := s.nextIdx
			s.nextIdx++
			s.msgIdx = &idx
			s.msgAdded = true
			if emitter != nil {
				emitter.emit("response.output_item.added", map[string]any{
					"output_index": idx,
					"item":         map[string]any{"id": s.msgID, "type": "message", "role": "assistant", "status": "in_progress", "content": []any{}},
				})
				emitter.emit("response.content_part.added", map[string]any{
					"item_id":       s.msgID,
					"output_index":  idx,
					"content_index": 0,
					"part":          map[string]any{"type": "output_text", "text": "", "annotations": []any{}},
				})
			}
		}
		s.msgText.WriteString(delta.Content)
		if emitter != nil {
			emitter.emit("response.output_text.delta", map[string]any{
				"item_id":       s.msgID,
				"output_index":  *s.msgIdx,
				"content_index": 0,
				"delta":         delta.Content,
				"logprobs":      []any{},
			})
		}
	}
	if len(delta.ToolFrags) > 0 {
		for _, frag := range delta.ToolFrags {
			st, ok := s.toolByIndex[frag.Index]
			if !ok {
				st = &stateTool{idx: frag.Index, outputIndex: -1}
				s.toolByIndex[frag.Index] = st
			}
			if st.added {
				if frag.Args != "" {
					st.args.WriteString(frag.Args)
					if emitter != nil {
						emitter.emit("response.function_call_arguments.delta", map[string]any{
							"item_id":      st.fcID,
							"output_index": st.outputIndex,
							"delta":        frag.Args,
						})
					}
				}
				continue
			}
			if frag.ID != "" {
				st.callID = frag.ID
			}
			if frag.Name != "" {
				st.name = frag.Name
			}
			if frag.Args != "" {
				st.args.WriteString(frag.Args)
			}
			if !st.added && st.name != "" {
				oIdx := s.nextIdx
				s.nextIdx++
				st.outputIndex = oIdx
				st.fcID = responsesNewID("fc_")
				if st.callID == "" {
					st.callID = responsesNewID("call_")
				}
				st.added = true
				s.toolOrder = append(s.toolOrder, st)
				if emitter != nil {
					emitter.emit("response.output_item.added", map[string]any{
						"output_index": oIdx,
						"item": map[string]any{
							"type":      "function_call",
							"id":        st.fcID,
							"call_id":   st.callID,
							"name":      st.name,
							"arguments": "",
							"status":    "in_progress",
						},
					})
					if st.args.Len() > 0 {
						merged := st.args.String()
						emitter.emit("response.function_call_arguments.delta", map[string]any{
							"item_id":      st.fcID,
							"output_index": oIdx,
							"delta":        merged,
						})
					}
				}
			}
		}
	}
}

// FinalizePendingTools allocates any tool fragments that never got a name, in index order.
func (s *ResponseState) FinalizePendingTools(emitter *responsesEmitter) {
	var pending []*stateTool
	for _, st := range s.toolByIndex {
		if !st.added {
			pending = append(pending, st)
		}
	}
	sort.Slice(pending, func(a, b int) bool { return pending[a].idx < pending[b].idx })
	for _, st := range pending {
		if st.callID == "" {
			st.callID = responsesNewID("call_")
		}
		if st.name == "" {
			st.name = "unknown"
		}
		st.outputIndex = s.nextIdx
		s.nextIdx++
		st.fcID = responsesNewID("fc_")
		st.added = true
		s.toolOrder = append(s.toolOrder, st)
		if emitter != nil {
			emitter.emit("response.output_item.added", map[string]any{
				"output_index": st.outputIndex,
				"item": map[string]any{
					"type":      "function_call",
					"id":        st.fcID,
					"call_id":   st.callID,
					"name":      st.name,
					"arguments": "",
					"status":    "in_progress",
				},
			})
			if st.args.Len() > 0 {
				emitter.emit("response.function_call_arguments.delta", map[string]any{
					"item_id":      st.fcID,
					"output_index": st.outputIndex,
					"delta":        st.args.String(),
				})
			}
		}
	}
}

// EmitDone emits the done lifecycle events sorted by output_index.
func (s *ResponseState) EmitDone(emitter *responsesEmitter) {
	type doneEntry struct {
		idx  int
		emit func()
	}
	var dones []doneEntry
	if s.rsAdded && s.rsIdx != nil {
		idx := *s.rsIdx
		full := s.rsText.String()
		rsIDCopy := s.rsID
		idxCopy := idx
		fullCopy := full
		dones = append(dones, doneEntry{idx: idxCopy, emit: func() {
			emitter.emit("response.reasoning_summary_text.done", map[string]any{
				"item_id":       rsIDCopy,
				"output_index":  idxCopy,
				"summary_index": 0,
				"text":          fullCopy,
			})
			emitter.emit("response.reasoning_summary_part.done", map[string]any{
				"item_id":       rsIDCopy,
				"output_index":  idxCopy,
				"summary_index": 0,
				"part":          map[string]any{"type": "summary_text", "text": fullCopy},
			})
			emitter.emit("response.output_item.done", map[string]any{
				"output_index": idxCopy,
				"item": map[string]any{
					"id":     rsIDCopy,
					"type":   "reasoning",
					"status": "completed",
					"summary": []any{
						map[string]any{"type": "summary_text", "text": fullCopy},
					},
				},
			})
		}})
	}
	if s.msgAdded && s.msgIdx != nil {
		idx := *s.msgIdx
		full := s.msgText.String()
		idxCopy := idx
		fullCopy := full
		msgIDCopy := s.msgID
		dones = append(dones, doneEntry{idx: idxCopy, emit: func() {
			emitter.emit("response.output_text.done", map[string]any{
				"item_id":       msgIDCopy,
				"output_index":  idxCopy,
				"content_index": 0,
				"text":          fullCopy,
				"logprobs":      []any{},
			})
			emitter.emit("response.content_part.done", map[string]any{
				"item_id":       msgIDCopy,
				"output_index":  idxCopy,
				"content_index": 0,
				"part": map[string]any{
					"type":        "output_text",
					"text":        fullCopy,
					"annotations": []any{},
				},
			})
			emitter.emit("response.output_item.done", map[string]any{
				"output_index": idxCopy,
				"item": map[string]any{
					"id":      msgIDCopy,
					"type":    "message",
					"role":    "assistant",
					"status":  "completed",
					"content": []any{map[string]any{"type": "output_text", "text": fullCopy, "annotations": []any{}}},
				},
			})
		}})
	}
	for _, st := range s.toolOrder {
		stCopy := st
		idxCopy := st.outputIndex
		dones = append(dones, doneEntry{idx: idxCopy, emit: func() {
			emitter.emit("response.function_call_arguments.done", map[string]any{
				"item_id":      stCopy.fcID,
				"output_index": idxCopy,
				"arguments":    stCopy.args.String(),
			})
			emitter.emit("response.output_item.done", map[string]any{
				"output_index": idxCopy,
				"item": map[string]any{
					"type":      "function_call",
					"id":        stCopy.fcID,
					"call_id":   stCopy.callID,
					"name":      stCopy.name,
					"arguments": stCopy.args.String(),
					"status":    "completed",
				},
			})
		}})
	}
	sort.Slice(dones, func(a, b int) bool { return dones[a].idx < dones[b].idx })
	for _, d := range dones {
		d.emit()
	}
}

// Snapshot builds the official Response object for completed/in_progress.
func (s *ResponseState) Snapshot(status string, includeUsage bool, completedAt *int64) map[string]any {
	mdl := s.model
	if mdl == "" {
		mdl = "unknown"
	}
	// build output items sorted by output_index
	type outItem struct {
		index int
		item  map[string]any
	}
	var items []outItem
	if s.rsAdded && s.rsIdx != nil {
		full := s.rsText.String()
		items = append(items, outItem{index: *s.rsIdx, item: map[string]any{
			"type":   "reasoning",
			"id":     s.rsID,
			"status": "completed",
			"summary": []any{map[string]any{
				"type": "summary_text",
				"text": full,
			}},
		}})
	}
	if s.msgAdded && s.msgIdx != nil {
		full := s.msgText.String()
		items = append(items, outItem{index: *s.msgIdx, item: map[string]any{
			"type":        "message",
			"id":          s.msgID,
			"status":      "completed",
			"role":        "assistant",
			"content":     []any{map[string]any{"type": "output_text", "text": full, "annotations": []any{}}},
			"output_text": full,
		}})
	}
	for _, st := range s.toolOrder {
		items = append(items, outItem{index: st.outputIndex, item: map[string]any{
			"type":      "function_call",
			"id":        st.fcID,
			"call_id":   st.callID,
			"name":      st.name,
			"arguments": st.args.String(),
			"status":    "completed",
		}})
	}
	sort.Slice(items, func(a, b int) bool { return items[a].index < items[b].index })
	outputs := []any{}
	for _, it := range items {
		outputs = append(outputs, it.item)
	}
	resp := map[string]any{
		"id":                  s.respID,
		"object":              "response",
		"created_at":          s.createdAt,
		"error":               nil,
		"incomplete_details":  nil,
		"instructions":        s.instructions,
		"model":               mdl,
		"tools":               s.tools,
		"output":              outputs,
		"parallel_tool_calls": s.parallelToolCalls,
		"metadata":            s.metadata,
		"tool_choice":         s.toolChoice,
		"temperature":         s.temperature,
		"top_p":               s.topP,
		"status":              status,
	}
	if includeUsage {
		if !s.usageSeen {
			resp["usage"] = nil
		} else {
			total := s.totalTokens
			if total == 0 && (s.inTokens != 0 || s.outTokens != 0) {
				total = s.inTokens + s.outTokens
			}
			resp["usage"] = map[string]any{
				"input_tokens":  s.inTokens,
				"output_tokens": s.outTokens,
				"total_tokens":  total,
				"input_tokens_details": map[string]any{
					"cached_tokens":      s.cachedTokens,
					"cache_write_tokens": s.cacheWriteTokens,
				},
				"output_tokens_details": map[string]any{
					"reasoning_tokens": s.reasoningTokens,
				},
			}
		}
	}
	if completedAt != nil {
		resp["completed_at"] = *completedAt
	}
	// backward compatible output_text (non-standard)
	compText := ""
	if s.msgAdded {
		compText = s.msgText.String()
	}
	resp["output_text"] = compText
	return resp
}
