package app

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

var responsesIDCounter atomic.Uint64

func responsesNewID(prefix string) string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err == nil {
		return prefix + hex.EncodeToString(b)
	}
	c := responsesIDCounter.Add(1)
	return fmt.Sprintf("%s%x_%d", prefix, time.Now().UnixNano(), c)
}

func responsesDeltaFromChoice(choice map[string]any) map[string]any {
	if choice == nil {
		return nil
	}
	if d, ok := choice["delta"].(map[string]any); ok && d != nil {
		hasPayload := false
		for _, k := range []string{"content", "tool_calls", "reasoning_content", "reasoning", "function_call", "role"} {
			if v, ok := d[k]; ok && v != nil {
				if s, ok := v.(string); ok && s == "" && k == "content" {
					continue
				}
				if arr, ok := v.([]any); ok && len(arr) == 0 {
					continue
				}
				hasPayload = true
				break
			}
		}
		if hasPayload {
			return d
		}
	}
	if msg, ok := choice["message"].(map[string]any); ok && msg != nil {
		delta := make(map[string]any, 6)
		for _, k := range []string{"content", "tool_calls", "reasoning_content", "reasoning", "function_call", "role"} {
			if v, ok := msg[k]; ok {
				delta[k] = v
			}
		}
		if len(delta) > 0 {
			return delta
		}
	}
	delta := make(map[string]any, 6)
	for _, k := range []string{"content", "tool_calls", "reasoning_content", "reasoning", "function_call", "role"} {
		if v, ok := choice[k]; ok {
			delta[k] = v
		}
	}
	if len(delta) > 0 {
		return delta
	}
	if d, ok := choice["delta"].(map[string]any); ok && d != nil {
		return d
	}
	return choice
}

// responsesToChat 将 Responses 请求体转换为 chat.completions 请求体
func responsesToChat(body map[string]any) map[string]any {
	out := map[string]any{}
	if m, ok := body["model"].(string); ok {
		out["model"] = m
	}
	if s, ok := body["stream"].(bool); ok {
		out["stream"] = s
	}
	if mt, ok := body["max_output_tokens"].(float64); ok {
		out["max_tokens"] = int(mt)
	}
	for _, k := range []string{"temperature", "top_p", "stop", "seed", "user", "metadata", "logit_bias"} {
		if v, ok := body[k]; ok {
			out[k] = v
		}
	}
	if instr, ok := body["instructions"].(string); ok && instr != "" {
		out["messages"] = append([]any{map[string]any{"role": "system", "content": instr}}, responsesInputToMessages(body["input"])...)
	} else {
		out["messages"] = responsesInputToMessages(body["input"])
	}
	if tools, ok := body["tools"].([]any); ok {
		out["tools"] = responsesToolsToChat(tools)
	}
	if tc, ok := body["tool_choice"]; ok {
		out["tool_choice"] = tc
	}
	return out
}

func responsesInputToMessages(input any) []any {
	var msgs []any
	switch v := input.(type) {
	case string:
		msgs = append(msgs, map[string]any{"role": "user", "content": v})
	case []any:
		for _, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			switch m["type"] {
			case "message":
				role, _ := m["role"].(string)
				if role == "" {
					role = "user"
				}
				msgs = append(msgs, map[string]any{"role": role, "content": stringifyResponsesContent(m["content"])})
			case "function_call":
				callID, _ := m["call_id"].(string)
				if callID == "" {
					callID, _ = m["id"].(string)
				}
				name, _ := m["name"].(string)
				args := ""
				switch a := m["arguments"].(type) {
				case string:
					args = a
				case map[string]any:
					if b, err := json.Marshal(a); err == nil {
						args = string(b)
					}
				}
				msgs = append(msgs, map[string]any{
					"role":       "assistant",
					"content":    "",
					"tool_calls": []any{map[string]any{"id": callID, "type": "function", "function": map[string]any{"name": name, "arguments": args}}},
				})
			case "function_call_output":
				callID, _ := m["call_id"].(string)
				if callID == "" {
					callID, _ = m["id"].(string)
				}
				output := ""
				switch o := m["output"].(type) {
				case string:
					output = o
				case map[string]any:
					if b, err := json.Marshal(o); err == nil {
						output = string(b)
					}
				}
				msgs = append(msgs, map[string]any{"role": "tool", "content": output, "tool_call_id": callID})
			case "reasoning":
				// 忽略 Reasoning 输入项(无法映射到 chat 输入)
			}
		}
	}
	return msgs
}

func stringifyResponsesContent(content any) string {
	switch v := content.(type) {
	case string:
		return v
	case []any:
		parts := []string{}
		for _, block := range v {
			if b, ok := block.(map[string]any); ok {
				if t, ok := b["text"].(string); ok {
					parts = append(parts, t)
				}
			}
		}
		return strings.Join(parts, "\n")
	}
	return ""
}

func responsesToolsToChat(tools []any) []any {
	out := make([]any, 0, len(tools))
	for _, t := range tools {
		tm, ok := t.(map[string]any)
		if !ok {
			continue
		}
		if tm["type"] == "function" {
			fn := map[string]any{}
			if n, ok := tm["name"].(string); ok {
				fn["name"] = n
			}
			if d, ok := tm["description"].(string); ok {
				fn["description"] = d
			}
			if p, ok := tm["parameters"].(map[string]any); ok {
				fn["parameters"] = p
			}
			out = append(out, map[string]any{"type": "function", "function": fn})
		}
	}
	return out
}

func intFromAny(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case int64:
		return int(x)
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return int(i)
		}
	}
	return 0
}

// parseUsageValues 统一解析上游 usage 的多种形态，返回 input/output/total/cached/cacheWrite/reasoning。
func parseUsageValues(u map[string]any) (int, int, int, int, int, int) {
	if u == nil {
		return 0, 0, 0, 0, 0, 0
	}
	var in, out, total, cached, cacheWrite, reasoning int
	if v, ok := u["prompt_tokens"]; ok {
		in = intFromAny(v)
	}
	if v, ok := u["input_tokens"]; ok {
		in = intFromAny(v)
	}
	if v, ok := u["completion_tokens"]; ok {
		out = intFromAny(v)
	}
	if v, ok := u["output_tokens"]; ok {
		out = intFromAny(v)
	}
	if v, ok := u["total_tokens"]; ok {
		total = intFromAny(v)
	}
	if pd, ok := u["prompt_tokens_details"].(map[string]any); ok {
		if ct, ok := pd["cached_tokens"]; ok {
			cached = intFromAny(ct)
		}
		if cw, ok := pd["cache_creation_input_tokens"]; ok {
			cacheWrite = intFromAny(cw)
		} else if cw, ok := pd["cache_write_tokens"]; ok {
			cacheWrite = intFromAny(cw)
		} else if cw, ok := pd["cached_creation_input_tokens"]; ok {
			cacheWrite = intFromAny(cw)
		}
	}
	if itd, ok := u["input_tokens_details"].(map[string]any); ok {
		if ct, ok := itd["cached_tokens"]; ok {
			cached = intFromAny(ct)
		}
		if cw, ok := itd["cache_creation_input_tokens"]; ok {
			cacheWrite = intFromAny(cw)
		} else if cw, ok := itd["cache_write_tokens"]; ok {
			cacheWrite = intFromAny(cw)
		} else if cw, ok := itd["cached_creation_input_tokens"]; ok {
			cacheWrite = intFromAny(cw)
		}
	}
	if ct, ok := u["cached_tokens"]; ok {
		cached = intFromAny(ct)
	}
	if cw, ok := u["cache_creation_input_tokens"]; ok {
		cacheWrite = intFromAny(cw)
	} else if cw, ok := u["cache_write_tokens"]; ok {
		cacheWrite = intFromAny(cw)
	}
	if od, ok := u["completion_tokens_details"].(map[string]any); ok {
		if rt, ok := od["reasoning_tokens"]; ok {
			reasoning = intFromAny(rt)
		}
	}
	if otd, ok := u["output_tokens_details"].(map[string]any); ok {
		if rt, ok := otd["reasoning_tokens"]; ok {
			reasoning = intFromAny(rt)
		}
	}
	if rt, ok := u["reasoning_tokens"]; ok {
		reasoning = intFromAny(rt)
	}
	if total == 0 && (in != 0 || out != 0) {
		total = in + out
	}
	return in, out, total, cached, cacheWrite, reasoning
}
