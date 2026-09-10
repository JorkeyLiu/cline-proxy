package app

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// upstreamDelta is the typed delta produced by the unified decoder.
type upstreamDelta struct {
	Model        string
	Content      string
	Reasoning    string
	ToolFrags    []toolFrag
	Usage        map[string]any
	FinishReason string
}

type toolFrag struct {
	Index int
	ID    string
	Name  string
	Args  string
}

// responsesDecoder unified decoder for non-stream JSON and SSE.
type responsesDecoder struct {
	reader       *bufio.Reader
	mode         string // "sse" or "json"
	jsonDelta    *upstreamDelta
	jsonConsumed bool
	jsonErr      error
}

func newResponsesDecoder(resp *http.Response) (*responsesDecoder, error) {
	r := bufio.NewReader(resp.Body)
	ct := resp.Header.Get("Content-Type")
	mode := ""
	if strings.Contains(ct, "application/json") {
		mode = "json"
	} else if strings.Contains(ct, "text/event-stream") {
		mode = "sse"
	} else {
		peek, _ := r.Peek(2048)
		s := strings.TrimSpace(string(peek))
		// strip BOM
		s = strings.TrimPrefix(s, "\xef\xbb\xbf")
		sTrim := strings.TrimLeft(s, " \t\r\n")
		if strings.HasPrefix(sTrim, "{") || strings.HasPrefix(sTrim, "[") {
			mode = "json"
		} else if strings.HasPrefix(sTrim, "data:") || strings.HasPrefix(sTrim, "event:") || strings.HasPrefix(sTrim, ":") {
			mode = "sse"
		} else if strings.Contains(sTrim, "\"choices\"") && strings.HasPrefix(sTrim, "{") {
			mode = "json"
		} else {
			mode = "sse"
		}
	}
	d := &responsesDecoder{reader: r, mode: mode}
	if mode == "json" {
		data, err := io.ReadAll(r)
		if err != nil && err != io.EOF {
			d.jsonErr = err
			return d, nil
		}
		s := strings.TrimSpace(string(data))
		if s == "" {
			d.jsonConsumed = true
			return d, nil
		}
		delta, err := parseUpstreamPayload(s)
		if err != nil {
			d.jsonErr = err
			return d, nil
		}
		d.jsonDelta = delta
	}
	return d, nil
}

// Next returns next typed delta. io.EOF signals end. Any other error is hard failure.
func (d *responsesDecoder) Next() (*upstreamDelta, error) {
	if d.mode == "json" {
		if d.jsonErr != nil {
			return nil, d.jsonErr
		}
		if d.jsonConsumed || d.jsonDelta == nil {
			return nil, io.EOF
		}
		// empty delta with no fields should be treated as EOF (no content)
		if isEmptyDelta(d.jsonDelta) {
			d.jsonConsumed = true
			return nil, io.EOF
		}
		d.jsonConsumed = true
		return d.jsonDelta, nil
	}
	// SSE mode
	for {
		payload, isEOF, err := readNextSSEPayload(d.reader)
		if err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, err
		}
		if payload == "" {
			if isEOF {
				return nil, io.EOF
			}
			continue
		}
		if payload == "[DONE]" {
			if isEOF {
				return nil, io.EOF
			}
			continue
		}
		delta, err := parseUpstreamPayload(payload)
		if err != nil {
			return nil, err
		}
		if delta == nil {
			if isEOF {
				return nil, io.EOF
			}
			continue
		}
		if isEmptyDelta(delta) {
			if isEOF {
				return nil, io.EOF
			}
			continue
		}
		return delta, nil
	}
}

func isEmptyDelta(d *upstreamDelta) bool {
	if d == nil {
		return true
	}
	return d.Model == "" && d.Usage == nil && d.Content == "" && d.Reasoning == "" && len(d.ToolFrags) == 0 && d.FinishReason == ""
}

// parseUpstreamPayload parses a JSON string (SSE data payload or full JSON body) into typed delta.
func parseUpstreamPayload(raw string) (*upstreamDelta, error) {
	s := strings.TrimSpace(raw)
	if s == "" || s == "[DONE]" {
		return nil, nil
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(s), &obj); err != nil {
		return nil, err
	}
	// unwrap {data: {...}}
	if data, ok := obj["data"]; ok {
		if d, ok := data.(map[string]any); ok {
			if _, hasChoices := d["choices"]; hasChoices {
				obj = d
			} else if _, hasID := d["id"]; hasID {
				obj = d
			} else if len(d) > 0 {
				obj = d
			}
		}
	}
	delta := &upstreamDelta{}
	if m, ok := obj["model"].(string); ok {
		delta.Model = m
	}
	if u, ok := obj["usage"].(map[string]any); ok && len(u) > 0 {
		delta.Usage = u
	}
	choices, _ := obj["choices"].([]any)
	if len(choices) > 0 {
		ch, _ := choices[0].(map[string]any)
		if ch != nil {
			if fr, ok := ch["finish_reason"].(string); ok {
				delta.FinishReason = fr
			}
			d := responsesDeltaFromChoice(ch)
			if d != nil {
				if c, ok := d["content"].(string); ok {
					delta.Content = c
				}
				if r, ok := d["reasoning_content"].(string); ok && r != "" {
					delta.Reasoning = r
				} else if r, ok := d["reasoning"].(string); ok && r != "" {
					delta.Reasoning = r
				}
				if tcRaw, ok := d["tool_calls"].([]any); ok {
					for _, tc := range tcRaw {
						m, _ := tc.(map[string]any)
						if m == nil {
							continue
						}
						frag := toolFrag{}
						if v, ok := m["index"]; ok {
							switch x := v.(type) {
							case float64:
								frag.Index = int(x)
							case int:
								frag.Index = x
							case json.Number:
								if i, e := x.Int64(); e == nil {
									frag.Index = int(i)
								}
							}
						}
						if id, ok := m["id"].(string); ok {
							frag.ID = id
						}
						if fn, ok := m["function"].(map[string]any); ok {
							if n, ok := fn["name"].(string); ok {
								frag.Name = n
							}
							if a, ok := fn["arguments"].(string); ok {
								frag.Args = a
							} else if am, ok := fn["arguments"].(map[string]any); ok {
								if b, err := json.Marshal(am); err == nil {
									frag.Args = string(b)
								}
							}
						}
						delta.ToolFrags = append(delta.ToolFrags, frag)
					}
				}
			}
		}
	}
	return delta, nil
}

// readNextSSEPayload reads a complete SSE event and returns joined data payload.
// isEOF indicates the reader hit EOF after this event.
func readNextSSEPayload(r *bufio.Reader) (string, bool, error) {
	var dataLines []string
	seenData := false
	seenEvent := false
	for {
		line, err := r.ReadString('\n')
		isEOF := err == io.EOF
		isHardErr := err != nil && err != io.EOF
		if isHardErr {
			return "", false, err
		}
		if line != "" {
			noNL := strings.TrimRight(line, "\r\n")
			if strings.TrimSpace(noNL) == "" {
				// blank line ends event
				if seenData {
					payload := strings.Join(dataLines, "\n")
					return payload, isEOF, nil
				}
				// heartbeat/comment empty event
				if isEOF {
					return "", true, io.EOF
				}
				if seenEvent {
					// no data for this event, continue to next event
					return "", false, nil
				}
				continue
			}
			seenEvent = true
			trimmed := strings.TrimSpace(noNL)
			if strings.HasPrefix(trimmed, ":") {
				continue
			}
			ls := strings.TrimLeft(noNL, " \t")
			if strings.HasPrefix(ls, "data:") {
				p := ls[len("data:"):]
				if strings.HasPrefix(p, " ") {
					p = p[1:]
				}
				dataLines = append(dataLines, p)
				seenData = true
			} else if strings.HasPrefix(ls, "event:") || strings.HasPrefix(ls, "id:") || strings.HasPrefix(ls, "retry:") {
				continue
			} else {
				continue
			}
		}
		if isEOF {
			if len(dataLines) > 0 {
				payload := strings.Join(dataLines, "\n")
				return payload, true, nil
			}
			if seenData {
				return strings.Join(dataLines, "\n"), true, nil
			}
			return "", true, io.EOF
		}
		// if we are here we have just processed a data line but not yet terminated by blank.
		// Continue reading until blank line or EOF. But we need to avoid busy loop when line is empty?
		// Loop will read next line.
		if seenData {
			// keep accumulating; next iteration will read next line
			continue
		}
	}
}
