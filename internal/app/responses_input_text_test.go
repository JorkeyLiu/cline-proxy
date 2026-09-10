package app

import "testing"

func TestStringifyResponsesContent_InputText(t *testing.T) {
	content := []any{
		map[string]any{"type": "input_text", "text": "hello"},
		map[string]any{"type": "input_text", "text": "world"},
	}
	got := stringifyResponsesContent(content)
	if got != "hello\nworld" {
		t.Fatalf("input_text block stringify failed: got %q want %q", got, "hello\nworld")
	}
}

func TestResponsesInputToMessages_InputText(t *testing.T) {
	input := []any{
		map[string]any{
			"type": "message",
			"role": "user",
			"content": []any{
				map[string]any{"type": "input_text", "text": "first"},
				map[string]any{"type": "input_text", "text": "second"},
			},
		},
	}
	msgs := responsesInputToMessages(input)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d: %v", len(msgs), msgs)
	}
	m, _ := msgs[0].(map[string]any)
	content, _ := m["content"].(string)
	if content != "first\nsecond" {
		t.Fatalf("input_text content merged failed: got %q want %q", content, "first\nsecond")
	}
}

func TestResponsesToChat_InputTextPreserved(t *testing.T) {
	body := map[string]any{
		"model": "m",
		"input": []any{
			map[string]any{
				"type": "message",
				"role": "user",
				"content": []any{
					map[string]any{"type": "input_text", "text": "hi"},
				},
			},
		},
	}
	chat := responsesToChat(body)
	msgs, _ := chat["messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 chat message, got %v", chat["messages"])
	}
	m, _ := msgs[0].(map[string]any)
	if c, _ := m["content"].(string); c != "hi" {
		t.Fatalf("chat content from input_text failed: %q", c)
	}
}
