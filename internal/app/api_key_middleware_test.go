package app

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestApiKeyMiddleware_AllowDeny(t *testing.T) {
	poolMu.Lock()
	oldPool := pool
	var oldKeys []string
	if oldPool != nil {
		oldKeys = append([]string(nil), oldPool.Keys...)
	}
	if pool == nil {
		pool = &AccountPool{Accounts: []*Account{}, Keys: []string{}}
	}
	poolMu.Unlock()
	defer func() {
		poolMu.Lock()
		if oldPool != nil {
			pool.Keys = oldKeys
		} else {
			pool = nil
		}
		poolMu.Unlock()
	}()

	next := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
	handler := apiKeyMiddleware(next)

	// case 1: no keys configured -> allow without header
	poolMu.Lock()
	pool.Keys = []string{}
	poolMu.Unlock()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/v1/models", nil)
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("no-keys should allow, got %d body %q", rec.Code, rec.Body.String())
	}

	// case 2: keys configured, valid x-api-key
	poolMu.Lock()
	pool.Keys = []string{"good-key"}
	poolMu.Unlock()
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/v1/models", nil)
	req.Header.Set("x-api-key", "good-key")
	handler(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "ok") {
		t.Fatalf("valid x-api-key should allow, got %d body %q", rec.Code, rec.Body.String())
	}

	// case 3: valid via Authorization Bearer
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer good-key")
	handler(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("valid Authorization should allow, got %d body %q", rec.Code, rec.Body.String())
	}

	// case 4: invalid key -> 401 with expected body/type
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/v1/models", nil)
	req.Header.Set("x-api-key", "bad-key")
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid key should be 401, got %d body %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "invalid API key") || !strings.Contains(rec.Body.String(), "auth_error") {
		t.Fatalf("invalid key body wrong: %q", rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("invalid key content-type should be json, got %q", ct)
	}

	// case 5: missing key -> 401
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/v1/models", nil)
	handler(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing key should be 401, got %d body %q", rec.Code, rec.Body.String())
	}
}
