package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openweft/weft-ha-postgresql/internal/postgres"
)

type fakeRP struct {
	role postgres.Role
	err  error
}

func (f fakeRP) Role(context.Context) (postgres.Role, error) { return f.role, f.err }

func do(t *testing.T, rp RoleProvider, path string) int {
	t.Helper()
	s := New(":0", rp, slog.New(slog.DiscardHandler))
	rec := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

func TestPrimaryEndpoint(t *testing.T) {
	if code := do(t, fakeRP{role: postgres.RolePrimary}, "/primary"); code != http.StatusOK {
		t.Errorf("/primary on primary: got %d, want 200", code)
	}
	if code := do(t, fakeRP{role: postgres.RoleReplica}, "/primary"); code != http.StatusServiceUnavailable {
		t.Errorf("/primary on replica: got %d, want 503", code)
	}
}

func TestReplicaEndpoint(t *testing.T) {
	if code := do(t, fakeRP{role: postgres.RoleReplica}, "/replica"); code != http.StatusOK {
		t.Errorf("/replica on replica: got %d, want 200", code)
	}
	if code := do(t, fakeRP{role: postgres.RolePrimary}, "/replica"); code != http.StatusServiceUnavailable {
		t.Errorf("/replica on primary: got %d, want 503", code)
	}
}

func TestRoleErrorIsUnavailable(t *testing.T) {
	if code := do(t, fakeRP{err: postgres.ErrNotConnected}, "/primary"); code != http.StatusServiceUnavailable {
		t.Errorf("/primary with role error: got %d, want 503", code)
	}
}

func TestHealthAlwaysOK(t *testing.T) {
	if code := do(t, fakeRP{err: postgres.ErrNotConnected}, "/health"); code != http.StatusOK {
		t.Errorf("/health: got %d, want 200", code)
	}
}

// TestHealthIETFVocabulary pins the IETF Health Check Response Format vocab
// ("pass" / "fail") on /health. Regression guard against the bug-1 drift
// captured in project_weft_3dc_live_cluster — postgres-ha and irods-ha both
// used "ok" / "unhealthy" at one point, which breaks generic IETF-aware
// probes (caddy active health check, kubelet readinessProbe, the openweft
// webui dashboard) that switch on `status`.
func TestHealthIETFVocabulary(t *testing.T) {
	cases := []struct {
		name       string
		rp         RoleProvider
		wantStatus string
		wantRole   string
	}{
		{"primary", fakeRP{role: postgres.RolePrimary}, "pass", "primary"},
		{"replica", fakeRP{role: postgres.RoleReplica}, "pass", "replica"},
		{"unknown-role-fails", fakeRP{err: postgres.ErrNotConnected}, "fail", "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := New(":0", tc.rp, slog.New(slog.DiscardHandler))
			rec := httptest.NewRecorder()
			s.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("status code = %d, want 200", rec.Code)
			}
			var body map[string]any
			if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body["status"] != tc.wantStatus {
				t.Errorf("status = %q, want %q", body["status"], tc.wantStatus)
			}
			if body["role"] != tc.wantRole {
				t.Errorf("role = %q, want %q", body["role"], tc.wantRole)
			}
		})
	}
}
