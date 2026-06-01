package api

import (
	"context"
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
	if code := do(t, fakeRP{err: postgres.ErrNotImplemented}, "/primary"); code != http.StatusServiceUnavailable {
		t.Errorf("/primary with role error: got %d, want 503", code)
	}
}

func TestHealthAlwaysOK(t *testing.T) {
	if code := do(t, fakeRP{err: postgres.ErrNotImplemented}, "/health"); code != http.StatusOK {
		t.Errorf("/health: got %d, want 200", code)
	}
}
