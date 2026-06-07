// Package api exposes the role endpoints the SQL router probes to find the
// primary. The contract mirrors Patroni so the router (caddy-l4 with an HTTP
// active health check) and existing tooling work unchanged.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/openweft/weft-ha-postgresql/internal/postgres"
)

// RoleProvider reports the current replication role of the local node.
type RoleProvider interface {
	Role(ctx context.Context) (postgres.Role, error)
}

// Server serves the role API.
//
// Contract:
//
//	GET /primary  -> 200 only when this node is the primary, else 503
//	GET /replica  -> 200 only when this node is a replica, else 503
//	GET /health   -> 200 always, body reports the role
//
// The router points its HTTP health check at /primary so a single upstream —
// the live primary — is selected, and traffic follows failover automatically.
type Server struct {
	rp  RoleProvider
	log *slog.Logger
	srv *http.Server
}

// New builds a role API server bound to addr.
func New(addr string, rp RoleProvider, log *slog.Logger) *Server {
	s := &Server{rp: rp, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /primary", s.handleRole(postgres.RolePrimary))
	mux.HandleFunc("GET /replica", s.handleRole(postgres.RoleReplica))
	s.srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s
}

func (s *Server) handleRole(want postgres.Role) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		role, err := s.rp.Role(req.Context())
		if err != nil {
			// Role unknown => not the requested role: report unavailable so the
			// router never sends traffic to a node we cannot vouch for.
			s.writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"role":  postgres.RoleUnknown.String(),
				"error": err.Error(),
			})
			return
		}
		status := http.StatusServiceUnavailable
		if role == want {
			status = http.StatusOK
		}
		s.writeJSON(w, status, map[string]any{"role": role.String()})
	}
}

// handleHealth follows the IETF Health Check Response Format for HTTP APIs
// (draft-inadarei-api-health-check) : the top-level `status` field is one of
// "pass" / "fail" so a generic IETF-aware probe (Caddy active health check,
// kubelet readinessProbe, openweft webui dashboard) can switch on the same
// vocabulary across postgres-ha, forgejo-ha and irods-ha. `role` is the
// openweft extension that lets the SQL router pick the primary.
//
// Even on a failed Role() lookup we keep the HTTP code at 200 — the endpoint
// reports liveness ("the agent is responding"). Use /primary or /replica to
// drive routing decisions.
func (s *Server) handleHealth(w http.ResponseWriter, req *http.Request) {
	body := map[string]any{}
	if role, err := s.rp.Role(req.Context()); err != nil {
		body["status"] = "fail"
		body["role"] = postgres.RoleUnknown.String()
		body["error"] = err.Error()
	} else {
		body["status"] = "pass"
		body["role"] = role.String()
	}
	s.writeJSON(w, http.StatusOK, body)
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		s.log.Error("encoding api response", "err", err)
	}
}

// Start binds the listener and serves in the background.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		return err
	}
	go func() {
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("api server stopped", "err", err)
		}
	}()
	return nil
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
