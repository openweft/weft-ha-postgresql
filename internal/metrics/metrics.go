// Package metrics exposes Prometheus metrics on a port separate from the role
// API, so a scrape handler hang can never stall the reconcile loop or the
// router's primary probe.
package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metric vectors are package-level so any subsystem can update them.
var (
	// NodeRole reports the current role (0=unknown, 1=primary, 2=replica).
	NodeRole = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "weft_ha_postgresql",
		Name:      "node_role",
		Help:      "Current replication role of the node (0=unknown, 1=primary, 2=replica).",
	}, []string{"node"})

	// FailoversTotal counts failovers this agent has driven.
	FailoversTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "weft_ha_postgresql",
		Name:      "failovers_total",
		Help:      "Total number of failovers this agent has driven.",
	})
)

func init() {
	prometheus.MustRegister(NodeRole, FailoversTotal)
}

// Server serves /metrics.
type Server struct {
	srv *http.Server
	log *slog.Logger
}

// New builds a metrics server bound to addr.
func New(addr string, log *slog.Logger) *Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	return &Server{
		srv: &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second},
		log: log,
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
			s.log.Error("metrics server stopped", "err", err)
		}
	}()
	return nil
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
