// grpc_stopper.go — concrete VMStopper backed by weft-agent's gRPC API.
// Kept in this package so main.go can wire it in one line ; the import
// graph stays narrow (only weft-proto + grpc are pulled in, no heavier
// weft-client surface).

package fencing

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	weftv1 "github.com/openweft/weft-proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// GRPCStopper dials a weft-agent gRPC endpoint and uses StopVM + VMStatus
// to confirm a fenced VM has reached a stopped state.
//
// SECURITY : the fencer is the load-bearing safety hinge of the HA story.
// A MITM that swallows StopVM lets an old primary keep accepting writes
// while the new one is promoted — split-brain. TLS is therefore SECURE BY
// DEFAULT : NewGRPCStopperTLS requires a *tls.Config with at least the
// server CA pinned. NewGRPCStopper (legacy) returns an insecure stopper
// and logs a loud warning so production deployments can grep for it.
type GRPCStopper struct {
	endpoint string
	project  string
	tls      *tls.Config // nil = insecure (legacy, warned)
	log      *slog.Logger

	// Pooled connection — reused across Fence calls so we don't pay the
	// dial cost on the failover hot path.
	conn *grpc.ClientConn
}

// NewGRPCStopperTLS returns a VMStopper that dials the weft-agent over
// TLS with the supplied config. Pass at minimum a RootCAs pool that
// trusts the weft-agent's server cert (or use LoadClientTLSConfig to
// read the CA + optional mTLS key pair from disk).
//
// The function does NOT dial eagerly ; the first StopVM/WaitStopped call
// opens the connection so the daemon doesn't fail to start when the
// control plane is briefly unreachable.
func NewGRPCStopperTLS(endpoint, project string, tlsCfg *tls.Config, log *slog.Logger) *GRPCStopper {
	if tlsCfg == nil {
		// Caller error — defensive : fall back to the insecure flavour
		// rather than crash, but loudly.
		log.Error("NewGRPCStopperTLS called with nil *tls.Config — falling back to INSECURE ; this is a programming bug")
		return NewGRPCStopper(endpoint, project, log)
	}
	return &GRPCStopper{endpoint: endpoint, project: project, tls: tlsCfg, log: log}
}

// NewGRPCStopper returns an INSECURE VMStopper. Kept for the dev /
// SSH-tunneled-loopback case where TLS would be redundant ; logs a
// loud warning on every dial so production misconfiguration is loud.
// Prefer NewGRPCStopperTLS.
func NewGRPCStopper(endpoint, project string, log *slog.Logger) *GRPCStopper {
	return &GRPCStopper{endpoint: endpoint, project: project, log: log}
}

// LoadClientTLSConfig builds a *tls.Config from disk paths. caPath is
// REQUIRED — it pins the agent's server identity. certPath + keyPath
// are OPTIONAL and enable mTLS when both are set (recommended for
// production : the agent verifies the fencer's identity too, so an
// attacker who steals only the CA bundle can't impersonate the fencer
// to issue arbitrary StopVMs).
//
// Empty caPath returns (nil, error) — the operator has to deliberately
// pick insecure mode through the main.go CLI gate instead.
func LoadClientTLSConfig(caPath, certPath, keyPath, serverName string) (*tls.Config, error) {
	if caPath == "" {
		return nil, errors.New("LoadClientTLSConfig: --weft-tls-ca is required (or pass --weft-insecure for the dev / SSH-tunnel case)")
	}
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read CA %s: %w", caPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("CA %s: no PEM certificates parsed", caPath)
	}
	cfg := &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
		ServerName: serverName, // empty = derive from dial target
	}
	if certPath != "" || keyPath != "" {
		if certPath == "" || keyPath == "" {
			return nil, errors.New("LoadClientTLSConfig: mTLS requires both --weft-tls-cert and --weft-tls-key")
		}
		pair, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("load mTLS keypair %s + %s: %w", certPath, keyPath, err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}

func (s *GRPCStopper) dial(ctx context.Context) (weftv1.WeftAgentClient, error) {
	if s.conn != nil {
		return weftv1.NewWeftAgentClient(s.conn), nil
	}
	var creds credentials.TransportCredentials
	if s.tls != nil {
		creds = credentials.NewTLS(s.tls)
	} else {
		// Loud warning every time the connection is established — production
		// deployments running insecure surface in operator logs.
		s.log.Warn("dialing weft-agent INSECURELY (no TLS) ; MITM can swallow StopVM and cause split-brain — set --weft-tls-ca for production",
			"endpoint", s.endpoint)
		creds = insecure.NewCredentials()
	}
	cc, err := grpc.NewClient(s.endpoint, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("dial weft-agent %s: %w", s.endpoint, err)
	}
	s.conn = cc
	return weftv1.NewWeftAgentClient(cc), nil
}

// Close releases the pooled connection. Idempotent.
func (s *GRPCStopper) Close() error {
	if s.conn == nil {
		return nil
	}
	err := s.conn.Close()
	s.conn = nil
	return err
}

// StopVM submits the stop request to the weft-agent. NotFound is collapsed
// to nil — if the VM has already been torn down, there's nothing to fence.
func (s *GRPCStopper) StopVM(ctx context.Context, name string) error {
	cli, err := s.dial(ctx)
	if err != nil {
		return err
	}
	_, err = cli.StopVM(ctx, &weftv1.StopVMRequest{Name: name, Project: s.project})
	if err == nil {
		s.log.Info("StopVM submitted", "name", name)
		return nil
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
		// Already gone — accept this as "fenced".
		s.log.Info("StopVM: vm already absent", "name", name)
		return nil
	}
	return fmt.Errorf("StopVM %s: %w", name, err)
}

// WaitStopped polls VMStatus until the VM reports stopped, the context is
// cancelled, or the timeout fires. We poll every 500 ms — slow enough to
// not hammer the agent, fast enough for a sub-2 s failover when the host
// is healthy.
func (s *GRPCStopper) WaitStopped(ctx context.Context, name string, timeout time.Duration) error {
	cli, err := s.dial(ctx)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	for {
		resp, err := cli.VMStatus(ctx, &weftv1.VMStatusRequest{Name: name, Project: s.project})
		if err != nil {
			if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
				// Gone from the registry — count as stopped.
				return nil
			}
			// Soft errors (transient network) — retry until deadline.
			s.log.Debug("VMStatus error (retrying)", "name", name, "err", err)
		} else if vm := resp.GetVm(); vm != nil {
			if isStopped(vm.GetState()) {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return errors.New("wait-stopped: timeout reached without confirmed-stopped state")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// isStopped maps the weft-proto VMInfo.State enum to a single boolean
// "this VM cannot accept writes". Both STOPPED (clean halt) and
// NOT_CREATED (the agent has nothing here) are safe states for fencing
// — the guest cannot commit writes. ERROR is also accepted : the agent
// has lost control of the VM, and the safe assumption is that whatever
// state Postgres is in, it can't talk to its disk anyway.
func isStopped(state weftv1.VMState) bool {
	switch state {
	case weftv1.VMState_VM_STATE_STOPPED,
		weftv1.VMState_VM_STATE_NOT_CREATED,
		weftv1.VMState_VM_STATE_ERROR:
		return true
	}
	return false
}
