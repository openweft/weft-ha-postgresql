// grpc_stopper.go — concrete VMStopper backed by weft-agent's gRPC API.
// Kept in this package so main.go can wire it in one line ; the import
// graph stays narrow (only weft-proto + grpc are pulled in, no heavier
// weft-client surface).

package fencing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	weftv1 "github.com/openweft/weft-proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// GRPCStopper dials a weft-agent gRPC endpoint and uses StopVM + VMStatus
// to confirm a fenced VM has reached a stopped state. Production wiring
// in main.go : NewGRPCStopper(endpoint, project, log).
type GRPCStopper struct {
	endpoint string
	project  string
	log      *slog.Logger

	// Pooled connection — reused across Fence calls so we don't pay the
	// dial cost on the failover hot path.
	conn *grpc.ClientConn
}

// NewGRPCStopper returns a VMStopper bound to a single weft-agent endpoint.
// endpoint is "host:port" ; project is the weft project the Postgres VMs
// live in (cluster.hcl's per-plugin project). TLS is OPTIONAL today — set
// up the daemon flag --weft-tls when the agent listener has TLS enabled.
//
// The function does NOT dial eagerly ; the first StopVM/WaitStopped call
// opens the connection so the daemon doesn't fail to start when the
// control plane is briefly unreachable.
func NewGRPCStopper(endpoint, project string, log *slog.Logger) *GRPCStopper {
	return &GRPCStopper{endpoint: endpoint, project: project, log: log}
}

func (s *GRPCStopper) dial(ctx context.Context) (weftv1.WeftAgentClient, error) {
	if s.conn != nil {
		return weftv1.NewWeftAgentClient(s.conn), nil
	}
	cc, err := grpc.NewClient(s.endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
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
