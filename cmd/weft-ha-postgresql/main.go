// Command weft-ha-postgresql is the Go-native PostgreSQL high-availability
// operator for openweft. One agent runs alongside each Postgres micro-VM.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	weftslognats "github.com/openweft/weft-slognats"

	"github.com/openweft/weft-ha-postgresql/internal/api"
	"github.com/openweft/weft-ha-postgresql/internal/config"
	"github.com/openweft/weft-ha-postgresql/internal/dcs"
	"github.com/openweft/weft-ha-postgresql/internal/fencing"
	"github.com/openweft/weft-ha-postgresql/internal/metrics"
	"github.com/openweft/weft-ha-postgresql/internal/postgres"
	"github.com/openweft/weft-ha-postgresql/internal/reconcile"
)

// Build metadata, injected via -ldflags.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:          "weft-ha-postgresql",
		Short:        "Go-native PostgreSQL high-availability operator for openweft",
		Long:         "weft-ha-postgresql elects a leader through etcd, drives synchronous\nreplication, and performs fenced failover so a whole datacenter can be\nlost without data loss.",
		SilenceUsage: true,
	}
	root.AddCommand(versionCmd(), agentCmd())
	return root
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "weft-ha-postgresql %s (commit %s, built %s)\n", version, commit, date)
			return err
		},
	}
}

func agentCmd() *cobra.Command {
	var (
		cfg    config.Config
		period time.Duration
	)
	cmd := &cobra.Command{
		Use:   "agent",
		Short: "Run the per-node HA agent (one per Postgres instance)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := cfg.Validate(); err != nil {
				return fmt.Errorf("invalid config: %w", err)
			}
			return runAgent(cmd.Context(), cfg, period)
		},
	}
	f := cmd.Flags()
	f.StringVar(&cfg.NodeName, "node-name", "", "unique node name within the cluster")
	f.StringVar(&cfg.ClusterName, "cluster-name", "", "logical cluster name")
	f.StringVar(&cfg.DC, "dc", "", "failure domain (datacenter / cell)")
	f.StringSliceVar(&cfg.EtcdEndpoints, "etcd", nil, "etcd endpoints (comma-separated)")
	f.StringVar(&cfg.PostgresConnURI, "postgres-uri", "", "local libpq connection string")
	f.StringVar(&cfg.APIAddr, "api-addr", ":8008", "role API listen address")
	f.StringVar(&cfg.MetricsAddr, "metrics-addr", ":9101", "Prometheus metrics listen address")
	f.StringVar(&cfg.WeftEndpoint, "weft-endpoint", "", "weft-agent gRPC endpoint for fencing (host:port)")
	f.StringVar(&cfg.WeftTLSCA, "weft-tls-ca", "", "PEM CA bundle to verify the weft-agent server cert (REQUIRED unless --weft-insecure)")
	f.StringVar(&cfg.WeftTLSCert, "weft-tls-cert", "", "client cert for mTLS to the weft-agent (optional ; pair with --weft-tls-key)")
	f.StringVar(&cfg.WeftTLSKey, "weft-tls-key", "", "client key for mTLS to the weft-agent (optional ; pair with --weft-tls-cert)")
	f.StringVar(&cfg.WeftTLSServerName, "weft-tls-server-name", "", "override SNI / ServerName for cert verification (defaults to the endpoint host)")
	f.BoolVar(&cfg.WeftInsecure, "weft-insecure", false, "dial the weft-agent without TLS (LOUD warning ; only legitimate over SSH-tunnel / WireGuard mesh ; NEVER in production)")
	f.StringVar(&cfg.WeftProject, "weft-project", "", "weft project hosting the Postgres microVMs")
	f.IntVar(&cfg.EtcdSessionTTLSec, "etcd-session-ttl", 15, "etcd lease TTL in seconds (failover floor)")
	f.DurationVar(&cfg.FenceTimeout, "fence-timeout", 30*time.Second, "wait-for-stopped timeout during fencing")
	f.DurationVar(&period, "reconcile-interval", 5*time.Second, "reconcile loop interval")
	return cmd
}

func runAgent(ctx context.Context, cfg config.Config, period time.Duration) error {
	// Logger fans out to stderr (always) AND NATS WARN+ERROR records
	// at weft.ha.postgres.<node>.log so weft-doctor can pick up
	// failover incidents. WEFT_NATS_URL env unset → stderr-only.
	log, logCloser := weftslognats.SetupFromEnv("weft.ha.postgres." + cfg.NodeName + ".log")
	defer logCloser.Close()
	slog.SetDefault(log)
	defer weftslognats.PanicReporter("weft-ha-postgresql")

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pg := postgres.NewLocalController(cfg.PostgresConnURI, log)
	defer pg.Close()

	store := dcs.NewEtcdDCS(cfg.EtcdEndpoints, cfg.ClusterName, cfg.EtcdSessionTTLSec, log)
	defer func() { _ = store.Close() }()

	// Wire the fencer. Configuration matrix :
	//   --weft-endpoint absent              → NoopFencer (test only, loud warn)
	//   --weft-endpoint + --weft-tls-ca     → TLS-gated fencer (RECOMMENDED)
	//   --weft-endpoint + --weft-insecure   → insecure dial (loud warn)
	//   --weft-endpoint without either flag → ERROR : refuse to start
	var fencer fencing.Fencer
	if cfg.WeftEndpoint == "" {
		log.Warn("no --weft-endpoint configured ; using NoopFencer — DO NOT run unattended")
		fencer = fencing.NewNoopFencer(log)
	} else {
		var stopper *fencing.GRPCStopper
		switch {
		case cfg.WeftTLSCA != "":
			tlsCfg, err := fencing.LoadClientTLSConfig(cfg.WeftTLSCA, cfg.WeftTLSCert, cfg.WeftTLSKey, cfg.WeftTLSServerName)
			if err != nil {
				return fmt.Errorf("weft-tls : %w", err)
			}
			stopper = fencing.NewGRPCStopperTLS(cfg.WeftEndpoint, cfg.WeftProject, tlsCfg, log)
			log.Info("fencer wired with TLS",
				"endpoint", cfg.WeftEndpoint,
				"ca", cfg.WeftTLSCA,
				"mtls", cfg.WeftTLSCert != "")
		case cfg.WeftInsecure:
			stopper = fencing.NewGRPCStopper(cfg.WeftEndpoint, cfg.WeftProject, log)
			log.Warn("fencer wired WITHOUT TLS (--weft-insecure) ; MITM can swallow StopVM and cause split-brain ; only legitimate over SSH-tunnel / WireGuard mesh ; NEVER in production")
		default:
			return fmt.Errorf("fencer : --weft-endpoint requires either --weft-tls-ca <path> (recommended) or --weft-insecure (dev only) ; refusing to start with an unauthenticated fencer that could silently split-brain")
		}
		defer stopper.Close()
		fencer = fencing.NewVMFencer(stopper, cfg.FenceTimeout, log)
	}

	apiSrv := api.New(cfg.APIAddr, pg, log)
	if err := apiSrv.Start(); err != nil {
		return fmt.Errorf("starting role API: %w", err)
	}
	defer shutdown(apiSrv)

	metricsSrv := metrics.New(cfg.MetricsAddr, log)
	if err := metricsSrv.Start(); err != nil {
		return fmt.Errorf("starting metrics server: %w", err)
	}
	defer shutdown(metricsSrv)

	log.Info("weft-ha-postgresql agent started",
		"node", cfg.NodeName, "cluster", cfg.ClusterName, "dc", cfg.DC,
		"api", cfg.APIAddr, "metrics", cfg.MetricsAddr)

	r := reconcile.New(pg, store, fencer, period, log)
	// selfFn rebuilds the Member identity every tick so a hot-edit of
	// the conn URI (e.g. password rotation) takes effect on the next
	// reconcile without restarting the daemon.
	r.SetSelfFn(func() dcs.Member {
		return dcs.Member{
			Name:    cfg.NodeName,
			DC:      cfg.DC,
			APIAddr: cfg.APIAddr,
			ConnURI: cfg.PostgresConnURI,
		}
	})
	if err := r.Run(ctx); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}

type shutdowner interface {
	Shutdown(context.Context) error
}

func shutdown(s shutdowner) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.Shutdown(ctx)
}
