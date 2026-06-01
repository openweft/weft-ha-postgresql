// Package config holds the static bootstrap configuration for a
// weft-ha-postgresql agent and its validation.
package config

import "fmt"

// Config is the static bootstrap configuration for one agent instance. In
// production it is populated from CLI flags fed by weft-network (served into
// the micro-VM alongside the Postgres data directory).
type Config struct {
	// NodeName uniquely identifies this Postgres node within the cluster.
	NodeName string
	// ClusterName groups all nodes of one logical Postgres cluster; it is the
	// key prefix under which leadership and membership live in the DCS.
	ClusterName string
	// DC is the failure domain (datacenter / cell) this node lives in. Failover
	// decisions use it so the cluster keeps a quorum spread across >=3 DCs.
	DC string
	// EtcdEndpoints lists the etcd endpoints backing the DCS.
	EtcdEndpoints []string
	// PostgresConnURI is the local libpq connection string used to manage and
	// inspect the Postgres instance.
	PostgresConnURI string
	// APIAddr is the listen address for the role API the SQL router probes.
	APIAddr string
	// MetricsAddr is the listen address for the Prometheus /metrics endpoint.
	MetricsAddr string
}

// Validate reports the first problem found with c, or nil if it is usable.
func (c Config) Validate() error {
	switch {
	case c.NodeName == "":
		return fmt.Errorf("node-name must not be empty")
	case c.ClusterName == "":
		return fmt.Errorf("cluster-name must not be empty")
	case c.DC == "":
		return fmt.Errorf("dc (failure domain) must not be empty")
	case len(c.EtcdEndpoints) == 0:
		return fmt.Errorf("at least one etcd endpoint is required")
	case c.PostgresConnURI == "":
		return fmt.Errorf("postgres-uri must not be empty")
	case c.APIAddr == "":
		return fmt.Errorf("api-addr must not be empty")
	case c.MetricsAddr == "":
		return fmt.Errorf("metrics-addr must not be empty")
	default:
		return nil
	}
}
