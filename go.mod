module github.com/openweft/weft-ha-postgresql

go 1.26

require (

	// Prometheus: /metrics scrape on a port separate from the role API, so a
	// scrape handler hang can never stall the reconcile loop or the router probe.
	github.com/prometheus/client_golang v1.20.5
	// Cobra: openweft CLI convention (never the stdlib flag package).
	github.com/spf13/cobra v1.8.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/sys v0.22.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)
