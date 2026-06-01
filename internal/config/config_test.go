package config

import "testing"

func validConfig() Config {
	return Config{
		NodeName:        "pg-dc1-0",
		ClusterName:     "tenant-acme",
		DC:              "dc1",
		EtcdEndpoints:   []string{"https://etcd-dc1:2379"},
		PostgresConnURI: "postgres:///postgres",
		APIAddr:         ":8008",
		MetricsAddr:     ":9101",
	}
}

func TestValidate_OK(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("expected valid config, got error: %v", err)
	}
}

func TestValidate_Errors(t *testing.T) {
	cases := map[string]func(*Config){
		"missing node name":   func(c *Config) { c.NodeName = "" },
		"missing cluster":     func(c *Config) { c.ClusterName = "" },
		"missing dc":          func(c *Config) { c.DC = "" },
		"no etcd endpoints":   func(c *Config) { c.EtcdEndpoints = nil },
		"missing postgres":    func(c *Config) { c.PostgresConnURI = "" },
		"missing api addr":    func(c *Config) { c.APIAddr = "" },
		"missing metrics addr": func(c *Config) { c.MetricsAddr = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := validConfig()
			mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatalf("expected error for %q, got nil", name)
			}
		})
	}
}
