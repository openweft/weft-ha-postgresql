package postgres

import "testing"

func TestRoleString(t *testing.T) {
	cases := map[Role]string{
		RolePrimary: "primary",
		RoleReplica: "replica",
		RoleUnknown: "unknown",
		Role(99):    "unknown",
	}
	for role, want := range cases {
		if got := role.String(); got != want {
			t.Errorf("Role(%d).String() = %q, want %q", role, got, want)
		}
	}
}
