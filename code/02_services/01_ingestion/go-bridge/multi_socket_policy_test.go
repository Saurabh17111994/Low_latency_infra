package main

import "testing"

// CHG-320: the multi-socket approval policy mirrored from the Java
// IngestionConfig gate — extra Arrow sockets need an explicit approval AND a
// non-production deployment; a blank deployment fails closed. Single-socket
// runs never consult the policy, so these cases are about >1 slot only.
func TestMultiSocketAllowedByPolicy(t *testing.T) {
	cases := []struct {
		name        string
		approved    string
		depEnv      string
		depEnvAlias string
		want        bool
	}{
		{"approved in dev", "true", "dev", "", true},
		{"approved case-insensitive", "TRUE", "dev", "", true},
		{"approved via DEPLOY_ENV alias", "true", "", "dev", true},
		{"approved in staging", "true", "staging", "", true},
		{"unapproved in dev", "false", "dev", "", false},
		{"unset approval", "", "dev", "", false},
		{"junk approval fails closed", "yes", "dev", "", false},
		{"production refuses", "true", "production", "", false},
		{"prod refuses", "true", "prod", "", false},
		{"blank deployment fails closed", "true", "", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("ARROW_HFT_MULTI_CONNECTION_APPROVED", c.approved)
			t.Setenv("DEPLOYMENT_ENV", c.depEnv)
			t.Setenv("DEPLOY_ENV", c.depEnvAlias)
			got, reason := multiSocketAllowedByPolicy()
			if got != c.want {
				t.Fatalf("allowed=%v want=%v (reason=%q)", got, c.want, reason)
			}
			if !got && reason == "" {
				t.Fatalf("a refusal must carry a reason")
			}
		})
	}
}
