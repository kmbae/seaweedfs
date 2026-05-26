package remote

import (
	"testing"
)

func TestIsLocalHost(t *testing.T) {
	cases := map[string]bool{
		"http://127.0.0.1:8444":  true,
		"http://localhost:8444":  true,
		"http://10.0.0.5:8444":   false,
		"http://volume-pod:8444": false,
	}
	for input, want := range cases {
		if got := IsLocalHost(input); got != want {
			t.Fatalf("IsLocalHost(%q) = %v, want %v", input, got, want)
		}
	}
}

func TestParseVolumeHost(t *testing.T) {
	host, err := ParseVolumeHost("http://100.64.1.10:8444")
	if err != nil {
		t.Fatal(err)
	}
	if host != "100.64.1.10" {
		t.Fatalf("host = %q", host)
	}
}
