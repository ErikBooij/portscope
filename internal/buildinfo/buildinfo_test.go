package buildinfo

import "testing"

func TestInfoStringIsUsefulForReleaseAndDevelopmentBuilds(t *testing.T) {
	if got := (Info{Version: "v1.2.3", Commit: "1234567890abcdef", Date: "2026-08-30T12:00:00Z"}).String(); got != "portscope v1.2.3 · commit 1234567890ab · built 2026-08-30T12:00:00Z" {
		t.Fatalf("release string = %q", got)
	}
	if got := (Info{Version: "dev"}).String(); got != "portscope dev" {
		t.Fatalf("development string = %q", got)
	}
}
