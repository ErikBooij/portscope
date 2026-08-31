package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/erikbooij/portscope/internal/config"
)

func TestInitCreatesVersionedRepositoryConfigurationWithoutOverwriting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", config.DefaultFilename)
	var stdout, stderr bytes.Buffer
	if code := Execute(context.Background(), []string{"init", "--config", path}, &stdout, &stderr); code != 0 {
		t.Fatalf("init exit %d: %s", code, stderr.String())
	}
	store, err := config.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	items := store.List()
	if len(items) != 1 || items[0].ID != "api" || items[0].Protocol != "http" {
		t.Fatalf("starter configuration = %#v", items)
	}
	ignore, err := os.ReadFile(filepath.Join(filepath.Dir(path), ".gitignore"))
	if err != nil || string(ignore) != "/.portscope/\n" {
		t.Fatalf("state ignore = %q, %v", ignore, err)
	}
	if code := Execute(context.Background(), []string{"init", "--config", path}, &stdout, &stderr); code == 0 || !strings.Contains(stderr.String(), "already exists") {
		t.Fatalf("second init exit %d: %s", code, stderr.String())
	}
}

func TestEnsureStateIgnoredPreservesExistingGitignore(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, ".gitignore")
	if err := os.WriteFile(path, []byte("node_modules/"), 0o644); err != nil {
		t.Fatal(err)
	}
	changed, err := ensureStateIgnored(directory)
	if err != nil || !changed {
		t.Fatalf("first update = %v, %v", changed, err)
	}
	changed, err = ensureStateIgnored(directory)
	if err != nil || changed {
		t.Fatalf("second update = %v, %v", changed, err)
	}
	data, _ := os.ReadFile(path)
	if string(data) != "node_modules/\n/.portscope/\n" {
		t.Fatalf("gitignore = %q", data)
	}
}

func TestVersionAndHelpDoNotNeedAConfiguration(t *testing.T) {
	for _, args := range [][]string{{"version"}, {"--version"}, {"help"}, {"--help"}} {
		var stdout, stderr bytes.Buffer
		if code := Execute(context.Background(), args, &stdout, &stderr); code != 0 || stdout.Len() == 0 || stderr.Len() != 0 {
			t.Fatalf("%v: exit=%d stdout=%q stderr=%q", args, code, stdout.String(), stderr.String())
		}
	}
}

func TestRunMissingDefaultConfigurationExplainsInitialization(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.json")
	var stdout, stderr bytes.Buffer
	if code := Execute(context.Background(), []string{"--config", path}, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "portscope init") {
		t.Fatalf("exit=%d stderr=%q", code, stderr.String())
	}
}
