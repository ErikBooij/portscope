package webui

import (
	"io/fs"
	"regexp"
	"strings"
	"testing"
)

func TestEmbeddedProductionFrontendIsSelfContained(t *testing.T) {
	index, err := fs.ReadFile(Files, "dist/index.html")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(index), "/web/src/") {
		t.Fatal("embedded index still references Vite development sources")
	}
	references := regexp.MustCompile(`(?:src|href)="(/assets/[^"]+)"`).FindAllStringSubmatch(string(index), -1)
	if len(references) < 2 {
		t.Fatalf("embedded index has no production asset references: %s", index)
	}
	for _, reference := range references {
		if _, err := fs.Stat(Files, "dist/"+strings.TrimPrefix(reference[1], "/")); err != nil {
			t.Fatalf("embedded asset %s is missing: %v", reference[1], err)
		}
	}
}
