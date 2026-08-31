package buildinfo

import (
	"runtime/debug"
	"strings"
)

var (
	Version = "dev"
	Commit  = ""
	Date    = ""
)

type Info struct {
	Version string
	Commit  string
	Date    string
	Dirty   bool
}

func Current() Info {
	result := Info{Version: Version, Commit: Commit, Date: Date}
	metadata, ok := debug.ReadBuildInfo()
	if !ok {
		return result
	}
	if result.Version == "dev" && metadata.Main.Version != "" && metadata.Main.Version != "(devel)" {
		result.Version = metadata.Main.Version
	}
	for _, setting := range metadata.Settings {
		switch setting.Key {
		case "vcs.revision":
			if result.Commit == "" {
				result.Commit = setting.Value
			}
		case "vcs.time":
			if result.Date == "" {
				result.Date = setting.Value
			}
		case "vcs.modified":
			result.Dirty = setting.Value == "true"
		}
	}
	return result
}

func (info Info) String() string {
	parts := []string{"portscope " + info.Version}
	if info.Commit != "" {
		commit := info.Commit
		if len(commit) > 12 {
			commit = commit[:12]
		}
		if info.Dirty {
			commit += "-dirty"
		}
		parts = append(parts, "commit "+commit)
	}
	if info.Date != "" {
		parts = append(parts, "built "+info.Date)
	}
	return strings.Join(parts, " · ")
}
