package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/erikbooij/portscope/internal/buildinfo"
	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
	"github.com/erikbooij/portscope/internal/proxy"
	"github.com/erikbooij/portscope/internal/proxy/httpadapter"
	"github.com/erikbooij/portscope/internal/proxy/mongoadapter"
	"github.com/erikbooij/portscope/internal/proxy/mysqladapter"
	"github.com/erikbooij/portscope/internal/proxy/postgresadapter"
	"github.com/erikbooij/portscope/internal/proxy/rabbitadapter"
	"github.com/erikbooij/portscope/internal/proxy/redisadapter"
	appserver "github.com/erikbooij/portscope/internal/server"
)

const usage = `Portscope — local inspection proxy

Usage:
  portscope [run] [flags]
  portscope init [--config path]
  portscope version

Run flags:
  --config path      project configuration (default ./portscope.json)
  --state-dir path   local capture state (default .portscope beside config)
  --addr address     management UI address (default 127.0.0.1:8090)

Run "portscope init" once in a repository, commit portscope.json and the
.gitignore update, then run "portscope" whenever inspection is needed.
`

func Execute(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		fmt.Fprint(stdout, usage)
		return 0
	}
	command := "run"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		command, args = args[0], args[1:]
	}
	switch command {
	case "run":
		return run(ctx, args, stdout, stderr)
	case "init":
		return initialize(args, stdout, stderr)
	case "version":
		if len(args) != 0 {
			fmt.Fprintln(stderr, "portscope version does not accept arguments")
			return 2
		}
		fmt.Fprintln(stdout, buildinfo.Current().String())
		return 0
	case "help":
		fmt.Fprint(stdout, usage)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", command, usage)
		return 2
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	runContext, cancel := context.WithCancel(ctx)
	defer cancel()

	flags := flag.NewFlagSet("portscope", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configurationPath := flags.String("config", config.DefaultFilename, "project configuration")
	stateDirectory := flags.String("state-dir", "", "local capture state (default .portscope beside config)")
	address := flags.String("addr", "127.0.0.1:8090", "management UI address")
	showVersion := flags.Bool("version", false, "print version")
	flags.Usage = func() { fmt.Fprint(flags.Output(), usage) }
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected argument %q\n", flags.Arg(0))
		return 2
	}
	if *showVersion {
		fmt.Fprintln(stdout, buildinfo.Current().String())
		return 0
	}
	configPath, err := filepath.Abs(*configurationPath)
	if err != nil {
		fmt.Fprintf(stderr, "resolve configuration path: %v\n", err)
		return 1
	}
	statePath := filepath.Join(filepath.Dir(configPath), ".portscope")
	if *stateDirectory != "" {
		statePath, err = filepath.Abs(*stateDirectory)
		if err != nil {
			fmt.Fprintf(stderr, "resolve state directory: %v\n", err)
			return 1
		}
	}
	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	configuration, err := config.OpenStore(configPath)
	if err != nil {
		fmt.Fprintf(stderr, "open configuration: %v\n", err)
		if errors.Is(err, os.ErrNotExist) {
			fmt.Fprintln(stderr, "Run 'portscope init' in this repository to create one.")
		}
		return 1
	}
	runtimeUpstreams, err := configuration.RuntimeList()
	if err != nil {
		fmt.Fprintf(stderr, "materialize configuration: %v\n", err)
		return 1
	}
	observations, err := observation.OpenStore(filepath.Join(statePath, "interactions.jsonl"), 5000)
	if err != nil {
		fmt.Fprintf(stderr, "open observations: %v\n", err)
		return 1
	}
	manager := proxy.NewManager(observations, factories())
	manager.Apply(runContext, runtimeUpstreams)
	defer manager.Close()
	server := &http.Server{Addr: *address, Handler: appserver.New(runContext, configuration, observations, manager, logger).Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 1 << 20}
	go func() {
		<-runContext.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	fmt.Fprintf(stdout, "Portscope %s\nUI: http://%s\nConfig: %s\nState: %s\n", buildinfo.Current().Version, *address, configPath, statePath)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(stderr, "server stopped: %v\n", err)
		return 1
	}
	return 0
}

func initialize(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("portscope init", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configurationPath := flags.String("config", config.DefaultFilename, "project configuration")
	flags.Usage = func() { fmt.Fprint(flags.Output(), usage) }
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected argument %q\n", flags.Arg(0))
		return 2
	}
	path, err := filepath.Abs(*configurationPath)
	if err != nil {
		fmt.Fprintf(stderr, "resolve configuration path: %v\n", err)
		return 1
	}
	if err := config.Create(path); err != nil {
		fmt.Fprintf(stderr, "initialize configuration: %v\n", err)
		return 1
	}
	ignored, err := ensureStateIgnored(filepath.Dir(path))
	if err != nil {
		fmt.Fprintf(stderr, "warning: could not update .gitignore: %v\n", err)
	}
	fmt.Fprintf(stdout, "Created %s\n", path)
	if ignored {
		fmt.Fprintf(stdout, "Added /.portscope/ to %s\n", filepath.Join(filepath.Dir(path), ".gitignore"))
	}
	fmt.Fprintln(stdout, "Edit the upstream, commit the configuration, then run 'portscope'.")
	return 0
}

func ensureStateIgnored(directory string) (bool, error) {
	path := filepath.Join(directory, ".gitignore")
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		switch strings.TrimSpace(line) {
		case ".portscope", ".portscope/", "/.portscope", "/.portscope/":
			return false, nil
		}
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return false, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return false, err
	}
	prefix := ""
	if len(data) > 0 && data[len(data)-1] != '\n' {
		prefix = "\n"
	}
	if _, err := io.WriteString(file, prefix+"/.portscope/\n"); err != nil {
		_ = file.Close()
		return false, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return false, err
	}
	if err := file.Close(); err != nil {
		return false, err
	}
	return true, nil
}

func factories() map[string]proxy.Factory {
	return map[string]proxy.Factory{
		"http":          func() proxy.Adapter { return httpadapter.New() },
		"elasticsearch": func() proxy.Adapter { return httpadapter.New() },
		"grpc":          func() proxy.Adapter { return httpadapter.New() },
		"redis":         func() proxy.Adapter { return redisadapter.New() },
		"mysql":         func() proxy.Adapter { return mysqladapter.New() },
		"postgres":      func() proxy.Adapter { return postgresadapter.New() },
		"mongodb":       func() proxy.Adapter { return mongoadapter.New() },
		"rabbitmq":      func() proxy.Adapter { return rabbitadapter.New() },
	}
}
