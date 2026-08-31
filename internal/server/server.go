package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
	"github.com/erikbooij/portscope/internal/proxy"
	"github.com/erikbooij/portscope/internal/webui"
)

type Server struct {
	config       *config.Store
	observations *observation.Store
	manager      *proxy.Manager
	root         context.Context
	logger       *slog.Logger
}

func New(root context.Context, configuration *config.Store, observations *observation.Store, manager *proxy.Manager, logger *slog.Logger) *Server {
	return &Server{root: root, config: configuration, observations: observations, manager: manager, logger: logger}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if err := s.observations.PersistenceError(); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "degraded", "storage": "observation journal unavailable", "time": time.Now()})
			return
		}
		writeJSON(w, 200, map[string]any{"status": "ok", "time": time.Now()})
	})
	mux.HandleFunc("GET /api/upstreams", s.listUpstreams)
	mux.HandleFunc("POST /api/upstreams", s.putUpstream)
	mux.HandleFunc("PUT /api/upstreams/{id}", s.putUpstream)
	mux.HandleFunc("DELETE /api/upstreams/{id}", s.deleteUpstream)
	mux.HandleFunc("GET /api/interactions", s.listInteractions)
	mux.HandleFunc("DELETE /api/interactions", s.clearInteractions)
	mux.HandleFunc("GET /api/events", s.events)
	dist, _ := fs.Sub(webui.Files, "dist")
	files := http.FileServer(http.FS(dist))
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if strings.HasPrefix(path, "api/") {
			problem(w, http.StatusNotFound, "API endpoint not found")
			return
		}
		if path != "" {
			if _, err := fs.Stat(dist, path); err == nil {
				if strings.HasPrefix(path, "assets/") {
					w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
				}
				files.ServeHTTP(w, r)
				return
			}
		}
		data, err := fs.ReadFile(dist, "index.html")
		if err != nil {
			http.Error(w, "web UI unavailable", 500)
			return
		}
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(data)
	})
	return requestLog(s.logger, securityHeaders(sameOrigin(mux)))
}

func (s *Server) listUpstreams(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]any{"items": config.PublicUpstreams(s.config.List()), "statuses": s.manager.Statuses()})
}
func (s *Server) putUpstream(w http.ResponseWriter, r *http.Request) {
	var item config.Upstream
	if err := decodeJSON(r, &item); err != nil {
		problem(w, 400, err.Error())
		return
	}
	if id := r.PathValue("id"); id != "" {
		item.ID = id
		existing, ok := s.config.Get(id)
		if !ok {
			problem(w, 404, "upstream not found")
			return
		}
		item = config.MergeSecrets(item, existing)
	}
	saved, err := s.config.Put(item)
	if err != nil {
		problem(w, 400, err.Error())
		return
	}
	if err := s.applyConfiguration(); err != nil {
		problem(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, config.PublicUpstream(saved))
}
func (s *Server) deleteUpstream(w http.ResponseWriter, r *http.Request) {
	if err := s.config.Delete(r.PathValue("id")); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			problem(w, 404, "upstream not found")
			return
		}
		problem(w, 500, err.Error())
		return
	}
	if err := s.applyConfiguration(); err != nil {
		problem(w, 500, err.Error())
		return
	}
	w.WriteHeader(204)
}

func (s *Server) applyConfiguration() error {
	items, err := s.config.RuntimeList()
	if err != nil {
		return fmt.Errorf("materialize configuration: %w", err)
	}
	s.manager.Apply(s.root, items)
	return nil
}
func (s *Server) listInteractions(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items := s.observations.List(observation.Query{UpstreamID: r.URL.Query().Get("upstream"), Protocol: r.URL.Query().Get("protocol"), Outcome: r.URL.Query().Get("outcome"), Search: r.URL.Query().Get("search"), Limit: limit})
	writeJSON(w, 200, map[string]any{"items": items})
}
func (s *Server) clearInteractions(w http.ResponseWriter, _ *http.Request) {
	if err := s.observations.Clear(); err != nil {
		problem(w, 500, err.Error())
		return
	}
	w.WriteHeader(204)
}
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		problem(w, 500, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	events, cancel := s.observations.Subscribe()
	defer cancel()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	fmt.Fprint(w, "event: ready\ndata: {}\n\n")
	flusher.Flush()
	for {
		select {
		case item := <-events:
			data, _ := json.Marshal(item)
			fmt.Fprintf(w, "event: interaction\ndata: %s\n\n", data)
			flusher.Flush()
		case <-heartbeat.C:
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; connect-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}

func sameOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
			if origin := r.Header.Get("Origin"); origin != "" {
				parsed, err := url.Parse(origin)
				if err != nil || !strings.EqualFold(parsed.Host, r.Host) {
					problem(w, http.StatusForbidden, "cross-origin management requests are not allowed")
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}
func decodeJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("invalid JSON: request body must contain exactly one object")
	}
	return nil
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func problem(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
func requestLog(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		logger.Debug("http request", "method", r.Method, "path", r.URL.Path, "duration", time.Since(started))
	})
}
