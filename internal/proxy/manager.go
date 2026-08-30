package proxy

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
)

// Adapter is the deliberate protocol seam. Implementations own connection state,
// framing, redaction, correlation, and protocol-specific observation details.
type Adapter interface {
	Run(context.Context, config.Upstream, observation.Sink, func(string)) error
}

type Factory func() Adapter

type Status struct {
	UpstreamID string    `json:"upstreamId"`
	State      string    `json:"state"`
	Detail     string    `json:"detail,omitempty"`
	Since      time.Time `json:"since"`
}
type running struct {
	cancel context.CancelFunc
	done   chan struct{}
	config config.Upstream
}
type Manager struct {
	mu        sync.RWMutex
	factories map[string]Factory
	sink      observation.Sink
	running   map[string]running
	statuses  map[string]Status
}

func NewManager(sink observation.Sink, factories map[string]Factory) *Manager {
	return &Manager{factories: factories, sink: sink, running: make(map[string]running), statuses: make(map[string]Status)}
}

func (m *Manager) Apply(parent context.Context, items []config.Upstream) {
	desired := make(map[string]config.Upstream, len(items))
	for _, item := range items {
		desired[item.ID] = item
	}
	var stopping []running
	m.mu.Lock()
	for id, process := range m.running {
		item, exists := desired[id]
		if exists && reflect.DeepEqual(item, process.config) {
			continue
		}
		process.cancel()
		stopping = append(stopping, process)
		delete(m.running, id)
	}
	for id := range m.statuses {
		if _, exists := desired[id]; !exists {
			delete(m.statuses, id)
		}
	}
	m.mu.Unlock()
	for _, process := range stopping {
		select {
		case <-process.done:
		case <-time.After(2 * time.Second):
		}
	}
	m.mu.Lock()
	for _, item := range items {
		if _, exists := m.running[item.ID]; exists {
			continue
		}
		if !item.Enabled {
			m.statuses[item.ID] = Status{UpstreamID: item.ID, State: "disabled", Since: time.Now()}
			continue
		}
		factory := m.factories[item.Protocol]
		if factory == nil {
			m.statuses[item.ID] = Status{UpstreamID: item.ID, State: "error", Detail: "no adapter registered for " + item.Protocol, Since: time.Now()}
			continue
		}
		ctx, cancel := context.WithCancel(parent)
		done := make(chan struct{})
		m.running[item.ID] = running{cancel: cancel, done: done, config: item}
		m.statuses[item.ID] = Status{UpstreamID: item.ID, State: "starting", Since: time.Now()}
		go m.run(ctx, item, factory(), done)
	}
	m.mu.Unlock()
}

func (m *Manager) run(ctx context.Context, item config.Upstream, adapter Adapter, done chan struct{}) {
	defer close(done)
	setStatus := func(detail string) {
		m.mu.Lock()
		if process, current := m.running[item.ID]; current && process.done == done {
			m.statuses[item.ID] = Status{UpstreamID: item.ID, State: "running", Detail: detail, Since: time.Now()}
		}
		m.mu.Unlock()
	}
	err := adapter.Run(ctx, item, m.sink, setStatus)
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	m.mu.Lock()
	if process, current := m.running[item.ID]; current && process.done == done {
		m.statuses[item.ID] = Status{UpstreamID: item.ID, State: "error", Detail: fmt.Sprintf("%v", err), Since: time.Now()}
		delete(m.running, item.ID)
	}
	m.mu.Unlock()
}

func (m *Manager) Statuses() []Status {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Status, 0, len(m.statuses))
	for _, status := range m.statuses {
		result = append(result, status)
	}
	return result
}
func (m *Manager) Close() {
	m.mu.Lock()
	processes := make([]running, 0, len(m.running))
	for _, process := range m.running {
		process.cancel()
		processes = append(processes, process)
	}
	m.running = make(map[string]running)
	m.mu.Unlock()
	for _, process := range processes {
		select {
		case <-process.done:
		case <-time.After(2 * time.Second):
		}
	}
}
