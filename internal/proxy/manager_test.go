package proxy

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erikbooij/portscope/internal/config"
	"github.com/erikbooij/portscope/internal/observation"
)

type discardSink struct{}

func (discardSink) Record(observation.Interaction) {}

type countingAdapter struct{ starts *atomic.Int32 }

func (a countingAdapter) Run(ctx context.Context, _ config.Upstream, _ observation.Sink, ready func(string)) error {
	a.starts.Add(1)
	ready("ready")
	<-ctx.Done()
	return ctx.Err()
}

type failingAdapter struct{ starts *atomic.Int32 }

func (a failingAdapter) Run(context.Context, config.Upstream, observation.Sink, func(string)) error {
	a.starts.Add(1)
	return errors.New("invalid runtime TLS material")
}

func TestApplyKeepsUnchangedAdaptersAndRestartsChangedOnes(t *testing.T) {
	var starts atomic.Int32
	manager := NewManager(discardSink{}, map[string]Factory{"http": func() Adapter { return countingAdapter{starts: &starts} }})
	defer manager.Close()
	ctx := context.Background()
	item := config.Upstream{ID: "one", Name: "API", Protocol: "http", ListenAddr: "127.0.0.1:9000", Target: "http://localhost:3000", Enabled: true}
	manager.Apply(ctx, []config.Upstream{item})
	waitFor(t, func() bool { return starts.Load() == 1 })
	manager.Apply(ctx, []config.Upstream{item})
	time.Sleep(20 * time.Millisecond)
	if starts.Load() != 1 {
		t.Fatalf("unchanged adapter restarted %d times", starts.Load())
	}
	item.Target = "http://localhost:3001"
	manager.Apply(ctx, []config.Upstream{item})
	waitFor(t, func() bool { return starts.Load() == 2 })
}

func TestFailedAdapterCanBeRetriedWithoutChangingConfiguration(t *testing.T) {
	var starts atomic.Int32
	manager := NewManager(discardSink{}, map[string]Factory{"http": func() Adapter { return failingAdapter{starts: &starts} }})
	defer manager.Close()
	item := config.Upstream{ID: "one", Name: "API", Protocol: "http", ListenAddr: "127.0.0.1:9000", Target: "http://localhost:3000", Enabled: true}
	manager.Apply(context.Background(), []config.Upstream{item})
	waitFor(t, func() bool { return starts.Load() == 1 && manager.Statuses()[0].State == "error" })
	manager.Apply(context.Background(), []config.Upstream{item})
	waitFor(t, func() bool { return starts.Load() == 2 })
}
func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
