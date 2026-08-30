package observation

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
)

type Store struct {
	mu          sync.RWMutex
	path        string
	max         int
	items       []Interaction
	writes      int
	lastError   error
	subscribers map[chan Interaction]struct{}
}

func OpenStore(path string, max int) (*Store, error) {
	if max < 1 {
		return nil, errors.New("observation retention must be positive")
	}
	store := &Store{path: path, max: max, subscribers: make(map[chan Interaction]struct{})}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create observation directory: %w", err)
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open observations: %w", err)
	}
	defer file.Close()
	if err := file.Chmod(0o600); err != nil {
		return nil, fmt.Errorf("secure observations: %w", err)
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 2*1024*1024)
	loaded := 0
	for scanner.Scan() {
		var item Interaction
		if json.Unmarshal(scanner.Bytes(), &item) == nil {
			store.items = append(store.items, item)
			loaded++
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read observations: %w", err)
	}
	if len(store.items) > max {
		store.items = slices.Clone(store.items[len(store.items)-max:])
	}
	if loaded > max {
		if err := store.rewriteLocked(); err != nil {
			return nil, fmt.Errorf("compact observations: %w", err)
		}
	}
	return store, nil
}

func (s *Store) Record(item Interaction) {
	s.mu.Lock()
	s.items = append(s.items, item)
	if len(s.items) > s.max {
		s.items = slices.Clone(s.items[len(s.items)-s.max:])
	}
	encoded, encodeErr := json.Marshal(item)
	if encodeErr != nil {
		s.lastError = encodeErr
	} else if file, openErr := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600); openErr != nil {
		s.lastError = openErr
	} else {
		if chmodErr := file.Chmod(0o600); chmodErr != nil {
			s.lastError = chmodErr
		} else if _, writeErr := file.Write(append(encoded, '\n')); writeErr != nil {
			s.lastError = writeErr
		} else if closeErr := file.Close(); closeErr != nil {
			s.lastError = closeErr
		} else {
			s.lastError = nil
		}
		_ = file.Close()
	}
	s.writes++
	if len(s.items) == s.max && s.writes >= max(1, s.max/4) {
		if err := s.rewriteLocked(); err != nil {
			s.lastError = err
		}
		s.writes = 0
	}
	for subscriber := range s.subscribers {
		select {
		case subscriber <- item:
		default:
		}
	}
	s.mu.Unlock()
}

func (s *Store) List(query Query) []Interaction {
	s.mu.RLock()
	defer s.mu.RUnlock()
	limit := query.Limit
	if limit <= 0 || limit > s.max {
		limit = min(250, s.max)
	}
	needle := strings.ToLower(query.Search)
	result := make([]Interaction, 0, min(limit, len(s.items)))
	for i := len(s.items) - 1; i >= 0 && len(result) < limit; i-- {
		item := s.items[i]
		if query.UpstreamID != "" && item.UpstreamID != query.UpstreamID {
			continue
		}
		if query.Protocol != "" && item.Protocol != query.Protocol {
			continue
		}
		if query.Outcome != "" && item.Outcome != query.Outcome {
			continue
		}
		if needle != "" && !strings.Contains(strings.ToLower(item.Operation+" "+item.Request.Summary+" "+item.Request.Text+" "+item.Response.Text+" "+item.Error), needle) {
			jsonText := string(item.Request.JSON) + " " + string(item.Response.JSON)
			if !strings.Contains(strings.ToLower(jsonText), needle) {
				continue
			}
		}
		result = append(result, item)
	}
	return result
}

func (s *Store) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = nil
	s.writes = 0
	if err := os.WriteFile(s.path, nil, 0o600); err != nil {
		s.lastError = err
		return fmt.Errorf("clear observations: %w", err)
	}
	s.lastError = nil
	return nil
}

func (s *Store) PersistenceError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastError
}

func (s *Store) rewriteLocked() error {
	temporary := s.path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	encoder := json.NewEncoder(file)
	for _, item := range s.items {
		if err := encoder.Encode(item); err != nil {
			_ = file.Close()
			return err
		}
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(temporary, s.path)
}

func (s *Store) Subscribe() (<-chan Interaction, func()) {
	channel := make(chan Interaction, 64)
	s.mu.Lock()
	s.subscribers[channel] = struct{}{}
	s.mu.Unlock()
	return channel, func() {
		s.mu.Lock()
		if _, ok := s.subscribers[channel]; ok {
			delete(s.subscribers, channel)
			close(channel)
		}
		s.mu.Unlock()
	}
}
