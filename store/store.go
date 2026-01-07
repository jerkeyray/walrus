package store

import (
	"github.com/jerkeyray/walrus/wal"
	"sync"
)

type Store struct {
	mu   sync.Mutex
	data map[string]string
	wal  *wal.WAL
}

func New(w *wal.WAL) *Store {
	return &Store{
		data: make(map[string]string),
		wal:  w,
	}
}

func (s *Store) Set(key, value string) error {
	rec := &wal.Record{
		Op:    wal.OpSet,
		Key:   []byte(key),
		Value: []byte(value),
	}

	// write to WAL first
	if err := s.wal.Append(rec); err != nil {
		return err
	}

	// mutate memory
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value

	return nil
}

// memory only, does not go through WAL
func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.data[key]
	return val, ok
}

func (s *Store) Delete(key string) error {
	rec := &wal.Record{
		Op:  wal.OpDelete,
		Key: []byte(key),
	}

	if err := s.wal.Append(rec); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)

	return nil
}

func (s *Store) Has(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.data[key]
	return val, ok
}

func (s *Store) Recover() error {
	records, err := s.wal.ReadAll()
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, rec := range records {
		switch rec.Op {
		case wal.OpSet:
			s.data[string(rec.Key)] = string(rec.Value)
		case wal.OpDelete:
			delete(s.data, string(rec.Key))
		}
	}
	return nil
}

func (s *Store) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}

	return keys
}

func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.data)
}
