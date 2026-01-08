package store

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jerkeyray/walrus/wal"
)

func newTestStore(t *testing.T) (*Store, func()) {
	t.Helper()

	file, err := os.CreateTemp("", "walrus-store-test-*")
	if err != nil {
		t.Fatal(err)
	}

	path := file.Name()
	file.Close()

	// Use fast flush interval for tests
	w, err := wal.Open(path, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	s := New(w)

	cleanup := func() {
		s.Close()
		os.Remove(path)
	}

	return s, cleanup
}

func TestSetAndGet(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	if err := s.Set("name", "jerk"); err != nil {
		t.Fatal(err)
	}

	val, ok := s.Get("name")
	if !ok {
		t.Fatal("expected key 'name' to exist")
	}

	if val != "jerk" {
		t.Fatalf("expected 'jerk', got '%s'", val)
	}
}

func TestHasAndLen(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	s.Set("a", "1")
	s.Set("b", "2")

	if !s.Has("a") {
		t.Fatal("expected to have key 'a'")
	}

	if !s.Has("b") {
		t.Fatal("expected to have key 'b'")
	}

	if s.Len() != 2 {
		t.Fatalf("expected len 2, got %d", s.Len())
	}
}

func TestKeys(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	s.Set("x", "1")
	s.Set("y", "2")

	keys := s.Keys()

	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}

	foundX := false
	foundY := false

	for _, k := range keys {
		if k == "x" {
			foundX = true
		}
		if k == "y" {
			foundY = true
		}
	}

	if !foundX || !foundY {
		t.Fatal("keys missing from Keys() output")
	}
}

func TestDelete(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	s.Set("temp", "123")

	if !s.Has("temp") {
		t.Fatal("expected key to exist before delete")
	}

	if err := s.Delete("temp"); err != nil {
		t.Fatal(err)
	}

	if s.Has("temp") {
		t.Fatal("expected key to be deleted")
	}

	if s.Len() != 0 {
		t.Fatalf("expected len 0 after delete, got %d", s.Len())
	}
}

func TestRecovery(t *testing.T) {
	file, err := os.CreateTemp("", "walrus-store-test-*")
	if err != nil {
		t.Fatal(err)
	}

	path := file.Name()
	file.Close()
	defer os.Remove(path)

	// Create first store instance
	w, err := wal.Open(path, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	s := New(w)

	s.Set("name", "jerk")
	s.Set("project", "walrus")

	// Ensure data is flushed
	s.Commit()

	s.Close()

	// Reopen and recover
	w2, err := wal.Open(path, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	s2 := New(w2)

	if err := s2.Recover(); err != nil {
		t.Fatal(err)
	}

	val, ok := s2.Get("name")
	if !ok || val != "jerk" {
		t.Fatal("recovery failed for key 'name'")
	}

	val, ok = s2.Get("project")
	if !ok || val != "walrus" {
		t.Fatal("recovery failed for key 'project'")
	}
}

// Test batch operations
func TestBatch(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	// Use Batch to group multiple operations
	err := s.Batch(func(s *Store) error {
		s.Set("key1", "val1")
		s.Set("key2", "val2")
		s.Set("key3", "val3")
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	// Verify all keys exist
	if !s.Has("key1") || !s.Has("key2") || !s.Has("key3") {
		t.Fatal("batch operations did not persist all keys")
	}

	if s.Len() != 3 {
		t.Fatalf("expected 3 keys, got %d", s.Len())
	}
}

// Test concurrent reads and writes
func TestConcurrentAccess(t *testing.T) {
	s, cleanup := newTestStore(t)
	defer cleanup()

	const numWriters = 5
	const numReaders = 5
	const opsPerWorker = 50

	var wg sync.WaitGroup

	// Start writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				key := "writer-key"
				value := "value"
				if err := s.Set(key, value); err != nil {
					t.Errorf("writer %d: set failed: %v", id, err)
				}
			}
		}(i)
	}

	// Start readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerWorker; j++ {
				s.Get("writer-key")
				s.Keys()
				s.Len()
			}
		}(i)
	}

	wg.Wait()

	// Final commit to ensure all data is flushed
	if err := s.Commit(); err != nil {
		t.Fatal(err)
	}
}

// Test recovery after delete
func TestRecoveryWithDelete(t *testing.T) {
	file, err := os.CreateTemp("", "walrus-store-test-*")
	if err != nil {
		t.Fatal(err)
	}

	path := file.Name()
	file.Close()
	defer os.Remove(path)

	// Create and populate store
	w, err := wal.Open(path, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	s := New(w)

	s.Set("keep", "this")
	s.Set("delete", "me")
	s.Delete("delete")

	s.Commit()
	s.Close()

	// Reopen and recover
	w2, err := wal.Open(path, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	s2 := New(w2)
	if err := s2.Recover(); err != nil {
		t.Fatal(err)
	}

	// "keep" should exist
	if !s2.Has("keep") {
		t.Fatal("expected key 'keep' to exist after recovery")
	}

	// "delete" should NOT exist
	if s2.Has("delete") {
		t.Fatal("expected key 'delete' to be gone after recovery")
	}

	if s2.Len() != 1 {
		t.Fatalf("expected 1 key after recovery, got %d", s2.Len())
	}
}

// Test that buffered writes survive background flush
func TestBackgroundFlushPersistence(t *testing.T) {
	file, err := os.CreateTemp("", "walrus-store-test-*")
	if err != nil {
		t.Fatal(err)
	}

	path := file.Name()
	file.Close()
	defer os.Remove(path)

	// Use 50ms flush interval
	w, err := wal.Open(path, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	s := New(w)

	// Write some data
	s.Set("background", "flush-test")

	// Wait for background flush
	time.Sleep(100 * time.Millisecond)

	s.Close()

	// Reopen and verify
	w2, err := wal.Open(path, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	s2 := New(w2)
	if err := s2.Recover(); err != nil {
		t.Fatal(err)
	}

	val, ok := s2.Get("background")
	if !ok || val != "flush-test" {
		t.Fatal("background flush did not persist data")
	}
}

// Test Commit() explicitly flushes
func TestCommit(t *testing.T) {
	file, err := os.CreateTemp("", "walrus-store-test-*")
	if err != nil {
		t.Fatal(err)
	}

	path := file.Name()
	file.Close()
	defer os.Remove(path)

	// Use very long flush interval (won't auto-flush during test)
	w, err := wal.Open(path, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	s := New(w)

	s.Set("commit", "test")

	// Explicitly commit
	if err := s.Commit(); err != nil {
		t.Fatal(err)
	}

	s.Close()

	// Reopen and verify data was committed
	w2, err := wal.Open(path, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	s2 := New(w2)
	if err := s2.Recover(); err != nil {
		t.Fatal(err)
	}

	val, ok := s2.Get("commit")
	if !ok || val != "test" {
		t.Fatal("commit did not persist data")
	}
}

// Benchmark Store operations
func BenchmarkStoreSet(b *testing.B) {
	file, err := os.CreateTemp("", "walrus-bench-*")
	if err != nil {
		b.Fatal(err)
	}

	path := file.Name()
	file.Close()
	defer os.Remove(path)

	w, err := wal.Open(path, 100*time.Millisecond)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	s := New(w)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Set("bench", "value")
	}

	s.Commit()
}

func BenchmarkStoreGet(b *testing.B) {
	file, err := os.CreateTemp("", "walrus-bench-*")
	if err != nil {
		b.Fatal(err)
	}

	path := file.Name()
	file.Close()
	defer os.Remove(path)

	w, err := wal.Open(path, 100*time.Millisecond)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	s := New(w)
	s.Set("bench", "value")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Get("bench")
	}
}

func BenchmarkStoreBatch(b *testing.B) {
	file, err := os.CreateTemp("", "walrus-bench-*")
	if err != nil {
		b.Fatal(err)
	}

	path := file.Name()
	file.Close()
	defer os.Remove(path)

	w, err := wal.Open(path, 100*time.Millisecond)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	s := New(w)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Batch(func(s *Store) error {
			s.Set("key1", "val1")
			s.Set("key2", "val2")
			s.Set("key3", "val3")
			return nil
		})
	}
}
