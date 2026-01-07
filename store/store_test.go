package store

import (
	"os"
	"testing"

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

	w, err := wal.Open(path)
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
	s, cleanup := newTestStore(t)
	defer cleanup()

	s.Set("name", "jerk")
	s.Set("project", "walrus")

	// simulate restart
	path := s.wal.Path() // or however you expose it

	s.Close()

	w2, err := wal.Open(path)
	if err != nil {
		t.Fatal(err)
	}

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
