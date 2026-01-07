package wal

import (
	"encoding/binary"
	"os"
	"testing"
)

func newTestWAL(t *testing.T) (*WAL, func()) {
	t.Helper()

	file, err := os.CreateTemp("", "walrus-wal-test-*")
	if err != nil {
		t.Fatal(err)
	}

	path := file.Name()
	file.Close()

	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	cleanup := func() {
		os.Remove(path)
	}

	return w, cleanup
}

func TestAppendAndReadAll(t *testing.T) {
	w, cleanup := newTestWAL(t)
	defer cleanup()

	r1 := &Record{
		Op:    OpSet,
		Key:   []byte("a"),
		Value: []byte("1"),
	}

	r2 := &Record{
		Op:    OpSet,
		Key:   []byte("b"),
		Value: []byte("2"),
	}

	if err := w.Append(r1); err != nil {
		t.Fatal(err)
	}

	if err := w.Append(r2); err != nil {
		t.Fatal(err)
	}

	records, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	if string(records[0].Key) != "a" || string(records[0].Value) != "1" {
		t.Fatal("first record mismatch")
	}

	if string(records[1].Key) != "b" || string(records[1].Value) != "2" {
		t.Fatal("second record mismatch")
	}
}

func TestDeleteRecord(t *testing.T) {
	w, cleanup := newTestWAL(t)
	defer cleanup()

	r1 := &Record{
		Op:    OpSet,
		Key:   []byte("x"),
		Value: []byte("42"),
	}

	r2 := &Record{
		Op:  OpDelete,
		Key: []byte("x"),
	}

	w.Append(r1)
	w.Append(r2)

	records, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	if records[1].Op != OpDelete {
		t.Fatal("expected delete op")
	}
}

func TestPartialWriteTruncation(t *testing.T) {
	w, cleanup := newTestWAL(t)
	defer cleanup()

	r1 := &Record{
		Op:    OpSet,
		Key:   []byte("a"),
		Value: []byte("1"),
	}

	if err := w.Append(r1); err != nil {
		t.Fatal(err)
	}

	// now manually write garbage to simulate crash mid-write
	f := w.file

	// write only length prefix, not payload
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], 100) // fake length
	f.Write(buf[:])
	f.Sync()

	// now read
	records, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 valid record after truncation, got %d", len(records))
	}

	if string(records[0].Key) != "a" {
		t.Fatal("valid record was corrupted by partial write")
	}
}
