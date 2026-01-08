package wal

import (
	"encoding/binary"
	"os"
	"sync"
	"testing"
	"time"
)

func newTestWAL(t *testing.T) (*WAL, func()) {
	t.Helper()

	file, err := os.CreateTemp("", "walrus-wal-test-*")
	if err != nil {
		t.Fatal(err)
	}

	path := file.Name()
	file.Close()

	// Use fast flush interval for tests
	w, err := Open(path, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	cleanup := func() {
		w.Close()
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

	// Flush before reading
	if err := w.Flush(); err != nil {
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

	// Flush before reading
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

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

	// Flush to write to disk
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// now manually write garbage to simulate crash mid-write
	w.mu.Lock()
	f := w.file

	// Write bad magic number to simulate corruption
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], 0xDEADBEEF) // wrong magic
	f.Write(buf[:])
	f.Sync()
	w.mu.Unlock()

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

// Test that data is buffered and not written until flush
func TestBuffering(t *testing.T) {
	w, cleanup := newTestWAL(t)
	defer cleanup()

	r1 := &Record{
		Op:    OpSet,
		Key:   []byte("buffered"),
		Value: []byte("data"),
	}

	// Append but don't flush
	if err := w.Append(r1); err != nil {
		t.Fatal(err)
	}

	// Should have data in buffer
	w.mu.Lock()
	bufferSize := len(w.buffer)
	w.mu.Unlock()

	if bufferSize == 0 {
		t.Fatal("expected data in buffer after Append")
	}

	// Should read zero records (data not flushed to disk)
	records, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 0 {
		t.Fatalf("expected 0 records before flush, got %d", len(records))
	}

	// Now flush
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Buffer should be empty
	w.mu.Lock()
	bufferSize = len(w.buffer)
	w.mu.Unlock()

	if bufferSize != 0 {
		t.Fatalf("expected empty buffer after flush, got %d bytes", bufferSize)
	}

	// Should read the record now
	records, err = w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record after flush, got %d", len(records))
	}

	if string(records[0].Key) != "buffered" {
		t.Fatal("record mismatch after flush")
	}
}

// Test background flush goroutine
func TestBackgroundFlush(t *testing.T) {
	file, err := os.CreateTemp("", "walrus-wal-test-*")
	if err != nil {
		t.Fatal(err)
	}

	path := file.Name()
	file.Close()
	defer os.Remove(path)

	// Use 50ms flush interval
	w, err := Open(path, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	r1 := &Record{
		Op:    OpSet,
		Key:   []byte("auto"),
		Value: []byte("flush"),
	}

	// Append without manual flush
	if err := w.Append(r1); err != nil {
		t.Fatal(err)
	}

	// Wait for background flush (2x flush interval to be safe)
	time.Sleep(100 * time.Millisecond)

	// Should be flushed to disk by now
	records, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record after background flush, got %d", len(records))
	}

	if string(records[0].Value) != "flush" {
		t.Fatal("record mismatch after background flush")
	}
}

// Test ForceFlush
func TestForceFlush(t *testing.T) {
	w, cleanup := newTestWAL(t)
	defer cleanup()

	r1 := &Record{
		Op:    OpSet,
		Key:   []byte("force"),
		Value: []byte("now"),
	}

	if err := w.Append(r1); err != nil {
		t.Fatal(err)
	}

	// Force immediate flush
	w.ForceFlush()

	// Should be on disk immediately
	records, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record after ForceFlush, got %d", len(records))
	}
}

// Test that Close() flushes remaining data
func TestCloseFlushes(t *testing.T) {
	file, err := os.CreateTemp("", "walrus-wal-test-*")
	if err != nil {
		t.Fatal(err)
	}

	path := file.Name()
	file.Close()
	defer os.Remove(path)

	w, err := Open(path, 1*time.Second) // Long interval so it won't auto-flush
	if err != nil {
		t.Fatal(err)
	}

	r1 := &Record{
		Op:    OpSet,
		Key:   []byte("close"),
		Value: []byte("test"),
	}

	if err := w.Append(r1); err != nil {
		t.Fatal(err)
	}

	// Close should flush
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and verify data was written
	w2, err := Open(path, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	records, err := w2.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record after close/reopen, got %d", len(records))
	}

	if string(records[0].Key) != "close" {
		t.Fatal("record not persisted after close")
	}
}

// Test concurrent appends
func TestConcurrentAppends(t *testing.T) {
	w, cleanup := newTestWAL(t)
	defer cleanup()

	const numGoroutines = 10
	const recordsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Spawn multiple goroutines appending concurrently
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < recordsPerGoroutine; j++ {
				r := &Record{
					Op:    OpSet,
					Key:   []byte("concurrent"),
					Value: []byte("data"),
				}

				if err := w.Append(r); err != nil {
					t.Errorf("goroutine %d: append failed: %v", id, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()

	// Flush all buffered data
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Verify all records written
	records, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	expected := numGoroutines * recordsPerGoroutine
	if len(records) != expected {
		t.Fatalf("expected %d records, got %d", expected, len(records))
	}
}

// Test checksum validation
func TestChecksumValidation(t *testing.T) {
	w, cleanup := newTestWAL(t)
	defer cleanup()

	r1 := &Record{
		Op:    OpSet,
		Key:   []byte("valid"),
		Value: []byte("checksum"),
	}

	if err := w.Append(r1); err != nil {
		t.Fatal(err)
	}

	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the data by modifying a byte in the file
	w.mu.Lock()
	// Get file size
	stat, err := w.file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt last byte (in the data section)
	corruptByte := []byte{0xFF}
	_, err = w.file.WriteAt(corruptByte, stat.Size()-1)
	if err != nil {
		t.Fatal(err)
	}
	w.file.Sync()
	w.mu.Unlock()

	// Reading should detect corruption and truncate
	records, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	// Should have truncated the corrupted record
	if len(records) != 0 {
		t.Fatalf("expected 0 records after checksum failure, got %d", len(records))
	}
}

// Benchmark batched writes
func BenchmarkBatchedWrites(b *testing.B) {
	file, err := os.CreateTemp("", "walrus-bench-*")
	if err != nil {
		b.Fatal(err)
	}

	path := file.Name()
	file.Close()
	defer os.Remove(path)

	// Use long flush interval for batching
	w, err := Open(path, 100*time.Millisecond)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	r := &Record{
		Op:    OpSet,
		Key:   []byte("benchmark"),
		Value: []byte("value"),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Append(r)
		if i%100 == 0 {
			w.Flush() // Flush every 100 records
		}
	}

	w.Flush() // Final flush
}

// Benchmark immediate writes (no batching)
func BenchmarkImmediateWrites(b *testing.B) {
	file, err := os.CreateTemp("", "walrus-bench-*")
	if err != nil {
		b.Fatal(err)
	}

	path := file.Name()
	file.Close()
	defer os.Remove(path)

	w, err := Open(path, 100*time.Millisecond)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	r := &Record{
		Op:    OpSet,
		Key:   []byte("benchmark"),
		Value: []byte("value"),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Append(r)
		w.Flush() // Flush every record (no batching)
	}
}
