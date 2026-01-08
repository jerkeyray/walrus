package wal

import (
	"os"
	"testing"
	"time"
)

// Benchmark different buffer sizes for WAL writes
func BenchmarkBufferSize1KB(b *testing.B)   { benchmarkBufferSize(b, 1024) }
func BenchmarkBufferSize2KB(b *testing.B)   { benchmarkBufferSize(b, 2*1024) }
func BenchmarkBufferSize4KB(b *testing.B)   { benchmarkBufferSize(b, 4*1024) }
func BenchmarkBufferSize8KB(b *testing.B)   { benchmarkBufferSize(b, 8*1024) }
func BenchmarkBufferSize16KB(b *testing.B)  { benchmarkBufferSize(b, 16*1024) }
func BenchmarkBufferSize32KB(b *testing.B)  { benchmarkBufferSize(b, 32*1024) }
func BenchmarkBufferSize64KB(b *testing.B)  { benchmarkBufferSize(b, 64*1024) }
func BenchmarkBufferSize128KB(b *testing.B) { benchmarkBufferSize(b, 128*1024) }

func benchmarkBufferSize(b *testing.B, bufferSize int) {
	dir, err := os.MkdirTemp("", "walrus-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, err := openWithBufferSize(dir, 100*time.Millisecond, 100*1024*1024, bufferSize)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	r := &Record{
		Op:    OpSet,
		Key:   []byte("benchmark-key"),
		Value: []byte("benchmark-value-with-some-data-to-make-it-realistic"),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := w.Append(r); err != nil {
			b.Fatal(err)
		}

		// Flush every 100 records to simulate batching
		if i%100 == 0 {
			w.Flush()
		}
	}

	w.Flush() // Final flush
}

// Helper to create WAL with custom buffer size
func openWithBufferSize(dir string, flushEvery time.Duration, maxSize int64, bufferSize int) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	w := &WAL{
		dir:        dir,
		buffer:     make([]byte, 0, bufferSize),
		segmentID:  1,
		maxSize:    maxSize,
		flushEvery: flushEvery,
		stopCh:     make(chan struct{}),
		stoppedCh:  make(chan struct{}),
	}

	if err := w.openSegment(); err != nil {
		return nil, err
	}

	go w.flushLoop()
	return w, nil
}

// Benchmark write throughput with different batch sizes
func BenchmarkBatchSize1(b *testing.B)    { benchmarkBatchSize(b, 1) }
func BenchmarkBatchSize10(b *testing.B)   { benchmarkBatchSize(b, 10) }
func BenchmarkBatchSize100(b *testing.B)  { benchmarkBatchSize(b, 100) }
func BenchmarkBatchSize1000(b *testing.B) { benchmarkBatchSize(b, 1000) }

func benchmarkBatchSize(b *testing.B, batchSize int) {
	dir, err := os.MkdirTemp("", "walrus-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, err := Open(dir, 1*time.Second, 100*1024*1024) // Long flush interval
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	r := &Record{
		Op:    OpSet,
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := w.Append(r); err != nil {
			b.Fatal(err)
		}

		if (i+1)%batchSize == 0 {
			w.Flush()
		}
	}

	w.Flush()
}

// Benchmark concurrent writes
func BenchmarkConcurrentWrites(b *testing.B) {
	dir, err := os.MkdirTemp("", "walrus-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, err := Open(dir, 100*time.Millisecond, 100*1024*1024)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	r := &Record{
		Op:    OpSet,
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := w.Append(r); err != nil {
				b.Fatal(err)
			}
		}
	})

	w.Flush()
}

// Benchmark record encoding
func BenchmarkRecordEncode(b *testing.B) {
	r := &Record{
		Op:    OpSet,
		Key:   []byte("benchmark-key"),
		Value: []byte("benchmark-value-with-some-data"),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := encodeRecord(r)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark record decoding
func BenchmarkRecordDecode(b *testing.B) {
	r := &Record{
		Op:    OpSet,
		Key:   []byte("benchmark-key"),
		Value: []byte("benchmark-value-with-some-data"),
	}

	data, err := encodeRecord(r)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := decodeRecord(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}
