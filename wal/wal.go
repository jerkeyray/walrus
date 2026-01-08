package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type WAL struct {
	mu     sync.Mutex
	dir    string
	file   *os.File // log file
	buffer []byte   // for batching

	segmentID int
	maxSize   int64

	flushEvery time.Duration
	stopCh     chan struct{}
	stoppedCh  chan struct{}

	closed bool
}

func Open(dir string, flushEvery time.Duration, maxSize int64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	w := &WAL{
		dir:        dir,
		buffer:     make([]byte, 0, 4096),
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

func (w *WAL) Append(r *Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("wal is closed")
	}
	data, err := encodeRecord(r)
	if err != nil {
		return err
	}

	length := uint32(len(data))
	checksum := crc32.ChecksumIEEE(data)

	var header [12]byte

	binary.BigEndian.PutUint32(header[0:4], recordMagic)
	binary.BigEndian.PutUint32(header[4:8], length)
	binary.BigEndian.PutUint32(header[8:12], checksum)

	w.buffer = append(w.buffer, header[:]...)
	w.buffer = append(w.buffer, data...)

	return nil
}

func writeUint32(f *os.File, v uint32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)

	_, err := f.Write(buf[:])
	return err
}

func (w *WAL) ReadAll() ([]*Record, error) {
	files, err := w.segmentFiles()
	if err != nil {
		return nil, err
	}

	var records []*Record

	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}

		recs, err := readAllFromFile(f)
		f.Close()

		if err != nil {
			return nil, err
		}

		records = append(records, recs...)
	}

	return records, nil
}

func readAllFromFile(f *os.File) ([]*Record, error) {
	var records []*Record
	var offset int64 = 0

	for {
		// read magic
		magic, err := readUint32At(f, offset)
		if err != nil {
			break
		}

		if magic != recordMagic {
			// garbage or corruption
			f.Truncate(offset)
			break
		}

		offset += 4

		// read length
		length, err := readUint32At(f, offset)
		if err != nil {
			f.Truncate(offset - 4)
			break
		}

		offset += 4

		// read checksum
		expectedChecksum, err := readUint32At(f, offset)
		if err != nil {
			f.Truncate(offset - 8)
			break
		}

		offset += 4

		// read data
		data := make([]byte, length)
		n, err := f.ReadAt(data, offset)
		if err != nil || n != int(length) {
			// partial write or corruption
			// truncate file to last good offset
			f.Truncate(offset - 12)
			break
		}

		offset += int64(length)

		// verify checksum
		actualChecksum := crc32.ChecksumIEEE(data)
		if actualChecksum != expectedChecksum {
			f.Truncate(offset - int64(length) - 12)
			break
		}

		rec, err := decodeRecord(data)
		if err != nil {
			// corrupt record -> truncate to before this record
			f.Truncate(offset - int64(length) - 12)
			break
		}

		records = append(records, rec)
	}

	return records, nil
}

func readUint32At(f *os.File, offset int64) (uint32, error) {
	var buf [4]byte
	_, err := f.ReadAt(buf[:], offset)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(buf[:]), nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.mu.Unlock()

	close(w.stopCh)
	<-w.stoppedCh

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		err := w.file.Close()
		w.file = nil
		return err
	}

	return nil
}

func (w *WAL) Flush() {
	w.flushOnce()
}

// flush every n ms -> on stop, flush and exit
func (w *WAL) flushLoop() {
	ticker := time.NewTicker(w.flushEvery)
	defer ticker.Stop()
	defer close(w.stoppedCh)

	for {
		select {
		case <-ticker.C:
			w.flushOnce()

		case <-w.stopCh:
			w.flushOnce()
			return
		}
	}
}

func (w *WAL) flushOnce() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		panic("flushOnce called with nil file")
	}

	if len(w.buffer) == 0 {
		return
	}

	info, err := w.file.Stat()
	if err != nil {
		panic(err)
	}
	if info.Size()+int64(len(w.buffer)) > w.maxSize {
		if err := w.file.Sync(); err != nil {
			panic(err)
		}
		if err := w.file.Close(); err != nil {
			panic(err)
		}
		w.file = nil

		w.segmentID++
		if err := w.openSegment(); err != nil {
			panic(err)
		}
	}

	if _, err := w.file.Write(w.buffer); err != nil {
		panic(err) // panic cause this shit is not recoverable
	}

	if err := w.file.Sync(); err != nil {
		panic(err)
	}

	w.buffer = w.buffer[:0]
}

func (w *WAL) ForceFlush() {
	w.flushOnce()
}

func (w *WAL) openSegment() error {
	path := filepath.Join(w.dir, fmt.Sprintf("wal-%04d.log", w.segmentID))

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	w.file = f
	return nil
}

func (w *WAL) segmentFiles() ([]string, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "wal-") {
			files = append(files, filepath.Join(w.dir, e.Name()))
		}
	}

	sort.Strings(files)
	return files, nil
}
