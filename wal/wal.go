package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"sync"
	"time"
)

type WAL struct {
	mu     sync.Mutex
	file   *os.File // log file
	buffer []byte   // for batching

	flushEvery time.Duration
	stopCh     chan struct{}
	stoppedCh  chan struct{}
}

func Open(path string, flushEvery time.Duration) (*WAL, error) {
	// if file doesn't exist create it
	// make it append only
	// allow reads and writes
	// file mode 0644: owner - read/write, others - read
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		file:       f,
		buffer:     make([]byte, 0, 4096),
		flushEvery: flushEvery,
		stopCh:     make(chan struct{}),
		stoppedCh:  make(chan struct{}),
	}

	go w.flushLoop()

	return w, nil
}

func (w *WAL) Append(r *Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

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
	w.mu.Lock()
	defer w.mu.Unlock()

	var records []*Record
	var offset int64 = 0

	for {
		// read magic
		magic, err := readUint32At(w.file, offset)
		if err != nil {
			break
		}

		if magic != recordMagic {
			// garbage or corruption
			w.file.Truncate(offset)
			break
		}

		offset += 4

		// read length
		length, err := readUint32At(w.file, offset)
		if err != nil {
			w.file.Truncate(offset - 4)
			break
		}

		offset += 4

		// read checksum
		expectedChecksum, err := readUint32At(w.file, offset)
		if err != nil {
			w.file.Truncate(offset - 8)
			break
		}

		offset += 4

		// read data
		data := make([]byte, length)
		n, err := w.file.ReadAt(data, offset)
		if err != nil || n != int(length) {
			// partial write or corruption
			// truncate file to last good offset
			w.file.Truncate(offset - 12)
			break
		}

		offset += int64(length)

		// verify checksum
		actualChecksum := crc32.ChecksumIEEE(data)
		if actualChecksum != expectedChecksum {
			w.file.Truncate(offset - int64(length) - 12)
			break
		}

		rec, err := decodeRecord(data)
		if err != nil {
			// corrupt record -> truncate to before this record
			w.file.Truncate(offset - int64(length) - 12)
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
	close(w.stopCh)
	<-w.stoppedCh

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	err := w.file.Close()
	w.file = nil
	return err
}

func (w *WAL) Path() string {
	if w.file == nil {
		return ""
	}
	return w.file.Name()
}

func (w *WAL) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.buffer) == 0 {
		return nil
	}

	if _, err := w.file.Write(w.buffer); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	// clear buffer
	w.buffer = w.buffer[:0]
	return nil
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

	if len(w.buffer) == 0 {
		return
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
