package wal

import (
	"encoding/binary"
	"os"
	"sync"
)

type WAL struct {
	mu   sync.Mutex
	file *os.File // log file
}

func Open(path string) (*WAL, error) {
	// if file doesn't exist create it
	// make it append only
	// allow reads and writes
	// file mode 0644: owner - read/write, others - read
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{file: f}, nil
}

func (w *WAL) Append(r *Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := encodeRecord(r)
	if err != nil {
		return err
	}

	length := uint32(len(data))

	// write length
	if err := writeUint32(w.file, length); err != nil {
		return err
	}

	// write data
	if _, err := w.file.Write(data); err != nil {
		return err
	}

	// fsync
	return w.file.Sync()
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
		length, err := readUint32At(w.file, offset)
		if err != nil {
			break
		}

		offset += 4

		data := make([]byte, length)
		n, err := w.file.ReadAt(data, offset)
		if err != nil || n != int(length) {
			// partial write or corruption
			// truncate file to last good offset
			w.file.Truncate(offset - 4)
			break
		}

		offset += int64(length)

		rec, err := decodeRecord(data)
		if err != nil {
			// corrupt record -> truncate to before this record
			w.file.Truncate(offset - int64(length) - 4)
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
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	// final fsync just to be safe
	if err := w.file.Sync(); err != nil {
		return err
	}

	err := w.file.Close()
	w.file = nil
	return err
}
