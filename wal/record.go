package wal

import (
	"encoding/binary"
	"fmt"
)

type OpType byte // operation type

const (
	OpSet    OpType = 1
	OpDelete OpType = 2
)

// log entry struct
type Record struct {
	Op    OpType
	Key   []byte
	Value []byte
}

func encodeRecord(r *Record) ([]byte, error) {
	keyLen := uint32(len(r.Key))
	valLen := uint32(len(r.Value))

	totalSize := 1 + 4 + 4 + int(keyLen) + int(valLen)

	buf := make([]byte, totalSize)

	offset := 0
	buf[offset] = byte(r.Op)
	offset += 1

	binary.BigEndian.PutUint32(buf[offset:offset+4], keyLen)
	offset += 4

	binary.BigEndian.PutUint32(buf[offset:offset+4], valLen)
	offset += 4

	copy(buf[offset:offset+int(keyLen)], r.Key)
	offset += int(keyLen)

	copy(buf[offset:offset+int(valLen)], r.Value)
	offset += int(valLen)

	return buf, nil
}

func decodeRecord(data []byte) (*Record, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf("data is too short to be a record.")
	}

	offset := 0
	op := OpType(data[offset])
	offset += 1

	keyLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	valLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	expected := int(keyLen + valLen)
	if len(data[offset:]) != expected {
		return nil, fmt.Errorf("invalid record length")
	}

	key := make([]byte, keyLen)
	copy(key, data[offset:offset+int(keyLen)])
	offset = int(keyLen)

	value := make([]byte, valLen)
	copy(value, data[offset:offset+int(valLen)])
	offset += int(valLen)

	rec := &Record{
		Op:    op,
		Key:   key,
		Value: value,
	}

	return rec, nil
}
