package wal

import "encoding/binary"

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

	copy(buf[offset:offset+int(keyLen)], r.Value)
	offset += int(valLen)

	return buf, nil
}
