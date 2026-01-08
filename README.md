```
 ___       __   ________  ___       ________  ___  ___  ________
|\  \     |\  \|\   __  \|\  \     |\   __  \|\  \|\  \|\   ____\
\ \  \    \ \  \ \  \|\  \ \  \    \ \  \|\  \ \  \\\  \ \  \___|_
 \ \  \  __\ \  \ \   __  \ \  \    \ \   _  _\ \  \\\  \ \_____  \
  \ \  \|\__\_\  \ \  \ \  \ \  \____\ \  \\  \\ \  \\\  \|____|\  \
   \ \____________\ \__\ \__\ \_______\ \__\\ _\\ \_______\____\_\  \
    \|____________|\|__|\|__|\|_______|\|__|\|__|\|_______|\_________\
                                                          \|_________|
```

# Walrus

A persistent key-value store with Write-Ahead Logging (WAL) for learning database internals.

## Features

- Write-ahead logging with automatic segment rotation
- Crash recovery by replaying WAL
- CRC32 checksums for corruption detection
- Buffered writes with background flushing
- Thread-safe concurrent access
- Interactive CLI with command history and tab completion

## Quick Start

```bash
# Build
go build -o walrus ./cmd/main.go

# Run
./walrus
```

## CLI Usage

```
SET <key> <value>     Store a key-value pair
GET <key>             Retrieve value
DELETE <key>          Remove a key
HAS <key>             Check if key exists
KEYS                  List all keys
LEN                   Show number of keys
COMMIT                Flush pending writes
EXIT                  Exit
```

## Example

```bash
walrus> SET name jerk
OK (set 'name' = 'jerk')

walrus> GET name
jerk

walrus> KEYS
Keys (1 total):
  1. name

walrus> EXIT
Goodbye!
```

## Programmatic Usage

```go
package main

import (
    "log"
    "time"
    "github.com/jerkeyray/walrus/store"
    "github.com/jerkeyray/walrus/wal"
)

func main() {
    // Open WAL (dir, flush interval, max segment size)
    w, _ := wal.Open("./data", 100*time.Millisecond, 10*1024*1024)
    defer w.Close()

    s := store.New(w)
    s.Recover() // Recover from disk

    s.Set("user", "alice")
    value, _ := s.Get("user")
    s.Commit() // Flush to disk
}
```

## Architecture

### WAL Record Format

```
[Magic: 4B][Length: 4B][Checksum: 4B][Data: NB]
```

### Data Format

```
[Op: 1B][KeyLen: 4B][ValLen: 4B][Key][Value]
```

Operations: `OpSet` (1), `OpDelete` (2)

### Directory Structure

```
walrus-data/
  wal-0001.log
  wal-0002.log  # Created when first segment reaches max size
  wal-0003.log
  ...
```

## Testing

```bash
# Run all tests
go test ./...

# Run with race detector
go test -race ./...

# Run benchmarks
go test -bench=. -benchmem ./...
```

## Benchmark Results

Buffer size (4KB default is optimal):
```
BenchmarkBufferSize4KB     25402    47558 ns/op    83 B/op    1 allocs/op
BenchmarkBufferSize16KB    25326    47164 ns/op    82 B/op    1 allocs/op
```

Batch size (batching = massive speedup):
```
BenchmarkBatchSize1          360    3721155 ns/op   # Single ops are slow
BenchmarkBatchSize100      26442      42651 ns/op   # 100x faster with batching
BenchmarkBatchSize1000    258242       4542 ns/op   # 800x faster
```

## How It Works

1. Every write is first appended to the WAL
2. Background goroutine flushes buffer every N milliseconds
3. On crash, replay all WAL segments to rebuild state
4. CRC32 checksums detect corrupted records
5. Segments auto-rotate when reaching max size

## Project Structure

```
walrus/
├── cmd/main.go          # CLI application
├── wal/
│   ├── wal.go           # WAL implementation
│   ├── record.go        # Record encoding/decoding
│   └── wal_test.go      # Tests & benchmarks
└── store/
    ├── store.go         # Key-value store
    └── store_test.go    # Tests
```

## License

MIT
