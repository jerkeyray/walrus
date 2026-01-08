package main

import (
	"fmt"
	"log"
	"time"

	"github.com/jerkeyray/walrus/store"
	"github.com/jerkeyray/walrus/wal"
)

func main() {
	// Open WAL with 100ms flush interval and 10MB max segment size
	w, err := wal.Open("./walrus-data", 100*time.Millisecond, 10*1024*1024)
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	s := store.New(w)

	if err := s.Recover(); err != nil {
		log.Fatal(err)
	}

	// test writes
	if err := s.Set("name", "jerk"); err != nil {
		log.Fatal(err)
	}

	if err := s.Set("Hello", "World!"); err != nil {
		log.Fatal(err)
	}

	if err := s.Set("coffee", "loveeee"); err != nil {
		log.Fatal(err)
	}

	// commit to ensure data is flushed
	s.Commit()

	fmt.Println("DONE")
}
