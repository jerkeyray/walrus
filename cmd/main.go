package main

import (
	"fmt"
	"log"
	"time"

	"github.com/jerkeyray/walrus/store"
	"github.com/jerkeyray/walrus/wal"
)

func main() {
	// Open WAL with 100ms flush interval for good performance
	w, err := wal.Open("walrus.log", 100*time.Millisecond)
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

	// Commit to ensure data is flushed
	if err := s.Commit(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("DONE")
}
