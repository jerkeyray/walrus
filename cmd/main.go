package main

import (
	"fmt"
	"log"

	"github.com/jerkeyray/walrus/store"
	"github.com/jerkeyray/walrus/wal"
)

func main() {
	w, err := wal.Open("walrus.log")
	if err != nil {
		log.Fatal(err)
	}

	s := store.New(w)

	if err := s.Recover(); err != nil {
		log.Fatal(err)
	}

	// test writes
	s.Set("name", "jerk")
	s.Set("Hello", "World!")
	s.Set("coffee", "loveeee")

	fmt.Println("DONE")
}
