package main

import (
	"log"
)

func main() {
	cfg := &Config{
		HTTPListenAddr: ":3000",
		WSListenAddr:   ":4000",
		StorageProducerFunc: func() Storage {
			return NewMemoryStorage()
		},
	}

	q, err := NewQueue(cfg)

	if err != nil {
		log.Fatal(err)
	}

	q.Start()
}
