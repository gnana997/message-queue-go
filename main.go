package main

import (
	"log"
)

func main() {
	cfg := &Config{
		ListenAddr: ":3000",
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
