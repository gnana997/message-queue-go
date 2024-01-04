package main

import (
	"fmt"
	"net/http"
)

type Config struct {
	ListenAddr          string
	StorageProducerFunc StorageProducerFunc
}

type Queue struct {
	*Config
	topics map[string]Storage
}

func NewQueue(cfg *Config) (*Queue, error) {
	return &Queue{
		Config: cfg,
		topics: make(map[string]Storage),
	}, nil
}

func (q *Queue) Start() {
	http.ListenAndServe(q.Config.ListenAddr, q)
}

func (q *Queue) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r.URL.Path)
}

func (q *Queue) createTopic(topic string) bool {
	if _, ok := q.topics[topic]; !ok {
		q.topics[topic] = q.StorageProducerFunc()
		return true
	}
	return false
}
