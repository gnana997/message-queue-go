package main

import (
	"fmt"
	"log"
	"log/slog"
)

type Message struct {
	Topic   string
	Payload []byte
}

type Config struct {
	ListenAddr          string
	StorageProducerFunc StorageProducerFunc
}

type Queue struct {
	*Config

	topics map[string]Storage

	consumers []Consumer
	producers []Producer
	producech chan Message
	quitch    chan struct{}
}

func NewQueue(cfg *Config) (*Queue, error) {
	producech := make(chan Message)
	return &Queue{
		Config: cfg,
		topics: make(map[string]Storage),
		producers: []Producer{
			NewHTTPProducer(cfg.ListenAddr, producech),
		},
		producech: producech,
		quitch:    make(chan struct{}),
	}, nil
}

func (q *Queue) Start() {
	// for _, consumer := range q.consumers {
	// 	if err := consumer.Start(); err != nil {
	// 		fmt.Println(err)
	// 	}
	// }

	for _, producer := range q.producers {
		go func(p Producer) {
			if err := p.Start(); err != nil {
				fmt.Println(err)
			}
		}(producer)
	}
	q.loop()
	//http.ListenAndServe(q.Config.ListenAddr, q)
}

func (q *Queue) loop() {
	for {
		select {
		case <-q.quitch:
			return
		case msg := <-q.producech:
			fmt.Println("produced -> ", msg)
			offset, err := q.publish(msg)
			if err != nil {
				log.Fatalf("Error occurred: %s", err)
			} else {
				fmt.Printf("Produced message in topic %s with offset %d", msg.Topic, offset)
			}
		}
	}
}

func (q *Queue) publish(msg Message) (int, error) {
	store := q.getStoreForTopic(msg.Topic)
	return store.Push(msg.Payload)
}

func (q *Queue) getStoreForTopic(topic string) Storage {
	if _, ok := q.topics[topic]; !ok {
		q.topics[topic] = q.StorageProducerFunc()
		slog.Info("Topis is created", "topic", topic)
	}
	return q.topics[topic]
}
