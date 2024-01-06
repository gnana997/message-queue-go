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
	HTTPListenAddr      string
	WSListenAddr        string
	StorageProducerFunc StorageProducerFunc
}

type Queue struct {
	*Config

	topics    map[string]Storage
	peers     map[Peer]bool
	consumers []Consumer
	producers []Producer
	producech chan Message
	peerch    chan Peer
	quitch    chan struct{}
}

func NewQueue(cfg *Config) (*Queue, error) {
	producech := make(chan Message)
	peerch := make(chan Peer)
	return &Queue{
		Config: cfg,
		topics: make(map[string]Storage),
		producers: []Producer{
			NewHTTPProducer(cfg.HTTPListenAddr, producech),
		},
		producech: producech,
		peerch:    peerch,
		peers:     make(map[Peer]bool),
		consumers: []Consumer{
			NewWSConsumer(cfg.WSListenAddr, peerch),
		},
		quitch: make(chan struct{}),
	}, nil
}

func (q *Queue) Start() {

	for _, consumer := range q.consumers {
		go func(c Consumer) {
			if err := c.Start(); err != nil {
				fmt.Println(err)
			}
		}(consumer)
	}

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
		case peer := <-q.peerch:
			slog.Info("added new connection", "peer", peer)
			q.peers[peer] = true
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
