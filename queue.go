package main

import (
	"fmt"
	"io"
	"log"
	"log/slog"
	"sync"
)

type Message struct {
	Topic   string
	Payload []byte
}

type PeerToTopics struct {
	Peer   Peer
	Action string
	Topics []string
}

type Config struct {
	HTTPListenAddr      string
	WSListenAddr        string
	StorageProducerFunc StorageProducerFunc
}

type Queue struct {
	*Config
	mu           sync.RWMutex
	topics       map[string]Storage
	peers        map[Peer]map[string]struct{}
	consumers    []Consumer
	producers    []Producer
	producech    chan Message
	peerch       chan Peer
	peerToTopics chan PeerToTopics
	quitch       chan struct{}
}

func NewQueue(cfg *Config) (*Queue, error) {
	producech := make(chan Message)
	peerch := make(chan Peer)
	peerToTopics := make(chan PeerToTopics)

	return &Queue{
		Config: cfg,
		topics: make(map[string]Storage),
		producers: []Producer{
			NewHTTPProducer(cfg.HTTPListenAddr, producech),
		},
		producech:    producech,
		peerch:       peerch,
		peers:        make(map[Peer]map[string]struct{}),
		peerToTopics: peerToTopics,
		consumers: []Consumer{
			NewWSConsumer(cfg.WSListenAddr, peerch, peerToTopics),
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
			q.peers[peer] = make(map[string]struct{})
		case msg := <-q.producech:
			fmt.Println("produced -> ", msg)
			offset, err := q.publish(msg)
			if err != nil {
				log.Fatalf("Error occurred: %s", err)
			} else {
				fmt.Printf("Produced message in topic %s with offset %d", msg.Topic, offset)
			}
		case peerToTopics := <-q.peerToTopics:
			slog.Info("peer topics update", "peer", peerToTopics.Peer, "topics", peerToTopics.Topics)
			// update peer topics
			for _, topic := range peerToTopics.Topics {
				go q.handlePeerTopicsUpdate(peerToTopics.Peer, peerToTopics.Action, topic)
			}
		}

	}
}

func (q *Queue) handlePeerTopicsUpdate(peer Peer, action string, topic string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	switch action {
	case "subscribe":
		fmt.Println("Inside Handle Peer Topics Update", action, topic)
		if _, ok := q.topics[topic]; !ok {
			if err := peer.Send([]byte("topic not found")); err != nil {
				if err == io.EOF {
					delete(q.peers, peer)
				} else {
					log.Println(err)
				}
			}
		} else {
			q.peers[peer][topic] = struct{}{}
			slog.Info("Peer subscribed to topics", "peer", peer, "topics", topic, "peersTopics", q.peers[peer])
			if err := peer.Send([]byte("subscribed to " + topic)); err != nil {
				if err == io.EOF {
					delete(q.peers, peer)
				} else {
					log.Println(err)
				}
			}
		}
	case "unsubscribe":
		if _, ok := q.peers[peer][topic]; !ok {
			if err := peer.Send([]byte("not subscribed to " + topic)); err != nil {
				if err == io.EOF {
					delete(q.peers, peer)
				} else {
					log.Println(err)
				}
			}
		} else {
			delete(q.peers[peer], topic)
			slog.Info("Peer unsubscribed from topics", "peer", peer, "topics", topic, "peersTopics", q.peers[peer])
			if err := peer.Send([]byte("unsubscribed from " + topic)); err != nil {
				if err == io.EOF {
					delete(q.peers, peer)
				} else {
					log.Println(err)
				}
			}
		}
	}
}

func (q *Queue) publish(msg Message) (int, error) {
	q.getStoreForTopic(msg.Topic)
	return q.topics[msg.Topic].Push(msg.Payload)
}

func (q *Queue) getStoreForTopic(topic string) {
	if _, ok := q.topics[topic]; !ok {
		q.topics[topic] = q.StorageProducerFunc()
		slog.Info("Topis is created", "topic", topic)
	}
}
