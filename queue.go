package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"sync"
)

type MessageToTopic struct {
	Topic   string
	Payload []byte
}

type MessageToPeer struct {
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
	Offset  int    `json:"offset"`
}

type PeerTopicsAction struct {
	Peer   Peer
	Action string
	Topics []string
}

type Config struct {
	HTTPListenAddr      string
	WSListenAddr        string
	StorageProducerFunc StorageProducerFunc
}

type PeerTopicDetails struct {
	Offset int
	Topic  string
}

type Queue struct {
	*Config
	mu               sync.RWMutex
	topics           map[string]Storage
	peers            map[Peer]bool
	consumers        []Consumer
	producers        []Producer
	peerToTopics     map[Peer][]*PeerTopicDetails
	producech        chan MessageToTopic
	peerch           chan Peer
	peerTopicsAction chan PeerTopicsAction
	quitch           chan struct{}
}

func NewQueue(cfg *Config) (*Queue, error) {
	producech := make(chan MessageToTopic)
	peerch := make(chan Peer)
	peerTopicsAction := make(chan PeerTopicsAction)

	return &Queue{
		Config: cfg,
		topics: make(map[string]Storage),
		producers: []Producer{
			NewHTTPProducer(cfg.HTTPListenAddr, producech),
		},
		consumers: []Consumer{
			NewWSConsumer(cfg.WSListenAddr, peerch, peerTopicsAction),
		},
		peers:            make(map[Peer]bool),
		peerToTopics:     make(map[Peer][]*PeerTopicDetails),
		producech:        producech,
		peerch:           peerch,
		peerTopicsAction: peerTopicsAction,
		quitch:           make(chan struct{}),
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
		case peerTopicsAction := <-q.peerTopicsAction:
			slog.Info("peer topics update", "peer", peerTopicsAction.Peer, "topics", peerTopicsAction.Topics)
			// update peer topics
			q.handlePeerTopicsUpdate(peerTopicsAction.Peer, peerTopicsAction.Action, peerTopicsAction.Topics)
		}

	}
}

func (q *Queue) handlePeerTopicsUpdate(peer Peer, action string, topics []string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	switch action {
	case "subscribe":
		fmt.Println("Inside Handle Peer Topics Update", action, topics)
		for _, topic := range topics {
			if _, ok := q.topics[topic]; !ok {
				msg, err := json.Marshal(MessageToPeer{
					Topic:   topic,
					Offset:  0,
					Payload: []byte("topic not found"),
				})
				if err != nil {
					slog.Error(fmt.Sprintf("Error marshalling message to peer %s", err))
				}
				if err := peer.Send(msg); err != nil {
					if err == io.EOF {
						delete(q.peers, peer)
						delete(q.peerToTopics, peer)
					} else {
						log.Println(err)
					}
				}
			} else {
				q.peerToTopics[peer] = append(q.peerToTopics[peer], &PeerTopicDetails{
					Offset: 0,
					Topic:  topic,
				})
				slog.Info("Peer subscribed to topics", "peer", peer, "topics", topic, "peerTopicdetails", q.peerToTopics[peer][len(q.peerToTopics[peer])-1])
				msg, err := json.Marshal(MessageToPeer{
					Topic:   topic,
					Offset:  0,
					Payload: []byte("subscribed to " + topic),
				})
				if err != nil {
					slog.Error(fmt.Sprintf("Error marshalling message to peer %s", err))
				}
				if err := peer.Send(msg); err != nil {
					if err == io.EOF {
						delete(q.peers, peer)
						delete(q.peerToTopics, peer)
					} else {
						log.Println(err)
					}
				}
			}
		}
	case "unsubscribe":
		for _, topic := range topics {
			if _, ok := q.topics[topic]; !ok {
				msg, err := json.Marshal(MessageToPeer{
					Topic:   topic,
					Offset:  0,
					Payload: []byte("topic not found"),
				})
				if err != nil {
					slog.Error(fmt.Sprintf("Error marshalling message to peer %s", err))
				}
				if err := peer.Send(msg); err != nil {
					if err == io.EOF {
						delete(q.peers, peer)
						delete(q.peerToTopics, peer)
					} else {
						log.Println(err)
					}
				}
			} else {
				for idx, topicDetails := range q.peerToTopics[peer] {
					if topicDetails.Topic == topic {
						q.peerToTopics[peer] = append(q.peerToTopics[peer][:idx], q.peerToTopics[peer][idx+1:]...)
						slog.Info("Peer unsubscribed from topics", "peer", peer, "topics", topic, "peersTopics", q.peerToTopics[peer])
						msg, err := json.Marshal(MessageToPeer{
							Topic:   topic,
							Offset:  0,
							Payload: []byte("unsubscribed from " + topic),
						})
						if err != nil {
							slog.Error(fmt.Sprintf("Error marshalling message to peer %s", err))
						}
						if err := peer.Send(msg); err != nil {
							if err == io.EOF {
								delete(q.peers, peer)
								delete(q.peerToTopics, peer)
							} else {
								log.Println(err)
							}
						}
						break
					}
					if idx == len(q.peerToTopics[peer])-1 {
						slog.Info("peer is not subscribed to topic anymore", "peer", peer, "topic", topic)
					}
				}
			}
		}
	}

	if len(q.peerToTopics[peer]) == 0 {
		return
	} else {
		go q.handlePeer(peer, q.peerToTopics[peer])
	}
}

// Need To rewrite this section to asynchronously write messages to peers
func (q *Queue) handlePeer(peer Peer, peerTopicDetails []*PeerTopicDetails) {
	for {
		select {
		case <-q.quitch:
			return
		case <-peer.GetPeerSubscription():
			return
		default:
			// Read message from storage
			if err := q.writeToPeer(peer, peerTopicDetails); err != nil {
				slog.Error("error writing message to peer", "peer", peer, "error", err)
				if err.Error() == "peer has caught up with latest offset for all topics" {
					continue
				}
				return
			}
		}
	}
}

func (q *Queue) writeToPeer(peer Peer, peerTopicDetails []*PeerTopicDetails) error {
	for {
		select {
		case <-peer.GetPeerSubscription():
			return fmt.Errorf("peer subscriptions updated")
		default:
			// Check if peer has caught up with latest offset of all topics
			for idx, peerTopicDetail := range peerTopicDetails {
				if peerTopicDetail.Offset <= q.topics[peerTopicDetail.Topic].Size() {
					break
				}
				if idx == len(peerTopicDetails)-1 {
					if peerTopicDetail.Offset >= q.topics[peerTopicDetail.Topic].Size() {
						return fmt.Errorf("peer has caught up with latest offset for all topics")
					}
				}
			}
			// Read message from storage
			for _, peerTopicDetail := range peerTopicDetails {
				payload, err := q.topics[peerTopicDetail.Topic].Fetch(peerTopicDetail.Offset)
				if err != nil {
					continue
				}

				msg, err := json.Marshal(MessageToPeer{
					Topic:   peerTopicDetail.Topic,
					Payload: payload,
					Offset:  peerTopicDetail.Offset,
				})
				if err != nil {
					log.Println("Error marshaling message:", err)
					continue
				}

				peerTopicDetail.Offset++

				if err := peer.Send(msg); err != nil {
					if err == io.EOF {
						delete(q.peers, peer)
						delete(q.peerToTopics, peer)
						return fmt.Errorf("peer got disconnected")
					} else {
						log.Println(err)
						return fmt.Errorf("peer probably got disconnected")
					}
				}
			}
		}
	}
}

func (q *Queue) publish(msg MessageToTopic) (int, error) {
	q.getStoreForTopic(msg.Topic)
	return q.topics[msg.Topic].Push(msg.Payload)
}

func (q *Queue) getStoreForTopic(topic string) {
	if _, ok := q.topics[topic]; !ok {
		q.topics[topic] = q.StorageProducerFunc()
		slog.Info("Topis is created", "topic", topic)
	}
}
