package main

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{}

type Consumer interface {
	Start() error
}

type WSConsumer struct {
	ListenAddr string
	peerch     chan<- Peer
}

func NewWSConsumer(listenAddr string, peerch chan Peer) *WSConsumer {
	return &WSConsumer{
		ListenAddr: listenAddr,
		peerch:     peerch,
	}
}

func (wc *WSConsumer) Start() error {
	slog.Info("websocket consumer started", "port", wc.ListenAddr)
	return http.ListenAndServe(wc.ListenAddr, wc)
}

func (wc *WSConsumer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	wc.peerch <- NewWSPeer(conn)
}

type WSMessage struct {
	Action string   `json:"action"`
	Topics []string `json:"topics"`
}
