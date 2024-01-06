package main

import (
	"fmt"
	"log/slog"

	"github.com/gorilla/websocket"
)

type Peer interface {
	Send([]byte) error
}

type WSPeer struct {
	conn *websocket.Conn
}

func NewWSPeer(conn *websocket.Conn) *WSPeer {
	p := &WSPeer{
		conn: conn,
	}

	go p.readLoop()

	return p
}

func (p *WSPeer) readLoop() {
	var msg WSMessage
	for {
		if err := p.conn.ReadJSON(&msg); err != nil {
			slog.Error("ws peer read error", "err", err)
			return
		}
		if err := p.handleMessage(msg); err != nil {
			slog.Error("ws peer handle msg error", "err", err)
			return
		}
	}
}

func (p *WSPeer) handleMessage(msg WSMessage) error {
	// validation of message
	fmt.Printf("handling message %+v \n", msg)
	return nil
}

func (p *WSPeer) Send(b []byte) error {
	return p.conn.WriteMessage(websocket.BinaryMessage, b)
}
