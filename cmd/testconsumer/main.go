package main

import (
	"log"
	"log/slog"

	"github.com/gorilla/websocket"
)

type WSMessage struct {
	Action string   `json:"action"`
	Topics []string `json:"topics"`
}

func main() {
	conn, _, err := websocket.DefaultDialer.Dial("ws://localhost:4000", nil)
	if err != nil {
		log.Fatal(err)
	}

	msg := WSMessage{
		Action: "subscribe",
		Topics: []string{"topic_1", "topic_2", "foobar"},
	}

	// b, err := json.Marshal(msg)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	if err := conn.WriteJSON(msg); err != nil {
		log.Fatal(err)
	}

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Fatal(err)
		}

		slog.Info("received message", "message", string(msg))
	}
}
