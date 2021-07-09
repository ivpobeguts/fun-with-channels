package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/gorilla/websocket"
)

func main() {
	apiKey := ""

	w, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf("wss://ws.finnhub.io?token=%s", apiKey), nil)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	symbols := []string{"BINANCE:BTCUSDT"} // "BINANCE:ETHUSDT", "BINANCE:ADAUSDT"}
	for _, s := range symbols {
		msg, _ := json.Marshal(map[string]interface{}{"type": "subscribe", "symbol": s})

		err = w.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			panic(err)
		}
	}

	var msg interface{}

	for {
		err := w.ReadJSON(&msg)
		if err != nil {
			panic(err)
		}

		log.Printf("Message from server: %+v\n", msg)
	}
}
