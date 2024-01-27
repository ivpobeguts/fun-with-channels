package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/gorilla/websocket"
)

type TradeData struct {
	Price     float64 `json:"p"`
	Timestamp int64   `json:"t"`
	Symbol    string  `json:"s"`
}

type APIResponse struct {
	Data []TradeData `json:"data"`
	Type string      `json:"type"`
}

type ThreadSafeCryptoBatch struct {
	slice      []TradeData
	mu         sync.Mutex
	windowSize int
	sum        float64
}

func (tss *ThreadSafeCryptoBatch) InsertSorted(item TradeData) {
	// Binary search to find the correct position based on timestamp
	index := sort.Search(len(tss.slice), func(i int) bool {
		return tss.slice[i].Timestamp >= item.Timestamp
	})

	// Insert the item at the correct position
	tss.slice = append(tss.slice[:index], append([]TradeData{item}, tss.slice[index:]...)...)
}

func main() {
	apiKey := "cmpois1r01qg7bbococ0cmpois1r01qg7bbococg"
	windowSize := 60

	btcChan := make(chan TradeData)
	ethChan := make(chan TradeData)
	ltcChan := make(chan TradeData)
	printerChan := make(chan map[string]float64)
	var wg sync.WaitGroup
	btcWindowBatch := &ThreadSafeCryptoBatch{windowSize: windowSize, slice: make([]TradeData, 0), sum: 0}
	ethWindowBatch := &ThreadSafeCryptoBatch{windowSize: windowSize, slice: make([]TradeData, 0), sum: 0}
	ltcWindowBatch := &ThreadSafeCryptoBatch{windowSize: windowSize, slice: make([]TradeData, 0), sum: 0}

	w, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf("wss://ws.finnhub.io?token=%s", apiKey), nil)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	symbols := []string{"BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "BINANCE:LTCUSDT"}
	for _, s := range symbols {
		msg, _ := json.Marshal(map[string]interface{}{"type": "subscribe", "symbol": s})
		w.WriteMessage(websocket.TextMessage, msg)
	}

	log.Println("Wating for first {1} values to calculate the first window", windowSize)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// Unmarshall message
			_, message, err := w.ReadMessage()
			if err != nil {
				log.Fatal("Error reading message from WebSocket:", err)
			}
			var apiResponse APIResponse
			err = json.Unmarshal(message, &apiResponse)
			if err != nil {
				log.Println("Error unmarshalling JSON:", err)
				continue
			}

			if len(apiResponse.Data) == 0 {
				log.Println("Empty trade data")
				continue
			}

			// Send to channel
			if apiResponse.Type == "trade" {
				for _, data := range apiResponse.Data {
					switch data.Symbol {
					case "BINANCE:BTCUSDT":
						btcChan <- data
					case "BINANCE:ETHUSDT":
						ethChan <- data
					case "BINANCE:LTCUSDT":
						ltcChan <- data
					}
				}
			}
		}
	}()

	// Goroutine for batch processing
	wg.Add(1)
	go processCryptoData("BTC", btcChan, printerChan, btcWindowBatch, &wg)

	wg.Add(1)
	go processCryptoData("ETH", ethChan, printerChan, ethWindowBatch, &wg)

	wg.Add(1)
	go processCryptoData("LTC", ltcChan, printerChan, ltcWindowBatch, &wg)

	// Goroutine for printing
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// Receive transformed data from the printer channel and print
			fmt.Println(<-printerChan)
		}
	}()

	// Wait for all goroutines to finish
	wg.Wait()
}

func processCryptoData(symbol string, inputChan <-chan TradeData, printerChan chan<- map[string]float64, batch *ThreadSafeCryptoBatch, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		apiData := <-inputChan
		batch.mu.Lock()
		batch.InsertSorted(apiData)
		batch.sum += apiData.Price
		if len(batch.slice) == batch.windowSize {
			average := batch.sum / float64(batch.windowSize)
			batch.slice = batch.slice[1:]
			batch.sum -= batch.slice[0].Price

			printerChan <- map[string]float64{symbol: average}
		}
		batch.mu.Unlock()
	}
}
