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
}

type APIResponse struct {
	Data []TradeData `json:"data"`
	Type string      `json:"type"`
}

type ThreadSafeSortedSlice struct {
	slice []TradeData
	mu    sync.Mutex
}

func (tss *ThreadSafeSortedSlice) InsertSorted(item TradeData) {
	// Binary search to find the correct position based on timestamp
	index := sort.Search(len(tss.slice), func(i int) bool {
		return tss.slice[i].Timestamp >= item.Timestamp
	})

	// Insert the item at the correct position
	tss.slice = append(tss.slice[:index], append([]TradeData{item}, tss.slice[index:]...)...)
}

func main() {
	apiKey := "cmpois1r01qg7bbococ0cmpois1r01qg7bbococg"

	apiDataChan := make(chan TradeData)
	printerChan := make(chan float64)
	var wg sync.WaitGroup
	windowBatch := &ThreadSafeSortedSlice{}

	w, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf("wss://ws.finnhub.io?token=%s", apiKey), nil)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	symbols := []string{"BINANCE:BTCUSDT"}
	for _, s := range symbols {
		msg, _ := json.Marshal(map[string]interface{}{"type": "subscribe", "symbol": s})
		w.WriteMessage(websocket.TextMessage, msg)
	}

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

			// Send to channel
			if apiResponse.Type == "trade" {
				apiDataChan <- apiResponse.Data[0]
			}

			// Print message
			// for _, tradeData := range apiResponse.Data {
			// 	fmt.Printf("Price: %f, Symbol: %s, Timestamp: %d, Volume: %f\n",
			// 		tradeData.Price, tradeData.Symbol, tradeData.Timestamp, tradeData.Volume)
			// }

		}
	}()

	// Goroutine for batch processing
	wg.Add(1)
	go func() {
		defer wg.Done()
		batchSize := 10
		batchSum := 0.0
		average := 0.0

		for {
			apiData := <-apiDataChan
			windowBatch.mu.Lock()
			windowBatch.InsertSorted(apiData)
			batchSum += apiData.Price
			if len(windowBatch.slice) == batchSize {
				average = batchSum / float64(batchSize)
				windowBatch.slice = windowBatch.slice[1:]
				batchSum -= windowBatch.slice[0].Price

				printerChan <- average
			}
			windowBatch.mu.Unlock()
		}
	}()

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
