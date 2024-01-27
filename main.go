package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
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

type MovingAverage struct {
	currency     string
	value        float64
	calculatedAt int64
}

func (batch *ThreadSafeCryptoBatch) InsertSorted(item TradeData) {
	// Binary search to find the correct position based on timestamp
	index := sort.Search(len(batch.slice), func(i int) bool {
		return batch.slice[i].Timestamp >= item.Timestamp
	})

	// Insert the item at the correct position
	batch.slice = append(batch.slice[:index], append([]TradeData{item}, batch.slice[index:]...)...)
}

func calculateMovingAverage(currency string, inputChan <-chan TradeData, outputChan chan<- MovingAverage, batch *ThreadSafeCryptoBatch, wg *sync.WaitGroup) {
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

			outputChan <- MovingAverage{currency: currency, value: average, calculatedAt: batch.slice[len(batch.slice)-1].Timestamp}
		}

		batch.mu.Unlock()
	}
}

func saveToDB(symbol string, average float64, calculatedAt int64, db *sql.DB) error {
	_, err := db.Exec(`
        INSERT INTO averages (symbol, average, calculated_at) VALUES ($1, $2, $3)
    `, symbol, average, time.Unix(calculatedAt, 0))
	return err
}

func main() {
	apiKey := "cmpois1r01qg7bbococ0cmpois1r01qg7bbococg"
	windowSize := 60

	btcChan := make(chan TradeData)
	ethChan := make(chan TradeData)
	ltcChan := make(chan TradeData)
	dbChan := make(chan MovingAverage)
	var wg sync.WaitGroup
	btcWindowBatch := &ThreadSafeCryptoBatch{windowSize: windowSize, slice: make([]TradeData, 0), sum: 0}
	ethWindowBatch := &ThreadSafeCryptoBatch{windowSize: windowSize, slice: make([]TradeData, 0), sum: 0}
	ltcWindowBatch := &ThreadSafeCryptoBatch{windowSize: windowSize, slice: make([]TradeData, 0), sum: 0}

	w, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf("wss://ws.finnhub.io?token=%s", apiKey), nil)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	// PostgreSQL database connection
	db, err := sql.Open("postgres", "host=postgres port=5432 user=postgres password=postgres dbname=postgres sslmode=disable")
	if err != nil {
		log.Fatal("Error opening database connection:", err)
	}
	defer db.Close()

	// Check if the db connection is successful
	err = db.Ping()
	if err != nil {
		log.Fatal("Error pinging database:", err)
	}

	symbols := []string{"BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "BINANCE:LTCUSDT"}
	for _, s := range symbols {
		msg, _ := json.Marshal(map[string]interface{}{"type": "subscribe", "symbol": s})
		w.WriteMessage(websocket.TextMessage, msg)
	}

	log.Printf("Wating for first %d values to calculate the first window", windowSize)

	// Goroutine for getting the data
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

	// Goroutines for batch processing
	wg.Add(1)
	go calculateMovingAverage("BTC", btcChan, dbChan, btcWindowBatch, &wg)

	wg.Add(1)
	go calculateMovingAverage("ETH", ethChan, dbChan, ethWindowBatch, &wg)

	wg.Add(1)
	go calculateMovingAverage("LTC", ltcChan, dbChan, ltcWindowBatch, &wg)

	// Goroutine for saving to DB
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			ma := <-dbChan

			err := saveToDB(ma.currency, ma.value, ma.calculatedAt, db)
			if err != nil {
				log.Println("Error saving to database:", err)
			}
		}
	}()

	// Wait for all goroutines to finish
	wg.Wait()
}
