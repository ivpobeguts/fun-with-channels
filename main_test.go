package main

import (
	"sync"
	"testing"
	"time"
)

func TestInsertSortedOk(t *testing.T) {
	batch := &ThreadSafeCryptoBatch{
		slice:      make([]TradeData, 0),
		windowSize: 3,
		sum:        0,
	}

	// Insert items in reverse order
	item1 := TradeData{Price: 10, Timestamp: 1, Symbol: "BTC"}
	item2 := TradeData{Price: 20, Timestamp: 2, Symbol: "BTC"}
	item3 := TradeData{Price: 30, Timestamp: 3, Symbol: "BTC"}

	batch.InsertSorted(item3)
	batch.InsertSorted(item2)
	batch.InsertSorted(item1)

	// Check if the items are sorted correctly
	expected := []TradeData{item1, item2, item3}
	for i, item := range batch.slice {
		if item != expected[i] {
			t.Errorf("Expected %v, but got %v", expected[i], item)
		}
	}
}

func TestCalculateMovingAverageOk(t *testing.T) {
	inputChan := make(chan TradeData)
	outputChan := make(chan MovingAverage)
	var wg sync.WaitGroup
	batch := &ThreadSafeCryptoBatch{
		slice:      make([]TradeData, 0),
		windowSize: 3,
		sum:        0,
	}

	wg.Add(1)
	go calculateMovingAverage("BTC", inputChan, outputChan, batch, &wg)

	// Create test data
	inputData := []TradeData{
		{Price: 10.1, Timestamp: 1, Symbol: "BTC"},
		{Price: 10.2, Timestamp: 2, Symbol: "BTC"},
		{Price: 10.3, Timestamp: 3, Symbol: "BTC"},
		{Price: 10.4, Timestamp: 4, Symbol: "BTC"},
	}

	wg.Add(1)
	go func() {
		for _, data := range inputData {
			inputChan <- data
		}
	}()

	// Sleep 2 seconds to wait goroutines to handle data
	time.Sleep(2 * time.Second)
	wg.Done()

	// Read and check results
	expected := []MovingAverage{
		{currency: "BTC", value: 10.2, calculatedAt: 3},
		{currency: "BTC", value: 10.27, calculatedAt: 4},
	}

	for _, ma := range expected {
		result, ok := <-outputChan
		if !ok {
			break
		}

		if result != ma {
			t.Errorf("Expected %v, but got %v", ma, result)
		}
	}
}
