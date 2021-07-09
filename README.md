Fun with Channels
=================

## Goals

- Create a simple concurrent data pipeline in Go
- Display an understanding of concurrency concepts
- Properly using a third party API

## Background

The idea here is to create a data pipeline from a continuous stream of third party API data.  The data will be sent through channels to perform a basic transformation function and then print the output.

The data generator will be Last Price Updates from [Finnhub](https://finnhub.io/docs/api/websocket-trades) of a cryptocurrency of your choice like `BINANCE:BTCUSDT`, `BINANCE:ETHUSDT`, or `BINANCE:ADAUSDT`.

The transformation function to be used is a [simple moving average](https://www.investopedia.com/articles/active-trading/052014/how-use-moving-average-buy-stocks.asp) calculation.

Although we are using financial data and algorithms, you do not need to understand these concepts because the goal here is to create a simple data pipeline and not to optimize a financial instrument.  The algorithm to be used can be found [here](https://nestedsoftware.com/2018/03/20/calculating-a-moving-average-on-streaming-data-5a7k.22879.html).  

One thing to keep in mind is the concept of the window size.  The window size refers to how many values you use to make the calculation.  This means that you have to wait for `n` values in order to make the calculation.  The stream of data seems to be every second, so choosing a window size of `n=60` would mean the calculation is a moving average for each minute.

## Requirements
- No third party packages other than the `gorilla/websocket` package used in the sample code of [Finnhub Docs](https://finnhub.io/docs/api/websocket-trades), and if you wish to choose to use a thread safe data structure to hold the window data.
- We're only interested in the Last price `p` and Timestamp `t`, as well as only considering prices that have the `type` of `trade`.

## Getting Started
- Register for a free account at [Finnhub](https://finnhub.io/dashboard) to get an API Key.
- The sample code is fine to use as the data generator, and you probably don't need to add to it.
- Start with just one crypto value like `BTC` or `ETH` and printing the output instead of storing it.

## Bonus
- Complete with 3 different cryptocurrencies.
- Store the moving averages to storage like a file or a database of your choosing.
- Write tests
- Dockerize
