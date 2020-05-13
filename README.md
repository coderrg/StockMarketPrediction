# StockMarketPrediction
Apache Kafka and Facebook Prophet to generate short-term stock market predictions.

In this code, the producer and consumer are both locally hosted on the same computer, and the producer uses the Alpha Vantage API to simulate production of real-time stock market data.

# How To Run Locally

First, you will need to install Kafka and the dependencies in requirements.txt.

Next, start up the Kafka zookeeper by running the command zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties (you may need to change the path). Then, open up another terminal window, and start the Kafka broker with the command kafka-server-start /usr/local/etc/kafka/server.properties (once again, the path may vary).

Now that our Kafka server is running, we can start simulating the Producer sending real-time stock market data to the consumer. The consumer will predict "UP" or "DOWN" indicating whether it thinks the share price is going to increase or decrease in the next minute, respectively, using Facebook Prophet. Note that the prediction model uses the last 10 minutes of price data, so when the market opens, the predictions will be inaccurate for a bit.
