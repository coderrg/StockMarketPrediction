# StockMarketPrediction
Apache Kafka and Facebook Prophet to generate short-term stock market predictions.

In this project, the Producer and Consumer are both locally hosted on the same device, and the Producer uses the Alpha Vantage API to simulate production of real-time stock market data.

# How To Run Locally

First, you will need to install Kafka and the dependencies in requirements.txt.

Next, open the simulated_predictor.py file and make sure the paths for zookeeper and broker are correct for your system under the main function. There are other constant variables you can change as well like the ticker symbol.

We can simulate the Producer sending real-time market data to the consumer by running the command python simulated_predictor.py. Once the Consumer has received 60 minutes of data, it will begin predicting "UP" or "DOWN" indicating whether the share price will increase or decrease in the next minute, respectively, using Facebook Prophet. After the next minute's price comes in from the Producer, we check the Consumer's prediction and keep a running tally of its performance.
