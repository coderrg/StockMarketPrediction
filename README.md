# StockMarketPrediction
Apache Kafka and Facebook Prophet to generate short-term stock market predictions.

In this code, the producer and consumer are both locally hosted on the same computer, and the producer uses the Alpha Vantage API to simulate production of real-time stock market data.

# How To Run Locally

First, you will need to install Kafka and the dependencies in requirements.txt.

Next, open up the simulated_predictor.py file and make sure the paths for zookeeper and broker are correct for your system under the main function. 

Now that our Kafka server is running, we can simulate the Producer sending real-time market data to the consumer by running the command python simulated_predictor.py. By default, the Producer sends data from the day before the day you run this code, starting at the market opening at 9:30 AM ET to its closing at 4:00 PM ET that day. Feel free to change the STOCK variable with other stock symbols. The default is Facebook (FB). 

The consumer will predict "UP" or "DOWN" indicating whether it thinks the share price is going to increase or decrease in the next minute, respectively, using Facebook Prophet. Note that the prediction model uses the last 10 minutes of price data sent to it, so when you just start running the code, the predictions will be inaccurate for a bit.
