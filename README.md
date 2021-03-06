# Stock Market Prediction
Apache Kafka and Facebook Prophet to simulate short-term stock market prediction.

In this project, the Producer and Consumer are both locally hosted on the same device, and the Producer uses the Alpha Vantage API to simulate production of real-time stock market data.

# How To Run Locally

Note: You need a [free Alpha Vantage API key](https://www.alphavantage.co/support/#api-key) in order to run this project locally. After obtaining the API key, save it in a file called secrets.json with the format {"api_key": "YOUR_API_KEY_HERE"} in the same directory as the code.

First, you will need to install Kafka and the dependencies in requirements.txt.

Next, open the simulated_predictor.py file and make sure the paths for zookeeper and broker are correct for your system under the main function. There are other constant variables you can change as well like the ticker symbol indicating which stock to simulate market prediction for.

We can simulate the Producer sending real-time market data to the consumer by running the command python simulated_predictor.py. Once the Consumer has received 60 minutes of data, it will begin predicting "UP" or "DOWN" indicating whether the share price will increase or decrease in the next minute, respectively, using Facebook Prophet. After the next minute's price comes in from the Producer, we check the Consumer's prediction and keep a running tally of its performance.
