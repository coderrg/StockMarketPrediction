# import necessary libraries

import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import time
from datetime import datetime, timedelta
from fbprophet import Prophet

import os
import sys
import subprocess
import multiprocessing

# constant variables
BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "simulated-realtime-stock-predictor"
TICKER = 'FB'

# specify which date from the last week you want to simulate in YYYY-MM-DD format
# make sure it is a weekday and not a stock market holiday
DATE_TO_SIMULATE = "2020-09-09"
BEG_TIME = DATE_TO_SIMULATE + " 09:30:00"
END_TIME = DATE_TO_SIMULATE + " 16:00:00"

# load API key from secrets file
import json
with open ("secrets.json") as f:
    api_key = json.load(f)["api_key"]

async def produce(topic_name):
    p = Producer({"bootstrap.servers": BROKER_URL})

    # use the Alpha Vantage API to get historical stock market data
    ts = TimeSeries(key=api_key, output_format='pandas')
    data, meta_data = ts.get_intraday(symbol='FB', interval = '1min', outputsize = 'full')
    data = data.reset_index()

    # keep datetime and opening price of each minute, convert the type of datetime column, and then rename the columns
    data_prophet = data[["date","1. open"]]
    data_prophet["date"] = pd.to_datetime(data_prophet["date"])
    data_prophet.columns = ["ds", "y"]

    # filter the dataframe for the date we want to simulate
    data_prophet = data_prophet[(data_prophet["ds"] >= BEG_TIME) & (data_prophet["ds"] <= END_TIME)]
    data_prophet = data_prophet.reset_index(drop=True)

    # reverse dataframe
    data_prophet = data_prophet.iloc[::-1]
    data_prophet = data_prophet.reset_index(drop = True)
    
    # produce rows one by one as would be the case in real life
    for row in range (data_prophet.shape[0]):
        # data_prophet.iloc[row]
        p.produce(topic_name, f"iteration {row}".encode("utf-8"))
        # sleep for 1 second
        await asyncio.sleep(1)

async def consume(topic_name):
    c = Consumer(
        {"bootstrap.servers": BROKER_URL, "group.id": "my-first-consumer-group"}
    )
    c.subscribe([topic_name])

    print("Streaming stock market data for " + TICKER + " on " + DATE_TO_SIMULATE)
    
    # start with an hour of data for the day and go through the whole day
    current_sample = 60
    
    # initialize variables for consumer
    seen = 0
    data_so_far = pd.DataFrame(columns=['ds','y'])
    prediction = None
    correct_predictions = 0
    total_predictions = 0
    
    while True:
        
        message = c.poll(1.0)
        
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
            
        else:
            seen += 1
            next_row = message.value()
            data_so_far.append(next_row)
            
            # compare last prediction to actual value
            if (prediction is not None):
                if (data_so_far['y'].iloc[-1] > data_so_far['y'].iloc[-2]):
                    print("Correct: UP")
                    if (prediction):
                        correct_predictions += 1
                else:
                    print("Correct: DOWN")
                    if (not prediction):
                        correct_predictions += 1
                total_predictions += 1
                print("Cumulative Correct Predictions:", correct_predictions)
                print("Cumulative Total Predictions:", total_predictions)
            print("-------")
            
            # only start making predictions after we have collected at least 60 samples (one hour of prices)
            if (seen >= 60):
                # look at data so far and make prediction for one future time sample with Prophet
                data_so_far = data_prophet.loc[:total_predictions]
                m = Prophet(yearly_seasonality=False, weekly_seasonality=False, daily_seasonality=False)
                m.fit(data_so_far)
                future = m.make_future_dataframe(periods=1, freq = 'min', include_history = False)
                forecast = m.predict(future)

                # check if the prediction is for the price to go up or down
                if (float(forecast['yhat']) > data_so_far['y'].iloc[-1]):
                    print("Prediction: UP")
                    prediction = True
                else:
                    print("Prediction: DOWN")
                    prediction = True
        
            print(f"Consumed message {message.key()}: {message.value()}")
            
        await asyncio.sleep(1)


async def produce_consume():
    t1 = asyncio.create_task(produce(TOPIC_NAME))
    t2 = asyncio.create_task(consume(TOPIC_NAME))
    await t1
    await t2

def main():
    try:
        zookeeper_server = subprocess.Popen(["zookeeper-server-start","/usr/local/etc/kafka/zookeeper.properties"])
        broker_server = subprocess.Popen(["kafka-server-start","/usr/local/etc/kafka/server.properties"])
        # Give some time for server to start up
        time.sleep(20)
        print("Kafka server is running!")
    except KeyboardInterrupt as e:
        print("Did not finish setting up Kafka server")

    client = AdminClient({"bootstrap.servers": BROKER_URL})
    topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)
    client.create_topics([topic])
    print("All set up!")

    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt as e:
        print("Did not finish producing and consuming available data")

    finally:
        client.delete_topics([topic])
        print("Topics deleted")

        os.system("zookeeper-server-stop /usr/local/etc/kafka/zookeeper.properties")
        print("Zookeeper stopped")
        os.system("kafka-server-stop /usr/local/etc/kafka/server.properties")
        print("Broker stopped")

        time.sleep(5)

        zookeeper_server.kill()
        print("Zookeeper terminated")
        broker_server.kill()
        print("Broker terminated")

if __name__ == "__main__":
    main()
