import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import time
from datetime import datetime, timedelta
from fbprophet import Prophet

import os
import subprocess
import multiprocessing

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "simulated-realtime-stock-predictor"
STOCK = 'FB'
api_key = 'B0N8Q38MJAVBSLOY'
# Simulate stock market stream from yesterday
DAY_TO_SIMULATE = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

async def start_zookeeper():
    os.system("zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties")

async def start_broker():
    os.system("kafka-server-start /usr/local/etc/kafka/server.properties")

async def start_kafka_server():
    zookeeper = asyncio.create_task(start_zookeeper())
    broker = asyncio.create_task(start_broker())
    await zookeeper
    await broker

async def produce(topic_name):
    p = Producer({"bootstrap.servers": BROKER_URL})

    ts = TimeSeries(key=api_key, output_format='pandas')
    data, meta_data = ts.get_intraday(symbol='FB', interval = '1min', outputsize = 'full')

    data = data.reset_index()
    data_prophet = data[["date","1. open"]]
    data_prophet.columns = ["ds", "y"]

    # Use to reverse dataframe
    data_prophet = data_prophet.iloc[::-1]
    data_prophet = data_prophet.reset_index(drop=True)

    # Only consider one day of data
    data_prophet['string_ds'] = data_prophet['ds'].astype(str)
    data_prophet = data_prophet[data_prophet.string_ds.str.startswith((DAY_TO_SIMULATE))]
    data_prophet = data_prophet.drop(['string_ds'], axis = 1)
    data_prophet = data_prophet.reset_index(drop=True)

    i = 0
    market_open = True
    market_closing_time = DAY_TO_SIMULATE + " 16:00:00"
    while (market_open):
        current_time = data_prophet['ds'].iloc[i]
        current_price = data_prophet['y'].iloc[i]
        
        if (market_closing_time == str(current_time)):
            market_open = False

        p.produce(topic_name, f"iteration {i}".encode("utf-8"))

        i = i + 1

        # sleep for 10 seconds
        await asyncio.sleep(10)


async def consume(topic_name):
    c = Consumer(
        {"bootstrap.servers": BROKER_URL, "group.id": "my-first-consumer-group"}
    )
    c.subscribe([topic_name])

    print("Streaming stock market data for " + STOCK + " on " + DAY_TO_SIMULATE)
    while True:
        message = c.poll(1.0)
        
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(1)


async def produce_consume():
    t1 = asyncio.create_task(produce(TOPIC_NAME))
    t2 = asyncio.create_task(consume(TOPIC_NAME))
    await t1
    await t2

def main():
    try:
        k = multiprocessing.Process(target=start_kafka_server)
        k.start()
        time.sleep(60)
        k.terminate()
        k.join()
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


if __name__ == "__main__":
    main()
