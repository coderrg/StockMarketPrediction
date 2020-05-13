import asyncio

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import time
from datetime import datetime, timedelta
from fbprophet import Prophet

import os

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "simulated-realtime-stock-predictor"
STOCK = 'FB'
api_key = 'B0N8Q38MJAVBSLOY'


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})

    d = datetime.today() - timedelta(days=1)
    day_to_simulate = d.strftime('%Y-%m-%d')

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
    data_prophet = data_prophet[data_prophet.string_ds.str.startswith((day_to_simulate))]
    data_prophet = data_prophet.drop(['string_ds'], axis = 1)
    data_prophet = data_prophet.reset_index(drop=True)

    i = 0
    market_open = True
    market_closing_time = day_to_simulate + " 16:00:00"
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
    """Consumes data from the Kafka Topic"""
    c = Consumer(
        {"bootstrap.servers": BROKER_URL, "group.id": "my-first-consumer-group"}
    )
    c.subscribe([topic_name])

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
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(TOPIC_NAME))
    t2 = asyncio.create_task(consume(TOPIC_NAME))
    await t1
    await t2


def main():
    """Runs the exercise"""
    try:
        # Start and set up Kafka Server
        os.system('zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties')
        os.system('kafka-server-start /usr/local/etc/kafka/server.properties')

        client = AdminClient({"bootstrap.servers": BROKER_URL})
        topic = NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1)
        client.create_topics([topic])
    except KeyboardInterrupt as e:
        print("Did not finish setting up Kafka server")
    finally:
        client.delete_topics([topic])

    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt as e:
        print("Did not finish producing and consuming available data")
    finally:
        client.delete_topics([topic])


if __name__ == "__main__":
    main()
