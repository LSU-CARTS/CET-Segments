import sys
import os
import pandas as pd
import numpy as np
import pika
from io import BytesIO
import json

def publish_event(channel, method, body,exchange):
    channel.basic_publish(exchange=exchange, routing_key='' ,body=body)
    channel.basic_ack(delivery_tag=method.delivery_tag)

def publish_statusevent(channel,body,exchange):
    channel.basic_publish(exchange=exchange, routing_key='' ,body=body)

def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='cet_test_queue')

    def cet(data):
        data = BytesIO(data).getvalue()
        data = json.loads(data)

