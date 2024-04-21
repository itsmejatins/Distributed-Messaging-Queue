import sys
import uuid

sys.path.insert(1, '../')
import threading
import json

from myconsumer import Consumer
from myproducer import Producer
from properties import Properties
from producer_record import ProducerRecord

import time
import random

config = json.load(open('config.json'))['brokers']
import signal

filter_ = None


class InvoiceFilter(threading.Thread):
    def __init__(self, fil_name, bootstrap_servers):
        threading.Thread.__init__(self)

        self.name = fil_name
        self.bootstrap_servers = bootstrap_servers

        producerProps = Properties()
        producerProps.put(key="bootstrap_servers", val=bootstrap_servers)
        producerProps.put(key="id", val=uuid.uuid4())
        self.producer = Producer(producerProps)

        consumerProps = Properties()
        consumerProps.put(key="bootstrap_servers", val=bootstrap_servers)
        consumerProps.put(key="id", val=uuid.uuid4())
        consumerProps.put(key="topic", val="All Invoices")

        self.consumer = Consumer(consumerProps)

        self._stopevent = threading.Event()

    def run(self):
        while True:
            message = self.consumer.get_message()
            if self._stopevent.isSet():
                break
            if message.startswith("Valid"):
                pr = ProducerRecord("Valid Invoices", message)
                self.producer.send(pr)
            else:
                pr = ProducerRecord("InValid Invoices", message)
                self.producer.send(pr)
            print(f"{self.name} received message: {message}")
            time.sleep(3)

    def stop(self):
        self._stopevent.set()
        self.consumer.stop()


def handler(signum, frame):
    print('Shutting down controller...')
    filter_.stop()


signal.signal(signal.SIGINT, handler)

if __name__ == '__main__':
    name = sys.argv[1]
    bootstrap_servers = []
    for i in range(2, len(sys.argv)):
        bootstrap_servers.append(sys.argv[i])

    filter_ = InvoiceFilter(name, bootstrap_servers)
    filter_.start()
