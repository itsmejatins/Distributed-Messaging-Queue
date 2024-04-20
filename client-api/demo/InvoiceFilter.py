import sys

sys.path.insert(1, '../')
import threading
import json
from portal_client import PortalClient
from consumer import PortalConsumer
from producer import PortalProducer
import time
import random

config = json.load(open('config.json'))['brokers']
import signal

filter = None


class InvoiceFilter(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.consumer_client = PortalClient(None)
        self.consumer = PortalConsumer(self.consumer_client, "All Invoices")
        self.producer_client = PortalClient(None)
        self.producer = PortalProducer(self.producer_client)
        self._stopevent = threading.Event()

    def run(self):
        while True:
            message = self.consumer.get_message()
            if self._stopevent.isSet():
                break
            if message.startswith("Valid"):
                self.producer.send_message("Valid Invoice", message)
            else:
                self.producer.send_message("Invalid Invoice", message)
            print(f"{self.name} received message: {message}")
            time.sleep(3)

    def stop(self):
        self._stopevent.set()
        self.consumer.stop()


def handler(signum, frame):
    print('Shutting down controller...')
    filter.stop()


signal.signal(signal.SIGINT, handler)

if __name__ == '__main__':
    name = sys.argv[1]
    filter = InvoiceFilter(name)
    filter.start()
