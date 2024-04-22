import sys

sys.path.insert(1, '../')
import threading
import json

from myproducer import Producer
from properties import Properties
from producer_record import ProducerRecord

import time
import random
import signal
import uuid

inGen = None


class InvoiceProducer(threading.Thread):
    def __init__(self, gen_name, bootstrap_servers, id):
        threading.Thread.__init__(self)

        self.props = Properties()
        self.props.put(key="bootstrap_servers", val=bootstrap_servers)
        self.props.put(key="id", val=id)

        self.producer = Producer(self.props)

        self._stopevent = threading.Event()
        self.gen_name = gen_name

    def generateInvoice(self):
        num = random.randint(1, 10)
        if num <= 5:
            return "Valid"
        else:
            return "Invalid"

    def run(self):
        count = 0
        while True:
            invoice = self.generateInvoice()
            if self._stopevent.isSet():
                break

            pr = ProducerRecord("All Invoices", f"{invoice} Invoice {self.gen_name} {count}")
            self.producer.send(pr)
            count += 1
            print(f"Invoice {self.gen_name} {count}")
            time.sleep(1)

    def stop(self):
        self._stopevent.set()


def handler(signum, frame):
    print('Shutting down generator...')
    inGen.stop()


signal.signal(signal.SIGINT, handler)

if __name__ == "__main__":
    gen_name = sys.argv[1]
    bootstrap_servers = []
    for i in range(2, len(sys.argv)):
        bootstrap_servers.append(sys.argv[i])

    id_ = uuid.uuid4()
    inGen = InvoiceProducer(gen_name, bootstrap_servers, int(id_))
    inGen.start()
