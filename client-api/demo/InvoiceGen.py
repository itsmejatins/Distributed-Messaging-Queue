import sys

sys.path.insert(1, '../')
import threading
import json
from portal_client import PortalClient
from producer import PortalProducer
import time
import random
import signal

config = json.load(open('config.json'))['brokers']
inGen = None


class InvoiceProducer(threading.Thread):
    def __init__(self, gen_name):
        threading.Thread.__init__(self)
        self.client = PortalClient(None)
        self.producer = PortalProducer(self.client)
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
            self.producer.send_message("All Invoices", f"{invoice} Invoice {self.gen_name} {count}")
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
    inGen = InvoiceProducer(gen_name)
    inGen.start()
