import uuid
from kazoo.client import KazooClient
import requests
import json
import logging
import time

TIMEOUT_CONNECT = 5
TIMEOUT_REQUEST = 3
MAX_RETRY = 20


class ProducerRecord:
    def __init__(self, topic: str, msg:str):
        self.topic = topic
        self.msg = msg
