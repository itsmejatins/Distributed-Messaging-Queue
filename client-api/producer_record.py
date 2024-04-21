import uuid
from kazoo.client import KazooClient
import requests
import json
import logging
import time



class ProducerRecord:
    def __init__(self, topic: str, msg:str):
        self.topic = topic
        self.msg = msg
