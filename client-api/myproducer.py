import json
import time

from kazoo.client import KazooClient

from properties import Properties
from producer_record import ProducerRecord
import requests

MAX_RETRY = 20


class Producer:
    def __init__(self, props: Properties):
        self.bootstrap_servers = props.get('bootstrap_servers')  # list of zookeeper addresses (ip:port)
        self.id = props.get('id')

        self.zk = KazooClient(hosts=self.bootstrap_servers[0])
        self.zk.start()
        data, _ = self.zk.get('/leader')
        self.leader = json.loads(data.decode())

        @self.zk.DataWatch('/leader')
        def watch_leader_node(data, abc):
            # This function will be called whenever the data of the "/leader" node changes
            self.leader = json.loads(data.decode())
            print(f"New leader: {self.leader}")

        self._register()

    def _send_request(self, path, data):
        tries = 0
        while tries < MAX_RETRY:
            try:
                response = requests.post(f"http://{self.leader['host']}:{self.leader['port']}{path}", json=data)
                if not response.status_code == 200:
                    return None, response.status_code
                return response.json(), response.status_code

            except Exception as e:
                time.sleep(1)
                tries += 1

            response = requests.post(f"http://{self.leader['host']}:{self.leader['port']}{path}", json=data)
            if not response.status_code == 200:
                return None, response.status_code
            return response.json(), response.status_code

    def _register(self):
        print(f"Registering producer id = {self.id}")
        data = {"id": str(self.id)}

        resp, resp_code = self._send_request(path='/producer/create', data=data)

        if resp_code != 200:
            print(f"Failed to register producer with id = {self.id}")
        else:
            print(f"Registered producer with id = {self.id} successfully")
            print(f"Response from broker = {resp}")

    def unregister(self):
        resp, resp_code = self._send_request(path='/producer/delete', data={"id": str(self.id)})
        if resp_code != 200:
            print(f"Unregistration failed for producer {self.id}")

        else:
            print(f"Successfully unregistered producer with id = {self.id}")
            print(f"Response from broker = {resp}")

    def is_exists_topic(self, topic):
        data = {
            "id": str(self.id),
            "name": topic
        }
        resp, status = self._send_request(path='/topic/exists', data=data)
        return resp['message']

    def create_topic(self, topic):
        data = {
            "id": str(self.id),
            "name": topic
        }
        resp, _ = self._send_request(path='/topic/create', data=data)
        print(resp['message'])

    def send(self, pr: ProducerRecord):
        topic = pr.topic
        msg = pr.msg

        if not self.is_exists_topic(topic):
            self.create_topic(topic)

        data = {
            "id": str(self.id),
            "name": topic,
            "message": msg
        }

        while True:
            resp, status = self._send_request(path='/publish', data=data)
            if status == 200:
                break

    def __del__(self):
        self.unregister()
