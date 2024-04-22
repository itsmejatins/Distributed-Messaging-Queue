import json
import threading
import time

import requests
from kazoo.client import KazooClient

from properties import Properties

MAX_RETRY = 10


class Consumer:
    def __init__(self, props: Properties):
        self.bootstrap_servers = props.get('bootstrap_servers')  # list of zookeeper addresses (ip:port)
        self.id = props.get('id')
        self.topic = props.get('topic')

        self.zk = KazooClient(hosts=self.bootstrap_servers[0])
        self.zk.start()
        data, _ = self.zk.get('/leader')
        self.leader = json.loads(data.decode())

        if not self.isTopic(self.topic):
            self.createTopic(self.topic)

        self._event = threading.Event()

        @self.zk.DataWatch('/leader')
        def watch_leader_node(data, abc):
            # This function will be called whenever the data of the "/leader" node changes
            self.leader = json.loads(data.decode())
            print(f"New leader: {self.leader}")

        @self.zk.DataWatch(f'locks/topics/{self.topic}')
        def watch_lock_status(data, stat):
            lock_state = data.decode()
            if lock_state == '0':
                self._event.set()
            else:
                self._event.clear()

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
        print(f"Registering consumer with id = {self.id}")
        data = {"id": str(self.id)}

        resp, resp_code = self._send_request(path='/consumer/create', data=data)
        self.zk.create(f'/consumers/{self.id}', ephemeral=True)

        if resp_code != 200:
            print(f"Failed to register consumer with id = {self.id}")
        else:
            print(f"Registered consumer with id = {self.id} successfully")
            print(f"Response from broker = {resp}")

    def _unregister(self):
        resp, status_code = self._send_request(path='/consumer/delete', data={"id": str(self.id)})
        if status_code != 200:
            print(f"Unregistration failed for consumer {self.id}")

        else:
            print(f"Successfully unregistered consumer with id = {self.id}")
            print(f"Response from broker = {resp}")

    def isTopic(self, topic):
        data = {
            "id": str(self.id),
            "name": topic
        }
        resp, _ = self._send_request(path='/topic/exists', data=data)
        return resp['message']

    def createTopic(self, topic):
        data = {
            "id": str(self.id),
            "name": topic
        }
        resp, _ = self._send_request(path='/topic/create', data=data)
        # print(resp['message'])

    def oneCycle(self, topic):
        try:
            resp, status = self._send_request(path='/read', data={"id": str(self.id), "name": topic})
            if status == 200:
                return resp
        except Exception:
            pass
        return None

    def consume(self):
        while True:
            resp_mesg = self.oneCycle(self.topic)
            if resp_mesg is None:
                self._event.wait()
                continue
            _,status=self._send_request(path='/consume',data={"id": str(self.id),"name":self.topic,"offset":resp_mesg['offset']})
            # self.topic_unlock(topic)
            if status == 200:
                return resp_mesg['message']

    def __del__(self):
        self._unregister()
