import json
import sys
import time

import requests
from kazoo.client import KazooClient

MAX_RETRY = 30


def _send_request( path, data):
    tries = 0
    while tries < MAX_RETRY:
        try:
            response = requests.post(f"http://{leader['host']}:{leader['port']}{path}", json=data)
            if not response.status_code == 200:
                return None, response.status_code
            return response.json(), response.status_code

        except Exception as e:
            time.sleep(1)
            tries += 1

        response = requests.post(f"http://{leader['host']}:{leader['port']}{path}", json=data)
        if not response.status_code == 200:
            return None, response.status_code
        return response.json(), response.status_code


if __name__ == "__main__":
    topic = sys.argv[1]
    bootstrap_servers = []
    for i in range(2, len(sys.argv)):
        bootstrap_servers.append(sys.argv[i])

    zk = KazooClient(hosts=bootstrap_servers[0])
    zk.start()
    data, _ = zk.get('/leader')
    leader = json.loads(data.decode())

    data = {
        "name": topic
    }
    resp, _ = _send_request(path='/topic/create', data=data)
    print(resp['message'])