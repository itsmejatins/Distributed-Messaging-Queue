class Properties:
    def __init__(self):
        # self.bootstrap_servers = [] # dict of ip:port of broker ips
        # self.id = None
        # self.zookeeper_host = {} # key = ip, key = where zookeeper is running
        # self.name = None
        self.props = {}

    def put(self, key, val):
        self.props[key] = val

    def get(self, key):
        return self.props[key]

