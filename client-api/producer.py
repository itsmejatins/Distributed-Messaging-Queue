from portal_client import PortalClient


class PortalProducer:
    def __init__(self, client: PortalClient):
        self.client = client
        self.register()

    def register(self):
        print("Registering producer for sensor")
        # you must be wondering what this data is. PortalProducer has a PortalClient's object inside it. This object
        # is responsible for making requests (POST and GET) to the broker which is a leader. It automatically keeps
        # track of the leader and sends requests to the leader only, not some random broker. PortalClient upon
        # creation automatically assigns itself a unique id using uuid.uuid4() It then always appends its id to the
        # post requests it makes. For get request, you don't need to send anybody, hence there is no data.

        # In the below step it just registers itself with the broker and the id of this producer is the id chosen by
        # the PortalClient object (appended to empty data json(dict) being sent.
        resp, _ = self.client.request('POST', '/producer/create', data={})
        print("ye dekh", resp) # Response -> Producer registered successfully

    def unregister(self):
        resp, _ = self.client.request('POST', '/producer/delete', data={})
        # print(resp)

    def is_exists_topic(self, topic):
        data = {
            "name": topic
        }
        resp, _ = self.client.request('POST', '/topic/exists', data)
        return resp['message']

    def create_topic(self, topic):
        data = {
            "name": topic
        }
        resp, _ = self.client.request('POST', '/topic/create', data)
        # print(resp['message'])

    def send_message(self, topic, message: str):
        if not self.is_exists_topic(topic):
            self.create_topic(topic)
        data = {
            "name": topic,
            "message": message
        }
        while True:
            resp, status = self.client.request('POST', '/publish', data)
            if status == 200:
                break
        # print(resp)

    def __del__(self):
        self.unregister()
