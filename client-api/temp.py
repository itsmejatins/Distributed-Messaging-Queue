from portal_client import PortalClient
from producer import PortalProducer


client = PortalClient(None)
producer = PortalProducer(client)

producer.send_message(topic="t1", message="message1")