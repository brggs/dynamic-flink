from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import datetime
import json

def send_message(producer, colour):
    message = {"@timestamp": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'), "colour": colour}
    producer.send('events', json.dumps(message).encode('utf-8'))

print ('Starting Producer', flush=True)

retries = 100

while True:
    try:
        producer = KafkaProducer(bootstrap_servers='kafka1:19092')
        send_message(producer, "Red")
        send_message(producer, "Blue")

        print ('Messages sent', flush=True)
        time.sleep(5)
    except NoBrokersAvailable as exc:
        if retries == 0:
            raise exc

        print ('Failed to connect to Kafka, retrying in 5s', flush=True)

        retries -= 1
        time.sleep(5)
