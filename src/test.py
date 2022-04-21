import json
from pprint import pprint
from typing import Callable

from confluent_kafka import Consumer

from kafka_queue import kafka_io
from utils.utils import load_config_yaml

conf = load_config_yaml()

conf = {'bootstrap.servers': conf['kafka']['servers'], 'group.id': conf['kafka']['groupid'],
        'session.timeout.ms': conf['kafka']['timeout'],
        'auto.offset.reset': conf['kafka']['offset_reset']}
with open("../data/user_events.json") as f:
    j = json.load(f)
for i in j:
    print(f"loading user_events records:: {i}")
    kafka_io.publish("user_events", i["received_at"], json.dumps(i))


def subscribe(topic: str, function: Callable = None):
    consumer: Consumer = Consumer(conf)
    consumer.subscribe([topic])

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(2.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                print(f"Consumed event from topic {msg.topic()}: key={msg.key().decode('utf-8')}, value:")
                v = msg.value().decode("utf-8")
                pprint(json.loads(v))
                print("")
                if function:
                    function(v)
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
