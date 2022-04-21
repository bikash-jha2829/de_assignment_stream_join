import json
import logging
import sys
from pprint import pprint
from typing import List, Callable

from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from utils.custom_logger import ensure_basic_logging
from utils.utils import load_config_yaml

LOG_FORMAT = " %(asctime)s %(levelname)-8s : %(name)s %(message)s"
logger = logging.getLogger("ETL_Pipeline_main")

conf = load_config_yaml()

config = {'bootstrap.servers': conf['kafka']['servers'], 'group.id': conf['kafka']['groupid'],
          'session.timeout.ms': conf['kafka']['timeout'],
          'auto.offset.reset': conf['kafka']['offset_reset']}


@ensure_basic_logging(level=logging.INFO, format=LOG_FORMAT)
def create_topics(topics: List[str]):
    # Create kafka topics.

    new_topics = [NewTopic(topic=topic, num_partitions=2, replication_factor=1) for topic in topics]

    admin = AdminClient(conf)

    fs = admin.create_topics(new_topics)

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logger.info("Topic {} created".format(topic))
        except Exception as e:
            logger.error("Failed to create topic {}: {}".format(topic, e))


@ensure_basic_logging(level=logging.INFO, format=LOG_FORMAT)
def publish(topic: str, key, value):
    # Publish to a  kafka topic
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' % (msg.topic(), msg.partition(), msg.offset()))

    # producer1.produce(topic=kafka_topic_name, key=str(uuid4()), value=jsonv1, on_delivery=delivery_report)
    producer.produce(topic, value, key, callback=delivery_callback)

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()


def subscribe(topics, function: Callable = None):
    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger('consumer')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
    logger.addHandler(handler)

    # Create Consumer instance
    # Hint: try debug='fetch' to generate some log messages
    c = Consumer(config, logger=logger)

    def print_assignment(consumer, partitions):
        logger.info('Assignment:', partitions)

    # Subscribe to topics
    c.subscribe(topics, on_assign=print_assignment)

    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = c.poll(timeout=2.0)
            if msg is None:
                logger.info("waiting for message to arrive")
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Proper message
                # Extract the (optional) key and value, and print.
                logger.info(f"Consumed event from topic {msg.topic()}: key={msg.key().decode('utf-8')}, value:")
                v = msg.value().decode("utf-8")
                pprint(json.loads(v))
                print("")
                if function:
                    function(v)

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        c.close()
