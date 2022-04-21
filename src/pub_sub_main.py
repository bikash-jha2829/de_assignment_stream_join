import json
import logging

import ray

from kafka_queue import kafka_io
from kafka_queue.kafka_handlers import handle_user_events, handle_org_events
from utils.custom_logger import ensure_basic_logging

logger = logging.getLogger("ETL_Pipeline_main")
LOG_FORMAT = " %(asctime)s %(levelname)-8s : %(name)s %(message)s"
ray.init()


@ensure_basic_logging(level=logging.INFO, format=LOG_FORMAT)
@ray.remote
def publish():
    topics = ["user_events", "org_events"]
    try:
        kafka_io.create_topics(topics)
    except Exception as err:
        logger.exception(err)

    def stream_user_events():
        with open("../data/user_events.json") as f:
            j = json.load(f)
        for i in j:
            logger.info(f"loading user_events records:: {i}")
            kafka_io.publish("user_events", i["received_at"], json.dumps(i))

    def stream_org_events():
        with open("../data/org_events.json") as f:
            j = json.load(f)
        for i in j:
            logger.info(f"loading org_events records:: {i}")
            kafka_io.publish("org_events", i["created_at"], json.dumps(i))

    stream_user_events()
    stream_org_events()


@ensure_basic_logging(level=logging.INFO, format=LOG_FORMAT)
@ray.remote
def subscribe():
    # Subscribe
    def subscribe_topic_users():
        kafka_io.subscribe("user_events", handle_user_events)

    def subscribe_topic_org():
        kafka_io.subscribe("org_events", handle_org_events)

    subscribe_topic_users()
    subscribe_topic_org()


if __name__ == '__main__':
    # Execute func1 and func2 in parallel.
    ray.get([publish.remote(), subscribe.remote()])
