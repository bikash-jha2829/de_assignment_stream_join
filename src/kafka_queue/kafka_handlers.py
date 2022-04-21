import json
import logging

from postgre_etl.postgre_op import PostgresEtl
from utils.custom_logger import ensure_basic_logging

LOG_FORMAT = " %(asctime)s %(levelname)-8s : %(name)s %(message)s"

logger = logging.getLogger("ETL_Pipeline_main")


def dead_letter_queue(queue_name, row):
    pass


@ensure_basic_logging(level=logging.INFO, format=LOG_FORMAT)
def handle_user_events(data: str):
    try:
        rows = json.loads(data)
        if not rows.get("username"):
            return

        obj = PostgresEtl()
        obj.upsert_user_details(rows)

    except Exception as err:
        logger.error(err)
        dead_letter_queue("dead_queue_sqs", rows)


@ensure_basic_logging(level=logging.INFO, format=LOG_FORMAT)
def handle_org_events(data: str):
    try:
        rows = json.loads(data)
        obj = PostgresEtl()
        obj.write_org_data(rows)

    except Exception as err:
        logger.error(err)
        dead_letter_queue("dead_queue_sqs", rows)
