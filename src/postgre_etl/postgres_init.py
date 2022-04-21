import logging

import psycopg2

from utils.custom_logger import ensure_basic_logging
from utils.utils import load_config_yaml

LOG_FORMAT = " %(asctime)s %(levelname)-8s : %(name)s %(message)s"
logger = logging.getLogger("ETL_Pipeline_main")

conf = load_config_yaml()


@ensure_basic_logging(level=logging.INFO, format=LOG_FORMAT)
def postgres_init(connection=None, **kwargs):
    logger.info("Creating psycopg2 connection")
    if not connection:
        connection = psycopg2.connect(user=conf['postgres']['server'],
                                      password=conf['postgres']['password'],
                                      host=conf['postgres']['host'],
                                      port=conf['postgres']['port'],
                                      database=conf['postgres']['database'])
    logger.info("Successfully created psycopg2 connection")
    return connection
