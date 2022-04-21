import logging

import pandas as pd
import yaml

from utils.custom_logger import ensure_basic_logging

LOG_FORMAT = " %(asctime)s %(levelname)-8s : %(name)s %(message)s"

logger = logging.getLogger("ETL_Pipeline_main")


@ensure_basic_logging(level=logging.INFO, format=LOG_FORMAT)
def upsert_query_user_events(df: pd.DataFrame):
    query = (f""" 
                INSERT INTO events.user_events(id, event_type, username, user_email, user_type, organization_name, received_at)
                VALUES {','.join([str(i) for i in list(df.to_records(index=False))])}
                ON CONFLICT (user_email)
                DO  UPDATE SET name= excluded.id,
                               event_type= excluded.event_type,
                               username= excluded.username,
                               user_email= excluded.email,
                               user_type= excluded.user_type,
                               organization_name=excluded.organization_name,
                               received_at=excluded.received_at
                               
                               
            """)


@ensure_basic_logging(level=logging.INFO, format=LOG_FORMAT)
def run_postgre_query(conn, cursor, query):
    with cursor:
        try:
            logger.info(f"running sql query")
            cursor.execute(query)
        except (TypeError, Exception) as err:
            conn.rollback()
            cursor.close()
            raise err


@ensure_basic_logging(level=logging.INFO, format=LOG_FORMAT)
def load_config_yaml():
    with open("../config_files/config.yaml", "r") as stream:
        try:
            config = (yaml.safe_load(stream))
        except yaml.YAMLError as exc:
            print(exc)
    return config
