import json
import logging
from typing import List

import pandas as pd

from postgre_etl.postgres_init import postgres_init
from utils.custom_logger import ensure_basic_logging
from utils.utils import run_postgre_query

LOG_FORMAT = " %(asctime)s %(levelname)-8s : %(name)s %(message)s"
logger = logging.getLogger("ETL_Pipeline_main")


@ensure_basic_logging(level=logging.INFO, format=LOG_FORMAT)
class PostgresEtl:
    def __init__(self):
        self.conn = postgres_init()
        self.cursor = self.conn.cursor()

    def create_prerequisites_table(self):
        create_table_sql_file = "./modelling/create_table.sql"
        with self.cursor:
            try:
                logger.info(f"running sql file :: {create_table_sql_file}")
                self.cursor.execute(open(create_table_sql_file, "r").read())
            except (TypeError, Exception) as err:
                self.conn.rollback()
                self.cursor.close()
                raise err

    def write_org_data(self, data: dict):
        """Write a row of organization data to db."""

        org_query = '''insert into events.org_events (''' + ','.join(list(data.keys())) + ''') values ''' + str(
            tuple(data.values()))
        run_postgre_query(self.conn, self.cursor, org_query)

    def upsert_user_details(self, data: dict):
        """Write a row of user data to db."""
        if not data["username"] or data['id']:
            df = pd.DataFrame(data,
                              columns=['id', 'event_type', 'username', 'user_email', 'user_type', 'organization_name',
                                       'received_at'])

            upsert_query = (f""" 
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
            run_postgre_query(self.conn, self.cursor, upsert_query)
        else:
            logger.error(f"skipped row: {data}")

    def get_user(self, username: str) -> List[dict]:
        """Joins both tables and returns user by name.
        response
        {
            "username": "Snake",
            "user_type": "Admin",
            "organization_name": "Metal Gear Solid",
            "organization_tier": "Medium"
        }
        """
        data = self.cur.execute(
            f"""
            SELECT
                username,
                user_type,
                user_events.organization_name,
                organization_tier
            FROM user_data 
            JOIN org_events 
            ON user_events.organization_name = org_events.organization_name 
            WHERE user_events.username LIKE '{username}';
            """
        ).fetchall()
        response = []
        for d in data:
            response.append({
                "username": d[0],
                "user_type": d[1],
                "organization_name": d[2],
                "organization_tier": d[3]
            })
        return response


if __name__ == "__main__":
    # Debugging
    test_db = PostgresEtl()
    test_db.create_prerequisites_table()
    org_data_sample = """
    {
        "organization_key": "ff3959a49ac10fc70181bc00e308fbeb",
        "organization_name": "Metal Gear Solid",
        "organization_tier": "Medium",
        "created_at": "2018-01-24 17:28:09.000000"
    }
    """
    test_db.write_org_data(json.loads(org_data_sample))
    user_event_data = """
    {
        "id": "069feb770fe581acc9d3313d59780196",
        "username": "Snake",
        "user_email": "snake@outerheaven.com",
        "user_type": "Admin",
        "organization_name": "Metal Gear Solid",
        "received_at": "2020-12-08 20:03:16.759617"
    }
    """
    test_db.upsert_user_details(json.loads(user_event_data))

    test_db.get_user("Snake")
