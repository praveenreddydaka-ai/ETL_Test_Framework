import pandas as pd
import mysql.connector

import logging
logger = logging.getLogger("ETLFramework")

def read_db(query, creds_file, db_type):

    try:
        logger.info(f"Reading DB for type: {db_type}")

        creds_df = pd.read_excel(creds_file)

        creds = creds_df[creds_df['database_type'] == db_type]

        if creds.empty:
            logger.error(f"No credentials found for {db_type}")
            raise Exception(f"No credentials found for {db_type}")

        creds = creds.iloc[0]

        logger.info(f"Connecting to DB: {creds['database']}")

        conn = mysql.connector.connect(
            host=creds['host'],
            port=int(creds['port']),
            user=creds['username'],
            password=creds['password'],
            database=creds['database']
        )

        logger.info("Connection successful")

        df = pd.read_sql(query, conn)

        logger.info(f"Query executed successfully. Rows fetched: {df.shape[0]}")

        return df

    except Exception as e:
        logger.error(f"Error in DB Read: {e}")
        raise
