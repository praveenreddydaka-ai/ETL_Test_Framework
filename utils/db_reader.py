import pandas as pd
import mysql.connector

def read_db(query, creds_file, db_type):

    creds_df = pd.read_excel(creds_file)
    creds = creds_df[creds_df['database_type'] == db_type]

    if creds.empty:
        raise Exception(f"No credentials found for {db_type}")

    creds = creds.iloc[0]

    conn = mysql.connector.connect(
        host=creds['host'],
        port=int(creds['port']),
        user=creds['username'],
        password=creds['password'],
        database=creds['database']
    )

    df = pd.read_sql(query, conn)
    return df
