import pandas as pd

import logging
logger = logging.getLogger("ETLFramework")

def read_file(path, file_type):

    try:
        logger.info(f"Reading file: {path} | Type: {file_type}")

        file_type = file_type.lower()

        if file_type == 'csv':
            df = pd.read_csv(path)

        elif file_type in ['xlsx', 'excel']:
            df = pd.read_excel(path)

        elif file_type == 'parquet':
            df = pd.read_parquet(path)

        elif file_type in ['txt', 'text']:
            df = pd.read_csv(path, sep=',')

        elif file_type == 'json':
            df = pd.read_json(path)

        else:
            logger.error(f"Unsupported file type: {file_type}")
            raise Exception(f"Unsupported file type: {file_type}")

        logger.info(f"File read successful. Rows: {df.shape[0]}")

        return df

    except Exception as e:
        logger.error(f"Error reading file: {e}")
        raise