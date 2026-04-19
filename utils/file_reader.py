import pandas as pd

def read_file(path, file_type):
    file_type = file_type.lower()

    if file_type == 'csv':
        return pd.read_csv(path)
    elif file_type in ['xlsx', 'excel']:
        return pd.read_excel(path)
    elif file_type == 'parquet':
        return pd.read_parquet(path)
    elif file_type in ['txt', 'text']:
        return pd.read_csv(path, sep=',')
    elif file_type == 'json':
        return pd.read_json(path)
    else:
        raise Exception(f"Unsupported file type: {file_type}")
