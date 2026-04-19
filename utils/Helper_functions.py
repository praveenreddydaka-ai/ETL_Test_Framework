def normalize_df(df):
    # Convert all numeric columns to float for consistency
    for col in df.columns:
        if df[col].dtype in ['int64', 'float64']:
            df[col] = df[col].astype(float)

    return df