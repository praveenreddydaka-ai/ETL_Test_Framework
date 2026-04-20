import pandas as pd
from utils.file_reader import read_file
from utils.db_reader import read_db
from utils.compare import check_row_count,check_column_schema,compare_data,check_duplicates
from utils.logger import setup_logger

logger = setup_logger()

TEST_CASE_FILE = "config/test_cases.xlsx"
CREDS_FILE = "config/db_creds.xlsx"

def process_test_case(row):

    logger.info("Processing Test Case")

    if row['source_type'].lower() == 'database':
        logger.info("Reading Source from DB")
        src_df = read_db(row['source_query'], CREDS_FILE, row['src_db_type'])
    else:
        logger.info("Reading Source from File...")
        src_df = read_file(row['source_path'], row['source_type'])

    if row['target_type'].lower() == 'database':
        logger.info("Reading Target from DB...")
        tgt_df = read_db(row['target_query'], CREDS_FILE, row['tgt_db_type'])
    else:
        logger.info("Reading Target from File...")
        tgt_df = read_file(row['target_path'], row['target_type'])

    logger.info(f"Source DF Shape: {src_df.shape}")
    logger.info(f"Target DF Shape: {tgt_df.shape}")

    return src_df, tgt_df

def main():

    test_df = pd.read_excel(TEST_CASE_FILE)

    for index, row in test_df.iterrows():

        if row['execution_ind'] != 'Y':
            continue

        try:
            src_df, tgt_df = process_test_case(row)

            pk_cols = row.get("primary_key", None)

            # 🔥 Validations

            #1.ROW COUNT VALIDATION
            check_row_count(src_df, tgt_df)

            #2.COLUMN SCHEMA VALIDATION
            check_column_schema(tgt_df, src_df)

            logger.info("Primary Key Columns: %s", pk_cols)

            #3 DUPLICATES CHECK
            src_dup_status = check_duplicates(src_df, pk_cols, "Source")
            tgt_dup_status = check_duplicates(tgt_df, pk_cols, "Target")

            # 4 DATA COMPARISON
            compare_data(src_df, tgt_df)

        except Exception as e:
            logger.error(f"Error in test case {index}: {e}")

if __name__ == "__main__":
    main()
