import pandas as pd
from utils.file_reader import read_file
from utils.db_reader import read_db
from utils.compare import compare_dataframes

TEST_CASE_FILE = "config/test_cases.xlsx"
CREDS_FILE = "config/db_creds.xlsx"

def process_test_case(row):

    print("\nProcessing Test Case...\n")

    if row['source_type'].lower() == 'database':
        print("Reading Source from DB...")
        src_df = read_db(row['source_query'], CREDS_FILE, row['src_db_type'])
    else:
        print("Reading Source from File...")
        src_df = read_file(row['source_path'], row['source_type'])

    if row['target_type'].lower() == 'database':
        print("Reading Target from DB...")
        tgt_df = read_db(row['target_query'], CREDS_FILE, row['tgt_db_type'])
    else:
        print("Reading Target from File...")
        tgt_df = read_file(row['target_path'], row['target_type'])

    print("Source DF Shape:", src_df.shape)
    print("Target DF Shape:", tgt_df.shape)

    return src_df, tgt_df

def main():

    test_df = pd.read_excel(TEST_CASE_FILE)

    for index, row in test_df.iterrows():

        if row['execution_ind'] != 'Y':
            continue

        try:
            src_df, tgt_df = process_test_case(row)

            # 🔥 Validations
            compare_dataframes(src_df, tgt_df)

        except Exception as e:
            print(f"Error in test case {index}: {e}")

if __name__ == "__main__":
    main()
