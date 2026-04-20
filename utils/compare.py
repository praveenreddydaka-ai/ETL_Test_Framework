from utils.Helper_functions import normalize_df
import logging

logger = logging.getLogger("ETLFramework")

# ==============================
# ROW COUNT CHECK
# ==============================

def check_row_count(src_df, tgt_df):

    logger.info("Row Count Check for SRC and TGT Started")

    if src_df.shape[0] == tgt_df.shape[0]:
        logger.info(f"Row Count Match: {src_df.shape[0]}")
        return True
    else:
        logger.info(f" Row Count Mismatch: Src={src_df.shape[0]}, Tgt={tgt_df.shape[0]}")
        return False


# ==============================
# COLUMN SCHEMA CHECK
# ==============================

def check_column_schema(src_df, tgt_df):

    logger.info("Starting Column Schema Check")

    src_cols = list(src_df.columns)
    tgt_cols = list(tgt_df.columns)

    if src_cols == tgt_cols:
        logger.info("Column Names Match")
        return True
    else:
        logger.error("Column Names Mismatch")
        logger.error(f"Source Columns: {src_cols}")
        logger.error(f"Target Columns: {tgt_cols}")
        return False

# ==============================
# DATA COMPARISON
# ==============================

def compare_data(src_df, tgt_df):

    logger.info("Starting Data Comparison")

    try:
        src_df = normalize_df(src_df)
        tgt_df = normalize_df(tgt_df)

        src_sorted = src_df.sort_values(by=list(src_df.columns)).reset_index(drop=True)
        tgt_sorted = tgt_df.sort_values(by=list(tgt_df.columns)).reset_index(drop=True)

        if src_sorted.equals(tgt_sorted):
            logger.info("Data Matches Successfully")
            return True
        else:
            logger.error("Data Mismatch Found")

            # 🔥 Optional: log sample difference
            logger.info("Logging sample mismatched rows (first 5)")

            diff = src_sorted.compare(tgt_sorted)
            logger.info(f"\n{diff.head()}")

            return False

    except Exception as e:
        logger.error(f"Error during data comparison: {e}")
        return False

#============================================
#DUPLICATE CHECKS IN SOURCE AND TARGET
#============================================
def check_duplicates(df, key_cols, df_name="Data"):

    logger.info(f"Starting Duplicate Check on {df_name}")

    # Step 1: Check if key provided
    if not key_cols:
        logger.warning("No primary key provided, skipping duplicate check")
        return True

    # Step 2: Convert string → list
    key_cols = [col.strip() for col in key_cols.split(",")]

    # Step 3: Validate columns exist
    missing_cols = [col for col in key_cols if col not in df.columns]
    if missing_cols:
        logger.error(f"Columns not found in {df_name}: {missing_cols}")
        return False

    # Step 4: Find duplicates
    dup_df = df[df.duplicated(subset=key_cols, keep=False)]

    # Step 5: Result
    if dup_df.empty:
        logger.info(f"No duplicates found in {df_name}")
        return True
    else:
        logger.error(f"Duplicates found in {df_name} on {key_cols}")
        logger.info(f"\n{dup_df.head()}")
        return False