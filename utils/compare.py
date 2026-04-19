from utils.Helper_functions import normalize_df

def compare_dataframes(src_df, tgt_df):

    print("\n--- BASIC RECONCILIATION STARTED ---")

    # 1. Row count check
    if src_df.shape[0] == tgt_df.shape[0]:
        print("✅ Row Count Match")
    else:
        print(f"❌ Row Count Mismatch: Src={src_df.shape[0]}, Tgt={tgt_df.shape[0]}")

    # 2. Column check
    if list(src_df.columns) == list(tgt_df.columns):
        print("✅ Column Names Match")
    else:
        print("❌ Column Names Mismatch")
        print("Source Columns:", list(src_df.columns))
        print("Target Columns:", list(tgt_df.columns))
        return  # stop further comparison if schema mismatch

    # 🔥 3. Sort before compare (NEW)
    try:
        # ✅ Normalize both dataframes
        src_df = normalize_df(src_df)
        tgt_df = normalize_df(tgt_df)
        src_sorted = src_df.sort_values(by=list(src_df.columns)).reset_index(drop=True)
        tgt_sorted = tgt_df.sort_values(by=list(tgt_df.columns)).reset_index(drop=True)

        if src_sorted.equals(tgt_sorted):
            print("✅ Data Matches (After Sorting)")
        else:
            print("❌ Data Mismatch Found")

    except Exception as e:
        print("⚠️ Error during sorted comparison:", e)

    print("--- RECONCILIATION COMPLETED ---\n")

