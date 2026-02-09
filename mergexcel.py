import boto3
import pandas as pd
import os
from io import BytesIO
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# AWS credentials from environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')

# Initialize S3 client with credentials
if aws_access_key_id and aws_secret_access_key:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
else:
    # Fallback to default credentials (IAM role, AWS CLI config, etc.)
    s3_client = boto3.client('s3', region_name=aws_region)

# Bucket name and folder paths from environment variables
BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'mbf-los-stg')
BASE_FOLDER = os.getenv('S3_BASE_FOLDER', 'bank_data/')

def get_latest_xlsx_files_from_s3(bucket_name, base_folder):
    """
    Get the latest .xlsx files from the bank subfolders in S3.
    """
    latest_files = {}

    # List all objects in the base folder (bank_data)
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=base_folder)

    # Traverse the objects in the folder
    for obj in response.get('Contents', []):
        key = obj['Key']

        if key.endswith('.xlsx'):  # Process only .xlsx files
            bank_name = key.split('/')[1]  # Bank folder is the second part of the path

            # Keep track of the latest .xlsx file for each bank
            if bank_name not in latest_files:
                latest_files[bank_name] = key
            else:
                # Compare timestamps (last_modified) and pick the latest
                if obj['LastModified'] > s3_client.head_object(Bucket=bucket_name, Key=latest_files[bank_name])['LastModified']:
                    latest_files[bank_name] = key

    return latest_files

def find_column_by_variations(df, target_name, variations):
    """
    Find a column in DataFrame by checking multiple variations (case-insensitive).
    Returns the actual column name if found, None otherwise.
    """
    # Create a mapping of lowercase column names to actual column names
    col_map = {col.lower().strip(): col for col in df.columns}

    # Check each variation
    for variation in variations:
        if variation.lower() in col_map:
            return col_map[variation.lower()]

    # Also check if target_name already exists
    if target_name.lower() in col_map:
        return col_map[target_name.lower()]

    return None

def find_branch_name_column_aggressive(df):
    """
    Aggressively search for branch name column using multiple strategies.
    Returns the column name if found, None otherwise.
    """
    # Strategy 1: Check all known variations
    branch_variations = [
        'Branch Name', 'Branch', 'BRANCH NAME', 'BRANCH', 'branch name', 'branch',
        'BranchName', 'BRANCHNAME', 'Branch_Name', 'BRANCH_NAME', 'branch_name',
        'Branch  Name', 'BRANCH  NAME', 'Branch  name', 'BRANCH  NAME',
        'BranchName', 'BRANCHNAME', 'Branch Name', 'BRANCH NAME',
        'Branch Details', 'BRANCH DETAILS', 'Branch Details', 'branch details',
        'Branch Location', 'BRANCH LOCATION', 'Branch Location', 'branch location',
        'Branch Office', 'BRANCH OFFICE', 'Branch Office', 'branch office',
        'Office Name', 'OFFICE NAME', 'Office Name', 'office name',
        'Office', 'OFFICE', 'office',
        'Location', 'LOCATION', 'location',
        'Branch Address', 'BRANCH ADDRESS', 'Branch Address', 'branch address'
    ]

    found_col = find_column_by_variations(df, 'Branch Name', branch_variations)
    if found_col:
        return found_col

    # Strategy 2: Search for columns containing "branch" anywhere in the name (case-insensitive)
    for col in df.columns:
        col_lower = col.lower().strip()
        if 'branch' in col_lower and col_lower != 'bank name':
            # Check if this column has actual data (not all empty)
            non_empty_count = df[col].astype(str).str.strip().ne('').sum()
            if non_empty_count > 0:
                print(f"  Found potential branch column '{col}' with {non_empty_count} non-empty values")
                return col

    # Strategy 3: Search for columns containing "office" or "location" (common alternatives)
    for col in df.columns:
        col_lower = col.lower().strip()
        if any(keyword in col_lower for keyword in ['office', 'location', 'details']) and col_lower != 'bank name':
            non_empty_count = df[col].astype(str).str.strip().ne('').sum()
            if non_empty_count > 0:
                print(f"  Found potential branch column '{col}' (contains office/location/details) with {non_empty_count} non-empty values")
                return col

    return None

def read_xlsx_from_s3(bucket_name, key, bank_name):
    """
    Read an .xlsx file from S3 and return a pandas DataFrame with standardized column names.
    Preserves ALL columns from source file - no data is dropped.
    Returns: (dataframe, original_row_count)
    """
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    file_stream = BytesIO(response['Body'].read())

    # Read Excel file - keep ALL rows, don't skip any
    # Important parameters to preserve all data:
    # - keep_default_na=False: Don't convert empty cells to NaN
    # - na_values=[]: Don't treat any values as NaN
    # - header=0: Use first row as header (default)
    # - skiprows=None: Don't skip any rows
    df = pd.read_excel(file_stream, keep_default_na=False, na_values=[], header=0)

    # Get original row count immediately after reading
    original_row_count = len(df)
    print(f"  📊 ORIGINAL ROW COUNT: {original_row_count} rows")

    # Standardize the column names by stripping any extra spaces
    df.columns = df.columns.str.strip()

    print(f"  Original columns in file: {list(df.columns)}")

    # Find and standardize 'Bank Name' column
    bank_name_variations = ['Bank Name', 'Bank', 'BANK NAME', 'BANK', 'bank name', 'bank', 'BankName', 'BANKNAME']
    bank_col = find_column_by_variations(df, 'Bank Name', bank_name_variations)

    if bank_col and bank_col != 'Bank Name':
        df.rename(columns={bank_col: 'Bank Name'}, inplace=True)
        print(f"  Renamed '{bank_col}' to 'Bank Name'")
    elif 'Bank Name' not in df.columns:
        df['Bank Name'] = bank_name  # Add bank name from folder name
        print(f"  Added 'Bank Name' column with value: {bank_name}")

    # Find and standardize 'Branch Name' column - use aggressive search
    branch_col = find_branch_name_column_aggressive(df)

    if branch_col and branch_col != 'Branch Name':
        # Check if the found column has actual data
        non_empty_before = df[branch_col].astype(str).str.strip().ne('').sum()
        df.rename(columns={branch_col: 'Branch Name'}, inplace=True)
        print(f"  ✓ Found and renamed '{branch_col}' to 'Branch Name' ({non_empty_before} non-empty values)")
    elif 'Branch Name' not in df.columns:
        # Last resort: Check if any column might contain branch data
        print(f"  ⚠ Branch Name column not found. Checking all columns for potential branch data...")

        # Show all columns and their data counts
        for col in df.columns:
            if col.lower() not in ['bank name', 'ifsc', 'address', 'city', 'city2', 'state', 'std code', 'phone']:
                non_empty = df[col].astype(str).str.strip().ne('').sum()
                if non_empty > 0:
                    print(f"    - Column '{col}': {non_empty} non-empty values (might contain branch data)")

        df['Branch Name'] = ''  # Add empty branch name column
        print(f"  ⚠ Added empty 'Branch Name' column - NO BRANCH DATA FOUND!")
    else:
        # Branch Name already exists, verify it has data
        non_empty = df['Branch Name'].astype(str).str.strip().ne('').sum()
        if non_empty == 0:
            print(f"  ⚠ WARNING: 'Branch Name' column exists but is EMPTY! Searching for alternative...")
            # Try to find alternative column
            alt_col = find_branch_name_column_aggressive(df)
            if alt_col and alt_col != 'Branch Name':
                # Copy data from alternative column
                non_empty_alt = df[alt_col].astype(str).str.strip().ne('').sum()
                if non_empty_alt > 0:
                    df['Branch Name'] = df[alt_col].astype(str)
                    print(f"  ✓ Copied data from '{alt_col}' to 'Branch Name' ({non_empty_alt} values)")
                else:
                    print(f"  ⚠ Alternative column '{alt_col}' also empty")
        else:
            print(f"  ✓ 'Branch Name' column found with {non_empty} non-empty values")

    # Find and standardize 'IFSC' column
    ifsc_variations = ['IFSC', 'ifsc', 'Ifsc', 'IFSC CODE', 'ifsc code', 'IFSC_CODE', 'ifsc_code', 'IFSC Code']
    ifsc_col = find_column_by_variations(df, 'IFSC', ifsc_variations)

    if ifsc_col and ifsc_col != 'IFSC':
        df.rename(columns={ifsc_col: 'IFSC'}, inplace=True)
        print(f"  Renamed '{ifsc_col}' to 'IFSC'")
    elif 'IFSC' not in df.columns:
        df['IFSC'] = ''  # Add empty IFSC column
        print(f"  Added empty 'IFSC' column")

    # Standardize other common columns (case-insensitive)
    column_mappings = {
        'ADDRESS': ['ADDRESS', 'Address', 'address', 'ADDR', 'Addr', 'addr'],
        'CITY': ['CITY', 'City', 'city'],
        'CITY2': ['CITY2', 'City2', 'city2', 'CITY 2', 'City 2'],
        'STATE': ['STATE', 'State', 'state'],
        'STD CODE': ['STD CODE', 'Std Code', 'std code', 'STDCODE', 'StdCode', 'STD_CODE', 'std_code'],
        'PHONE': ['PHONE', 'Phone', 'phone', 'PHONE NUMBER', 'Phone Number', 'phone number', 'PHONENUMBER', 'PhoneNumber']
    }

    for standard_name, variations in column_mappings.items():
        found_col = find_column_by_variations(df, standard_name, variations)
        if found_col and found_col != standard_name:
            df.rename(columns={found_col: standard_name}, inplace=True)
            print(f"  Renamed '{found_col}' to '{standard_name}'")
        elif standard_name not in df.columns:
            df[standard_name] = ''  # Add missing column with empty values
            print(f"  Added empty '{standard_name}' column")

    # Clean 'IFSC' column: Remove extra spaces, non-printing characters, and ensure it's treated as a string
    if 'IFSC' in df.columns:
        df['IFSC'] = df['IFSC'].apply(lambda x: str(x).strip() if pd.notnull(x) else '').astype(str)

    # Clean 'Branch Name' column: Remove extra spaces and handle NaN values
    if 'Branch Name' in df.columns:
        # Convert to string first, then clean
        df['Branch Name'] = df['Branch Name'].astype(str)
        df['Branch Name'] = df['Branch Name'].apply(lambda x: x.strip() if x and x != 'nan' else '')
        # Replace 'nan' strings with empty
        df['Branch Name'] = df['Branch Name'].replace(['nan', 'None', 'none', 'NULL', 'null'], '')

        # Final verification - show branch name statistics
        non_empty_branch = df['Branch Name'].str.strip().ne('').sum()
        total_rows = len(df)
        if non_empty_branch > 0:
            print(f"  ✓ Branch Name: {non_empty_branch}/{total_rows} rows have data")
        else:
            print(f"  ⚠ WARNING: Branch Name is EMPTY for all {total_rows} rows!")

    # Clean 'Bank Name' column: Remove extra spaces and handle NaN values
    if 'Bank Name' in df.columns:
        df['Bank Name'] = df['Bank Name'].apply(lambda x: str(x).strip() if pd.notnull(x) else '').astype(str)
        # Fill any empty bank names with the bank_name parameter
        df['Bank Name'] = df['Bank Name'].replace('', bank_name)

    # Fill all NaN values with empty strings to prevent data loss
    # Use fillna with inplace=False to avoid dropping rows
    df = df.fillna('')

    # Convert all columns to string type to prevent type mismatches during merge
    # Do this carefully to preserve all rows
    for col in df.columns:
        # Convert to string, handling NaN properly
        df[col] = df[col].astype(str)
        # Replace 'nan' string (from NaN conversion) with empty string
        df[col] = df[col].replace(['nan', 'None', 'none', 'NULL', 'null', 'NaT'], '')

    # Final row count check
    final_row_count = len(df)
    print(f"  Final columns: {list(df.columns)}")
    print(f"  📊 FINAL ROW COUNT: {final_row_count} rows")

    # Verify no rows were lost
    if final_row_count != original_row_count:
        print(f"  ⚠⚠⚠ WARNING: ROW COUNT MISMATCH! Original: {original_row_count}, Final: {final_row_count}")
        print(f"  ⚠⚠⚠ LOST {original_row_count - final_row_count} ROWS DURING PROCESSING!")
    else:
        print(f"  ✓ Row count verified: {final_row_count} rows (no rows lost)")

    return df, original_row_count



def merge_and_save_xlsx(bucket_name, base_folder, output_file):
    """
    Merges the latest .xlsx files from all bank subfolders and saves as a single .xlsx file.
    Preserves ALL columns from all source files - no data is lost.
    """
    latest_files = get_latest_xlsx_files_from_s3(bucket_name, base_folder)

    if not latest_files:
        print("No Excel files found in S3 bucket!")
        return

    print(f"\nFound {len(latest_files)} bank files to process:\n")
    for bank_name, file_key in latest_files.items():
        print(f"  - {bank_name}: {file_key}")

    all_dataframes = []
    all_columns = set()
    file_row_counts = {}  # Track row counts per file
    total_input_rows = 0  # Total rows from all input files

    # First pass: Read all files and collect all unique columns
    print("\n" + "="*60)
    print("STEP 1: Reading and standardizing all files...")
    print("="*60)

    for bank_name, file_key in latest_files.items():
        print(f"\nProcessing file: {file_key} for bank: {bank_name}")

        try:
            # Read each file into a DataFrame
            df, original_row_count = read_xlsx_from_s3(bucket_name, file_key, bank_name)

            # Track row counts
            file_row_counts[bank_name] = {
                'original': original_row_count,
                'file_path': file_key
            }
            total_input_rows += original_row_count

            # Collect all columns from this file
            all_columns.update(df.columns)
            all_dataframes.append(df)

            # Verify critical columns exist and have data
            critical_cols = ['Bank Name', 'Branch Name', 'IFSC']
            missing_critical = [col for col in critical_cols if col not in df.columns]
            if missing_critical:
                print(f"  ⚠ WARNING: Missing critical columns: {missing_critical}")

            # Check if Branch Name has data
            if 'Branch Name' in df.columns:
                branch_data_count = df['Branch Name'].astype(str).str.strip().ne('').sum()
                total_rows = len(df)
                if branch_data_count == 0:
                    print(f"  ⚠ CRITICAL: Branch Name column exists but is EMPTY for all {total_rows} rows!")
                elif branch_data_count < total_rows:
                    print(f"  ⚠ WARNING: Branch Name has data for only {branch_data_count}/{total_rows} rows")
                else:
                    print(f"  ✓ Branch Name: {branch_data_count}/{total_rows} rows have data")

        except Exception as e:
            print(f"  ERROR processing {file_key}: {str(e)}")
            continue

    if not all_dataframes:
        print("\nNo files were successfully processed!")
        return

    # Second pass: Ensure all DataFrames have all columns (fill missing with empty strings)
    print("\n" + "="*60)
    print("STEP 2: Aligning columns across all files...")
    print("="*60)

    all_columns = sorted(list(all_columns))
    print(f"\nTotal unique columns found: {len(all_columns)}")
    print(f"Columns: {all_columns}")

    standardized_dfs = []
    for i, df in enumerate(all_dataframes):
        rows_before = len(df)

        # Add any missing columns with empty strings
        missing_cols = set(all_columns) - set(df.columns)
        if missing_cols:
            print(f"  File {i+1}: Adding {len(missing_cols)} missing columns")
            for col in missing_cols:
                df[col] = ''

        # Reorder columns to match the standard order
        # Put critical columns first, then others
        critical_cols = ['Bank Name', 'Branch Name', 'IFSC']
        other_cols = [col for col in all_columns if col not in critical_cols]
        column_order = [col for col in critical_cols if col in all_columns] + other_cols

        df = df[column_order]

        rows_after = len(df)
        if rows_before != rows_after:
            print(f"  ⚠⚠⚠ WARNING: File {i+1} lost rows during standardization! Before: {rows_before}, After: {rows_after}")
        else:
            print(f"  ✓ File {i+1}: {rows_after} rows (no rows lost during standardization)")

        standardized_dfs.append(df)

    # Merge all DataFrames
    print("\n" + "="*60)
    print("STEP 3: Merging all data...")
    print("="*60)

    # Count rows before merge
    total_rows_before_merge = sum(len(df) for df in standardized_dfs)
    print(f"\nTotal rows in all DataFrames before merge: {total_rows_before_merge}")

    # Merge with explicit parameters to ensure no data loss
    merged_df = pd.concat(standardized_dfs, ignore_index=True, sort=False)

    total_output_rows = len(merged_df)
    print(f"Total rows in merged DataFrame: {total_output_rows}")

    # Verify row count
    if total_output_rows != total_rows_before_merge:
        lost_rows = total_rows_before_merge - total_output_rows
        print(f"\n⚠⚠⚠ CRITICAL ERROR: ROWS LOST DURING MERGE!")
        print(f"⚠⚠⚠ Expected: {total_rows_before_merge} rows")
        print(f"⚠⚠⚠ Got: {total_output_rows} rows")
        print(f"⚠⚠⚠ LOST: {lost_rows} rows")
    else:
        print(f"\n✓ Merge successful: All {total_output_rows} rows preserved")

    print(f"\nMerged DataFrame Statistics:")
    print(f"  Total rows: {total_output_rows}")
    print(f"  Total columns: {len(merged_df.columns)}")
    print(f"  Columns: {list(merged_df.columns)}")

    # Verify critical data
    print(f"\nData Verification:")
    print(f"  Rows with Bank Name: {merged_df['Bank Name'].astype(str).str.strip().ne('').sum()}")
    print(f"  Rows with Branch Name: {merged_df['Branch Name'].astype(str).str.strip().ne('').sum()}")
    print(f"  Rows with IFSC: {merged_df['IFSC'].astype(str).str.strip().ne('').sum()}")

    # Check for any completely empty rows
    empty_rows = merged_df.isnull().all(axis=1).sum()
    if empty_rows > 0:
        print(f"  WARNING: Found {empty_rows} completely empty rows")

    # ========== CRITICAL: ROW COUNT VERIFICATION BEFORE SAVING ==========
    print("\n" + "="*70)
    print("="*70)
    print("CRITICAL ROW COUNT VERIFICATION - BEFORE SAVING")
    print("="*70)
    print("="*70)

    print(f"\n📊 INPUT FILES ROW COUNTS:")
    print("-" * 70)
    for bank_name, counts in file_row_counts.items():
        print(f"  {bank_name:30s}: {counts['original']:6d} rows")
    print("-" * 70)
    print(f"  {'TOTAL INPUT ROWS':30s}: {total_input_rows:6d} rows")

    print(f"\n📊 OUTPUT FILE ROW COUNT:")
    print("-" * 70)
    print(f"  {'MERGED FILE ROWS':30s}: {total_output_rows:6d} rows")
    print("-" * 70)

    print(f"\n📊 VERIFICATION:")
    print("-" * 70)

    # CRITICAL CHECK: Only proceed if row counts match
    if total_output_rows != total_input_rows:
        lost_rows = total_input_rows - total_output_rows
        print(f"  ❌❌❌ CRITICAL ERROR: ROW COUNT MISMATCH!")
        print(f"  ❌❌❌ Input: {total_input_rows} rows")
        print(f"  ❌❌❌ Output: {total_output_rows} rows")
        print(f"  ❌❌❌ LOST: {lost_rows} rows ({lost_rows/total_input_rows*100:.2f}%)")
        print("-" * 70)
        print(f"\n🚫🚫🚫 ABORTING FILE SAVE - DATA LOSS DETECTED!")
        print(f"🚫🚫🚫 The merged file will NOT be saved to S3.")
        print(f"🚫🚫🚫 Please review the code and fix the data loss issue.")
        print(f"🚫🚫🚫 Expected {total_input_rows} rows but got {total_output_rows} rows.")
        print("="*70)
        return  # Exit function without saving

    # Row counts match - proceed with saving
    print(f"  ✓✓✓ SUCCESS: All rows preserved!")
    print(f"  ✓✓✓ Input: {total_input_rows} rows = Output: {total_output_rows} rows")
    print(f"  ✓✓✓ NO DATA LOSS - Proceeding with file save...")
    print("-" * 70)
    print("="*70)

    # Save the merged DataFrame to a new file (only if row counts match)
    print("\n" + "="*60)
    print("STEP 4: Saving merged file...")
    print("="*60)

    try:
        with BytesIO() as output:
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                merged_df.to_excel(writer, index=False, sheet_name='Merged Data')
            output.seek(0)

            # Upload the merged file back to S3
            s3_client.upload_fileobj(output, bucket_name, output_file)

            # Final success message
            print("\n" + "="*70)
            print("="*70)
            print("FILE SAVED SUCCESSFULLY")
            print("="*70)
            print("="*70)
            print(f"\n✓ Successfully saved merged file to: s3://{bucket_name}/{output_file}")
            print(f"  Total rows in saved file: {total_output_rows}")
            print(f"  Total columns in saved file: {len(merged_df.columns)}")
            print(f"  ✓ Row count verified: {total_input_rows} input rows = {total_output_rows} output rows")
            print("="*70)

    except Exception as e:
        print(f"\n❌ ERROR: Failed to save file to S3: {str(e)}")
        print(f"  File was not saved due to error.")
        raise

# Run the merge process
if __name__ == "__main__":
    # Output file name for the merged data from environment variable
    OUTPUT_FILE = os.getenv('S3_OUTPUT_FILE', 'merged_bank_data.xlsx')

    # Merge the files and save to S3
    merge_and_save_xlsx(BUCKET_NAME, BASE_FOLDER, OUTPUT_FILE)
