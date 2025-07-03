import pandas as pd
import os
import sys

# Define the essential columns at the top for clarity and reuse.
ID_VARS = ['date', 'users_active', 'total_sales', 'new_customers', 'report_generated']
FINAL_COLUMNS = ID_VARS + ['region', 'regional_sales', 'product_id']

def is_wide_format(df):
    """Checks if the dataframe is in the wide format (contains 'col_6')."""
    return 'col_6' in df.columns

def validate_schema(df):
    """Checks for essential columns and adds them with defaults if missing."""
    print("1. Validating initial schema")
    
    for col in ID_VARS:
        if col not in df.columns:
            print(f"WARNING: Missing required column '{col}'. Creating it with default value 0.")
            df[col] = 0
            
    print("Initial schema validation passed.")
    return df

def unpivot_data(df):
    """
    Transforms messy, wide-format data into a clean, long format.
    This function intelligently handles misaligned data by identifying the type of
    data in each cell rather than assuming a fixed pattern.
    """
    print("2. Unpivoting data")
    
    id_vars_present = [col for col in ID_VARS if col in df.columns]
    regional_cols = [col for col in df.columns if col.startswith('col_')]

    processed_records = []
    for index, row in df.iterrows():
        base_data = row[id_vars_present].to_dict()
        
        # This list will temporarily hold parts of a regional record
        current_record_parts = {}

        for col_name in regional_cols:
            value = row[col_name]
            if pd.isna(value):
                continue

            # Intelligently determine what the value represents
            if isinstance(value, str):
                # If we find a new region name, save the previous record if it's complete
                if 'region' in current_record_parts:
                    record = base_data.copy()
                    record.update(current_record_parts)
                    processed_records.append(record)
                # Start a new record
                current_record_parts = {'region': value}
            elif 'region' in current_record_parts:
                # If we have a region, the next number is sales
                if 'regional_sales' not in current_record_parts:
                    current_record_parts['regional_sales'] = value
                # The number after that is the product_id
                elif 'product_id' not in current_record_parts:
                    current_record_parts['product_id'] = value

        # Add the last record from the row if it exists
        if 'region' in current_record_parts:
            record = base_data.copy()
            record.update(current_record_parts)
            processed_records.append(record)

    df_processed = pd.DataFrame(processed_records)
    print(f"Successfully unpivoted data into {len(df_processed)} records.")
    return df_processed

def clean_and_validate_data(df_in):
    """
    Detects, reports, and handles all data quality issues for any file format.
    """
    print("3. Cleaning and validating data types")

    if df_in.empty:
        print("WARNING: DataFrame is empty. No data to clean.")
        return df_in
    
    # Create an explicit copy to prevent SettingWithCopyWarning.
    df = df_in.copy()

    # --- Date Validation ---
    invalid_date_mask = pd.to_datetime(df['date'], errors='coerce').isnull()
    if invalid_date_mask.any():
        print(f"\nWARNING: Found {invalid_date_mask.sum()} rows with invalid date formats. These rows will be dropped.")
        print("Sample of invalid date rows (up to 10):")
        print(df[invalid_date_mask].head(10))
        df = df[~invalid_date_mask]

    # --- Categorical Validation (Region) ---
    # We define valid regions as the most common string values found in the column
    if 'region' in df.columns and not df['region'].empty:
        valid_regions = set(df['region'].value_counts().nlargest(4).index)
        invalid_region_mask = ~df['region'].isin(valid_regions) & df['region'].notna()
        
        if invalid_region_mask.any():
            print(f"\nWARNING: Found {invalid_region_mask.sum()} rows with unexpected region names. These will be corrected to 'Unknown'.")
            print("Sample of invalid region rows (up to 10):")
            print(df[invalid_region_mask].head(10))
            df.loc[invalid_region_mask, 'region'] = 'Unknown'

    # --- Numeric Validation ---
    numeric_cols = ['users_active', 'total_sales', 'new_customers', 'regional_sales', 'product_id']
    for col in numeric_cols:
        if col in df.columns:
            is_not_numeric = pd.to_numeric(df[col], errors='coerce').isnull()
            was_not_null = df[col].notna()
            corrupted_rows = df[is_not_numeric & was_not_null]

            if not corrupted_rows.empty:
                print(f"\nWARNING: Found {len(corrupted_rows)} rows with non-numeric values in '{col}'.")
                print(f"Sample of corrupted rows in '{col}' (up to 10):")
                print(corrupted_rows.head(10))
            
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # --- Final Cleanup ---
    df.fillna({
        'regional_sales': 0, 'product_id': -1, 'users_active': 0,
        'total_sales': 0, 'new_customers': 0, 'region': 'Unknown'
    }, inplace=True)
    
    for col in ['product_id', 'users_active', 'new_customers']:
        df[col] = df[col].astype(int)
    
    print("\nData cleaning and validation complete.")
    return df

def save_data(df, output_path):
    """Saves the final cleaned dataframe."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    # Ensure the final output has a consistent column order
    df[FINAL_COLUMNS].to_csv(output_path, index=False)
    print(f"4. Preprocessing complete. Cleaned data saved to: {output_path}")

def main(input_file, output_file):
    """Main function to run the entire preprocessing pipeline."""
    try:
        raw_df = pd.read_csv(input_file)
        
        # Step 1: Validate the schema of the raw file
        validated_df = validate_schema(raw_df)
        
        # Step 2: Unpivot the data only if it's in the wide format
        if is_wide_format(validated_df):
            processed_df = unpivot_data(validated_df)
        else:
            print("2. Unpivoting data (long format detected, skipping unpivot)")
            processed_df = validated_df
        
        # Step 3: Clean and handle any remaining data quality issues
        cleaned_df = clean_and_validate_data(processed_df)
        
        # Step 4: Save the final, clean data
        save_data(cleaned_df, output_file)
        
    except FileNotFoundError:
        print(f"ERROR: Input file not found at {input_file}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python scripts/preprocess_raw_data.py <input_file_path> <output_file_path>")
    else:
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        main(input_path, output_path)