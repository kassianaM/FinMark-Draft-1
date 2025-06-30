import pandas as pd
import os

def validate_schema(df):
    """
    Checks for the presence of essential columns. If a column is missing,
    it adds it with a default value and prints a warning.
    """
    print("1. Validating initial schema")
    required_cols = ['date', 'users_active', 'total_sales', 'new_customers']
    is_valid = True
    
    for col in required_cols:
        if col not in df.columns:
            print(f"WARNING: Missing required column '{col}'. Creating it with default value 0.")
            df[col] = 0
            is_valid = False
            
    if is_valid:
        print("Initial schema validation passed.")
    
    return df

def unpivot_data(df):
    """
    Transforms the wide-format raw data into a clean, long-format DataFrame.
    """
    print("2. Unpivoting data from wide to long format")
    id_vars = ['date', 'users_active', 'total_sales', 'new_customers', 'report_generated']
    regional_cols = [col for col in df.columns if col.startswith('col_')]
    
    processed_records = []
    for index, row in df.iterrows():
        base_data = row[id_vars].to_dict()
        for i in range(0, len(regional_cols), 3):
            if i + 2 < len(regional_cols):
                region = row[regional_cols[i]]
                if pd.notna(region):
                    record = base_data.copy()
                    record['region'] = region
                    record['regional_sales'] = row[regional_cols[i+1]]
                    record['product_id'] = row[regional_cols[i+2]]
                    processed_records.append(record)

    df_processed = pd.DataFrame(processed_records)
    print(f"Successfully unpivoted data into {len(df_processed)} records.")
    return df_processed

def clean_and_validate_data(df):
    """
    Handles data type conversions, nulls, and corrupted data with warnings.
    """
    print("3. Cleaning and validating data types")
    
    # Handle dates with robust error checking
    original_rows = len(df)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df.dropna(subset=['date'], inplace=True)
    if len(df) < original_rows:
        print(f"WARNING: Removed {original_rows - len(df)} rows with invalid date formats.")

    # Handle numeric columns with robust error checking
    for col in ['users_active', 'total_sales', 'new_customers', 'regional_sales', 'product_id']:
        # Count nulls before conversion to detect corruption
        nulls_before = df[col].isnull().sum()
        df[col] = pd.to_numeric(df[col], errors='coerce')
        nulls_after = df[col].isnull().sum()
        
        corrupted_count = nulls_after - nulls_before
        if corrupted_count > 0:
            print(f"WARNING: Found and fixed {corrupted_count} corrupted non-numeric value(s) in '{col}'.")
    
    # Fill any remaining NaN values with sensible defaults
    df.fillna({'regional_sales': 0, 'product_id': -1}, inplace=True)
    
    # Convert to final, correct integer types
    df['product_id'] = df['product_id'].astype(int)
    df['users_active'] = df['users_active'].astype(int)
    df['new_customers'] = df['new_customers'].astype(int)
    
    print("Data cleaning and type validation complete.")
    return df

def save_data(df, output_path):
    """Saves the final cleaned dataframe to a CSV file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"4. Preprocessing complete. Cleaned data saved to: {output_path}")

def main():
    """Main function to run the entire preprocessing pipeline."""
    # Define file paths
    RAW_DATA_FILE = 'data/raw/marketing_summary.csv'
    CLEANED_OUTPUT_FILE = 'data/processed/marketing_summary_cleaned.csv'

    # Run the pipeline
    raw_df = pd.read_csv(RAW_DATA_FILE)
    validated_df = validate_schema(raw_df)
    unpivoted_df = unpivot_data(validated_df)
    cleaned_df = clean_and_validate_data(unpivoted_df)
    save_data(cleaned_df, CLEANED_OUTPUT_FILE)

if __name__ == '__main__':
    main()
