import pandas as pd
import os

def preprocess_raw_data(raw_path, output_path):
    """
    Loads the wide-format raw marketing data, unpivots it into a long format,
    cleans it, and saves the result as a new CSV.
    """
    print(f"--- Starting Preprocessing ---")
    print(f"Loading raw data from: {raw_path}")
    df_raw = pd.read_csv(raw_path)

    # 1. Unpivot the data 
    print("Unpivoting data from wide to long format...")
    id_vars = ['date', 'users_active', 'total_sales', 'new_customers', 'report_generated']
    regional_cols = [col for col in df_raw.columns if col.startswith('col_')]
    
    processed_records = []
    for index, row in df_raw.iterrows():
        base_data = row[id_vars].to_dict()
        for i in range(0, len(regional_cols), 3):
            # Ensure all three columns in the group exist
            if i + 2 < len(regional_cols):
                region = row[regional_cols[i]]
                if pd.notna(region): # Only process if there's a region name
                    record = base_data.copy()
                    record['region'] = region
                    record['regional_sales'] = row[regional_cols[i+1]]
                    record['product_id'] = row[regional_cols[i+2]]
                    processed_records.append(record)

    df_processed = pd.DataFrame(processed_records)
    print(f"Successfully unpivoted {len(df_processed)} records.")

    # 2. Clean the data
    print("Cleaning data: handling nulls and setting data types...")
    df_processed['date'] = pd.to_datetime(df_processed['date'])
    df_processed['regional_sales'] = pd.to_numeric(df_processed['regional_sales'], errors='coerce')
    df_processed['product_id'] = pd.to_numeric(df_processed['product_id'], errors='coerce')

    # Fill NaN values with appropriate defaults
    df_processed.fillna({'regional_sales': 0, 'product_id': -1}, inplace=True)

    # Convert columns to their final, correct types
    df_processed['product_id'] = df_processed['product_id'].astype(int)

    # 3. Save the cleaned file
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_processed.to_csv(output_path, index=False)
    
    print(f"Preprocessing complete. Cleaned data saved to: {output_path}")
    return df_processed

if __name__ == '__main__':
    # Define file paths based on your project structure
    RAW_DATA_FILE = 'data/raw/marketing_summary.csv'
    CLEANED_OUTPUT_FILE = 'data/processed/marketing_summary_cleaned.csv'
    
    preprocess_raw_data(RAW_DATA_FILE, CLEANED_OUTPUT_FILE)