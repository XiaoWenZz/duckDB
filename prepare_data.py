import duckdb
import glob
import os
import time

def convert_parquet_to_csv():
    """
    Converts all .parquet files in the data/ directory to .csv format
    using DuckDB's efficient internal COPY command.
    """
    # Ensure data directory exists
    if not os.path.exists('data'):
        print("Error: 'data/' directory not found. Please create it and add .parquet files.")
        return

    parquet_files = glob.glob('data/*.parquet')
    
    if not parquet_files:
        print("No .parquet files found in data/")
        return

    print(f"Found {len(parquet_files)} parquet files. Starting conversion...")
    
    conn = duckdb.connect()
    
    for p_file in parquet_files:
        csv_file = p_file.replace('.parquet', '.csv')
        
        if os.path.exists(csv_file):
            print(f"Skipping {p_file} -> {csv_file} (Already exists)")
            continue
            
        print(f"Converting {p_file} -> {csv_file}...", end="", flush=True)
        start_time = time.time()
        
        try:
            # efficient copy using DuckDB engine
            conn.execute(f"COPY (SELECT * FROM '{p_file}') TO '{csv_file}' (HEADER, DELIMITER ',')")
            elapsed = time.time() - start_time
            print(f" Done ({elapsed:.2f}s)")
        except Exception as e:
            print(f" Error: {e}")

    print("\nConversion complete.")

if __name__ == "__main__":
    convert_parquet_to_csv()
