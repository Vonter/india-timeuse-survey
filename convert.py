import os
import polars as pl
from pathlib import Path

def convert_csv_to_parquet(input_dir='raw', output_dir='raw'):
    """
    Convert all CSV files in input_dir to Parquet format and save them in output_dir.
    
    Args:
        input_dir (str): Directory containing CSV files
        output_dir (str): Directory where Parquet files will be saved
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Get all CSV files in the input directory
    csv_files = list(Path(input_dir).glob('*.csv'))
    
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return
    
    print(f"Found {len(csv_files)} CSV files to convert")
    
    # Process each CSV file
    for csv_path in csv_files:
        try:
            # Read CSV file
            print(f"Reading {csv_path}...")
            # Define schema overrides for known problematic columns
            schema_overrides = {
                "FOD Sub-Region": pl.Utf8,
                "relationship of the informant with the household member": pl.Utf8
            }
            df = pl.read_csv(
                csv_path, 
                infer_schema_length=100,
                null_values=["", "NA", "null", "NULL", "None", "*", "NR"],
                schema_overrides=schema_overrides
            )

            # Create output path with .parquet extension
            parquet_path = Path(output_dir) / f"{csv_path.stem}.parquet"
            
            # Write to Parquet format
            print(f"Writing to {parquet_path}...")
            df.write_parquet(parquet_path)
            
            print(f"Successfully converted {csv_path} to {parquet_path}")
        except Exception as e:
            print(f"Error converting {csv_path}: {e}")

if __name__ == "__main__":
    convert_csv_to_parquet()
    print("Conversion complete!")
