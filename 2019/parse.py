import os
import polars as pl
from glob import glob
from mappings import *

# Define the columns to group by
GROUP_BY_COLUMNS = [
    "FSU Serial No.", "Sector",
    "State", "District", 
    "Stratum", "Sub-Stratum", "Sub-Round", 
    "FOD Sub-Region", "Sample hhld. No."
]

# Alternative column names that should be treated the same
COLUMN_ALIASES = {
    "Schedule": [", Schedule"],  # Treat "Schedule" and ", Schedule" as the same column
    "Sl.No.": ["Person serial no.", "Serial no.of member"], # Treat "Sl.No." and "Person serial no." as the same column
}

def process_parquet_file(file_path):
    """Process a single parquet file."""
    print(f"\nProcessing file: {file_path}")
    
    try:
        # Read the parquet file with Polars
        df = pl.read_parquet(file_path)
        
        # Drop the first 4 columns except the 2nd column
        columns_to_keep = [df.columns[1]] + df.columns[4:]  # Keep 2nd column and columns from 5th onwards
        df = df.select(columns_to_keep)
        
        # Normalize column names - replace aliases with standard names
        for standard_name, aliases in COLUMN_ALIASES.items():
            for alias in aliases:
                if alias in df.columns and standard_name not in df.columns:
                    df = df.rename({alias: standard_name})
        
        # Check which grouping columns are available
        available_group_cols = [col for col in GROUP_BY_COLUMNS if col in df.columns]
        
        if not available_group_cols:
            print(f"Error: No grouping columns found in {file_path}")
            return None
            
        missing_columns = [col for col in GROUP_BY_COLUMNS if col not in df.columns]
        if missing_columns:
            print(f"Warning: Missing columns in {file_path}: {missing_columns}")
            print(f"Grouping by available columns: {available_group_cols}")
        
        # Create household_id by concatenating the values of group columns
        try:
            # Convert all grouping columns to strings
            for col in available_group_cols:
                df = df.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
            
            # Concatenate columns to create household_id
            print("Creating household_id by concatenating columns")
            df = df.with_columns(
                household_id=pl.concat_str([pl.col(col) for col in available_group_cols])
            )
            
            return df
            
        except Exception as e:
            print(f"Error during dataframe transformation: {str(e)}")
            print(f"Available columns: {df.columns}")
            return None
    
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return None

def create_individual_profiles(parquet_files):
    """Create individual profiles from multiple parquet files."""
    print("\nCreating individual profiles...")
    
    # Dictionary to store dataframes
    dfs = {}
    
    # Group files by base name (without numbers)
    file_groups = {}
    for file_path in parquet_files:
        file_name = os.path.basename(file_path).split('.')[0]
        # Extract base name without numbers (e.g., "TUS106_L05_1" -> "tus106_l05")
        base_name = file_name.lower()
        if "_" in base_name and base_name.split("_")[-1].isdigit():
            parts = base_name.split("_")
            base_name = "_".join(parts[:-1])
        
        if base_name not in file_groups:
            file_groups[base_name] = []
        file_groups[base_name].append(file_path)
    
    # Process each file group
    for base_name, file_paths in file_groups.items():
        print(f"\nProcessing file group: {base_name} ({len(file_paths)} files)")
        
        if len(file_paths) == 1:
            # Single file case
            df = process_parquet_file(file_paths[0])
            if df is not None:
                dfs[base_name] = df
        else:
            # Multiple files case - concatenate them
            combined_df = None
            for file_path in file_paths:
                df = process_parquet_file(file_path)
                if df is not None:
                    if combined_df is None:
                        combined_df = df
                    else:
                        # Check for schema compatibility issues
                        try:
                            # Try to concatenate directly first
                            combined_df = pl.concat([combined_df, df], how="vertical")
                        except pl.exceptions.SchemaError as e:
                            print(f"Schema mismatch when concatenating files: {e}")
                            print("Attempting to harmonize schema...")
                            
                            # Get all columns from both dataframes
                            all_columns = set(combined_df.columns).union(set(df.columns))
                            
                            # For each dataframe, ensure all columns exist with compatible types
                            for col in all_columns:
                                # If column exists in both dataframes but with different types
                                if col in combined_df.columns and col in df.columns:
                                    combined_type = combined_df.schema[col]
                                    df_type = df.schema[col]
                                    
                                    if combined_type != df_type:
                                        print(f"Column '{col}' has different types: {combined_type} vs {df_type}")
                                        
                                        # Try to convert to string as a fallback
                                        if col in combined_df.columns:
                                            combined_df = combined_df.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
                                        if col in df.columns:
                                            df = df.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
                                
                                # If column exists in one dataframe but not the other, add it with nulls
                                elif col in combined_df.columns and col not in df.columns:
                                    df = df.with_columns(pl.lit(None).alias(col))
                                elif col not in combined_df.columns and col in df.columns:
                                    combined_df = combined_df.with_columns(pl.lit(None).alias(col))
                            
                            # Try concatenating again after harmonizing schema
                            try:
                                combined_df = pl.concat([combined_df, df], how="vertical")
                                print("Successfully concatenated after schema harmonization")
                            except Exception as e2:
                                print(f"Failed to concatenate even after schema harmonization: {e2}")
                                print("Continuing with partial data")
            
            if combined_df is not None:
                print(f"Combined {len(file_paths)} files for {base_name}")
                dfs[base_name] = combined_df
    
    # Add individual demographics information
    print("Adding individual demographics information from tus106_l02")
    individual_demographics_df = dfs["tus106_l02"]
    
    # Define columns for individual demographics
    individual_demographics_columns = [
        "household_id", "Sl.No.",
        "State", "District", 
        "Gender", "Age", "marital status",
        "highest level of education", "usual principal activity: status (code)",
        "industry of work: 2-digit of NIC 2008"
    ]
    
    # Select available columns
    available_columns = [col for col in individual_demographics_columns if col in individual_demographics_df.columns]
    individual_profile = individual_demographics_df.select(available_columns)
    # Add person_id by concatenating household_id and Sl.No.
    if "Sl.No." in individual_profile.columns:
        individual_profile = individual_profile.with_columns(
            person_id=pl.concat_str([pl.col("household_id"), pl.col("Sl.No.")])
        )
        print(f"Added person_id column by concatenating household_id and Sl.No.")
        
        # Drop Sl.No. column after creating person_id
        individual_profile = individual_profile.drop("Sl.No.")
        
        # Make person_id the first column
        columns = individual_profile.columns
        new_column_order = ["person_id"] + [col for col in columns if col != "person_id"]
        individual_profile = individual_profile.select(new_column_order)
    else:
        print(f"Warning: Sl.No. column not found, person_id could not be created")
    
    # Map district codes if column exists
    if "District" in individual_profile.columns:
        individual_profile = map_district_codes(individual_profile)

    # Map education codes if column exists
    if "highest level of education" in individual_profile.columns:
        individual_profile = map_codes_to_descriptions(
            individual_profile, 
            "highest level of education", 
            EDUCATION_MAPPING
        )

    # Map state codes if column exists
    if "State" in individual_profile.columns:
        individual_profile = map_codes_to_descriptions(
            individual_profile, 
            "State", 
            STATE_MAPPING
        )
        
    # Map gender codes if column exists
    if "Gender" in individual_profile.columns:
        individual_profile = map_codes_to_descriptions(
            individual_profile,
            "Gender",
            GENDER_MAPPING
        )
        
    # Map marital status codes if column exists
    if "marital status" in individual_profile.columns:
        individual_profile = map_codes_to_descriptions(
            individual_profile,
            "marital status",
            MARITAL_STATUS_MAPPING
        )
    
    # Map principal activity codes if column exists
    if "usual principal activity: status (code)" in individual_profile.columns:
        individual_profile = map_codes_to_descriptions(
            individual_profile,
            "usual principal activity: status (code)",
            PRINCIPAL_ACTIVITY_MAPPING
        )

    # Map industry codes if column exists
    if "industry of work: 2-digit of NIC 2008" in individual_profile.columns:
        individual_profile = map_codes_to_descriptions(
            individual_profile,
            "industry of work: 2-digit of NIC 2008",
            INDUSTRY_CODE_MAPPING
        )
    
    # Add household demographics information
    print("Adding household demographics information from tus106_l03")
    household_demographics_df = dfs["tus106_l03"]
    
    # Define columns for household demographics
    household_demographics_columns = [
        "household_id", "Household size", "religion", "Social group ",
        "usual monthly consumer expenditure E: [A+B+C+(D/12)]"
    ]

    # Select available columns
    available_columns = [col for col in household_demographics_columns if col in household_demographics_df.columns]
    household_info = household_demographics_df.select(available_columns)
    
    # Map religion codes if column exists
    if "religion" in household_info.columns:
        household_info = map_codes_to_descriptions(
            household_info,
            "religion",
            RELIGION_MAPPING
        )
    
    # Map social group codes if column exists
    if "Social group " in household_info.columns:
        household_info = map_codes_to_descriptions(
            household_info,
            "Social group ",
            SOCIAL_GROUP_MAPPING
        )

    individual_profile = individual_profile.join(
        household_info,
        on="household_id",
        how="left"
    )
    
    # Rename columns to match the required output
    column_mapping = {
        "State": "state",
        "District": "district",
        "Gender": "gender",
        "Age": "age",
        "marital status": "marital_status",
        "highest level of education": "education",
        "usual principal activity: status (code)": "principal_activity",
        "industry of work: 2-digit of NIC 2008": "industry",
        "Household size": "household_size",
        "religion": "religion",
        "Social group ": "social_group",
        "usual monthly consumer expenditure E: [A+B+C+(D/12)]": "monthly_expenditure"
    }
    
    # Apply renaming for columns that exist
    rename_dict = {old: new for old, new in column_mapping.items() if old in individual_profile.columns}
    if rename_dict:
        individual_profile = individual_profile.rename(rename_dict)
    
    # Extract time use data from TUS106_L05
    print("Extracting time use data from tus106_l05")
    time_use_df = dfs["tus106_l05"]
    
    # Define columns to extract from time use data
    time_use_columns = [
        "household_id", "Sl.No.",
        "srl. No of activity", "time from", "time to",
        "whether performed multiple activity in the time slot",
        "whether simultaneous activity", "whether a major activity",
        "3-didit activity code", "where the activity was performed",
        "unpaid/paid status of activity", "enterprise type"
    ]
    
    # Select available columns
    available_columns = [col for col in time_use_columns if col in time_use_df.columns]
    time_use_data = time_use_df.select(available_columns)
    
    # Create person_id to join with individual_profile
    if "Sl.No." in time_use_data.columns:
        time_use_data = time_use_data.with_columns(
            person_id=pl.concat_str([pl.col("household_id"), pl.col("Sl.No.")])
        )
        time_use_data = time_use_data.drop("Sl.No.")
        time_use_data = time_use_data.drop("household_id")
        individual_profile = individual_profile.drop("household_id")
        
        # Rename columns to more user-friendly names
        column_mapping = {
            "srl. No of activity": "activity_serial_no",
            "time from": "time_from",
            "time to": "time_to",
            "whether performed multiple activity in the time slot": "multiple_activity",
            "whether simultaneous activity": "simultaneous_activity",
            "whether a major activity": "is_major_activity",
            "3-didit activity code": "activity_code",
            "where the activity was performed": "activity_location",
            "unpaid/paid status of activity": "payment_status",
            "enterprise type": "enterprise_type"
        }
        
        # Apply renaming for columns that exist
        rename_dict = {old: new for old, new in column_mapping.items() if old in time_use_data.columns}
        if rename_dict:
            time_use_data = time_use_data.rename(rename_dict)
        
        # Map enterprise type codes if column exists
        if "enterprise_type" in time_use_data.columns:
            time_use_data = map_codes_to_descriptions(
                time_use_data,
                "enterprise_type",
                ENTERPRISE_TYPE_MAPPING
            )

        # Map activity location codes if column exists
        if "activity_location" in time_use_data.columns:
            time_use_data = map_codes_to_descriptions(
                time_use_data,
                "activity_location",
                ACTIVITY_LOCATION_MAPPING
            )

        # Map multiple activity codes if column exists
        if "multiple_activity" in time_use_data.columns:
            time_use_data = map_codes_to_descriptions(
                time_use_data,
                "multiple_activity",
                {"1": "yes", "2": "no", None: "yes"}
            )

        # Map simultaneous activity codes if column exists
        if "simultaneous_activity" in time_use_data.columns:
            time_use_data = map_codes_to_descriptions(
                time_use_data,
                "simultaneous_activity",
                {"1": "yes", "2": "no", None: "yes"}
            )

        # Map is major activity codes if column exists
        if "is_major_activity" in time_use_data.columns:
            time_use_data = map_codes_to_descriptions(
                time_use_data,
                "is_major_activity",
                {"1": "yes", "2": "no"}
            )

        # Map payment status codes if column exists
        if "payment_status" in time_use_data.columns:
            time_use_data = map_codes_to_descriptions(
                time_use_data,
                "payment_status",
                PAYMENT_STATUS_MAPPING
            )

        # Add mapping for activity codes
        if "activity_code" in time_use_data.columns:
            time_use_data = map_codes_to_descriptions(
                time_use_data,
                "activity_code",
                ACTIVITY_CODE_MAPPING
            )
        
        # Join individual profiles and time use data
        print("Joining individual profiles and time use data")
        individual_profile_detailed = time_use_data.join(
            individual_profile,
            on="person_id",
            how="left"
        )
        
        # Set the specific column order as requested
        desired_column_order = [
            "person_id",
            "state",
            "district",
            "gender",
            "age",
            "marital_status",
            "education",
            "religion",
            "social_group",
            "household_size",
            "monthly_expenditure",
            "principal_activity",
            "industry",
            "activity_serial_no",
            "time_from",
            "time_to",
            "multiple_activity",
            "simultaneous_activity",
            "is_major_activity",
            "activity_code",
            "activity_location",
            "payment_status",
            "enterprise_type"
        ]
        
        # Filter the desired columns to only include those that actually exist in the DataFrame
        available_desired_columns = [col for col in desired_column_order if col in individual_profile_detailed.columns]
        
        # Get any columns that exist in the DataFrame but aren't in our desired order list
        remaining_columns = [col for col in individual_profile_detailed.columns if col not in desired_column_order]
        
        # Create the final column order with available desired columns first, then any remaining columns
        final_column_order = available_desired_columns + remaining_columns
        
        # Reorder the columns
        individual_profile_detailed = individual_profile_detailed.select(final_column_order)
        
        # Save the joined data to a parquet file
        individual_profile_detailed_path = "data/individual_daily_schedule.parquet"
        individual_profile_detailed.write_parquet(individual_profile_detailed_path)
        print(f"Joined individual and time use data saved to {individual_profile_detailed_path}")
        
        # Print sample of the joined data
        print("\nSample of joined individual and time use data:")
        print(individual_profile_detailed.head(10))
    else:
        print(f"Warning: Sl.No. column not found in time use data, cannot create person_id")
    
    return individual_profile

def map_codes_to_descriptions(df, column_name, mapping_dict):
    """
    Maps code values in a column to their string descriptions.
    
    Args:
        df: Polars DataFrame
        column_name: Name of the column containing codes to map
        mapping_dict: Dictionary mapping codes to descriptions
        
    Returns:
        Updated DataFrame with mapped values
    """
    if column_name not in df.columns:
        print(f"Column '{column_name}' not found in DataFrame")
        return df
    
    print(f"Mapping {column_name} codes to descriptions")
    
    try:
        # Check if the column is a struct type
        if df.schema[column_name] == pl.Struct:
            print(f"{column_name} column is a struct type, extracting value before mapping")
            # Extract the value from the struct before mapping
            df = df.with_columns(
                pl.col(column_name).cast(pl.Utf8).alias(column_name)
            )
        
        # Get unique values in the column
        unique_values = df.select(pl.col(column_name).unique()).to_series().to_list()
        print(f"Unique values in {column_name}: {unique_values}")
        
        # Create a simpler approach using a custom function
        def map_value(value):
            if value is None:
                return None
            str_value = str(value)
            if str_value in mapping_dict:
                return mapping_dict[str_value]
            return f"Unknown ({value})"
        
        # Apply the mapping using a user-defined function with return_dtype specified
        df = df.with_columns(
            pl.col(column_name).map_elements(map_value, return_dtype=pl.Utf8).alias(column_name)
        )
        
        return df
        
    except Exception as e:
        print(f"Error mapping {column_name} codes: {str(e)}")
        print(f"{column_name} column type: {df.schema[column_name]}")
        # Return original dataframe if mapping fails
        return df

def map_district_codes(df):
    """
    Maps district codes to district names based on the state and district.
    
    Args:
        df: Polars DataFrame with 'State' and 'District' columns
        
    Returns:
        Updated DataFrame with district codes mapped to names
    """
    if "State" not in df.columns or "District" not in df.columns:
        print("Either 'State' or 'District' column not found in DataFrame")
        return df
    
    print("Mapping district codes to district names based on state")
    
    try:
        # Create a function to map district codes based on state
        def map_district(state_val, district_val):
            if state_val is None or district_val is None:
                return None
                
            # Convert to strings for comparison
            state_str = str(state_val)
            district_str = str(district_val)
            
            # Get district mapping for the state
            if state_str in DISTRICT_MAPPING:
                district_map = DISTRICT_MAPPING[state_str]
                if district_str in district_map:
                    return district_map[district_str]
            
            return f"Unknown District ({district_val})"
        
        # Apply the mapping using wit1h_columns
        df = df.with_columns(
            district_name=pl.struct(["State", "District"]).map_elements(
                lambda x: map_district(x["State"], x["District"]),
                return_dtype=pl.Utf8
            )
        )
        
        # Replace the District column with the mapped names
        df = df.drop("District").rename({"district_name": "District"})
        
        return df
        
    except Exception as e:
        print(f"Error mapping district codes: {str(e)}")
        # Return original dataframe if mapping fails
        return df

def main():
    # Find all parquet files in the data directory
    parquet_files = glob("raw/*.parquet")
    
    if not parquet_files:
        print("No parquet files found in the raw/ directory")
        return
    
    print(f"Found {len(parquet_files)} parquet files")
    
    # Create individual profiles
    create_individual_profiles(parquet_files)

if __name__ == "__main__":
    main()
