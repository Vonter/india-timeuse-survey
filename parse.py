#!/usr/bin/env python3
import pandas as pd
import os
from mappings import *

def map_codes_to_descriptions(df, column_name, mapping_dict):
    """
    Maps code values in a column to their string descriptions.
    
    Args:
        df: Pandas DataFrame
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
        # Convert column to string type for consistent mapping
        df[column_name] = df[column_name].astype(str)
        
        # Clean float values by removing .0 suffix
        mask = df[column_name].str.endswith('.0')
        df.loc[mask, column_name] = df.loc[mask, column_name].str.replace('.0', '')
        
        # Create a Series with mapped values
        mapped_values = df[column_name].map(mapping_dict)
        
        # Handle unmapped values (keeping original value in the 'Unknown' message)
        unmapped_mask = mapped_values.isna()
        if unmapped_mask.any():
            unmapped_values = df.loc[unmapped_mask, column_name]
            mapped_values.loc[unmapped_mask] = "Unknown (" + unmapped_values + ")"
        
        # Replace NaN values with "Unknown"
        mapped_values = mapped_values.fillna("Unknown")
        
        # Update the dataframe column
        df[column_name] = mapped_values
        
        return df
        
    except Exception as e:
        print(f"Error mapping {column_name} codes: {str(e)}")
        # Return original dataframe if mapping fails
        return df

def map_district_codes(df):
    """
    Maps district codes based on FOD Sub-Region and District codes from districts.csv.
    
    Args:
        df: Pandas DataFrame with 'FOD Sub-Region' and 'District' columns
        
    Returns:
        Updated DataFrame with district codes mapped to district names, state names, region names, and sub-region names
    """
    # Input validation
    if "FOD Sub-Region" not in df.columns or "District" not in df.columns:
        print("Either 'FOD Sub-Region' or 'District' column not found in DataFrame")
        return df
    
    print("Mapping district codes using raw/districts.csv")
    
    try:
        # Ensure log directory exists
        os.makedirs('logs', exist_ok=True)
        
        # Read reference district data
        districts_df = pd.read_csv('raw/districts.csv')
        
        # Prepare data for mapping
        print("Preparing district lookup...")
        
        # Convert codes to integers for consistent matching (strip leading zeros)
        districts_df['Sub-region Code'] = districts_df['Sub-region Code'].astype(int)
        districts_df['District Code'] = districts_df['District Code'].astype(int)
        districts_df['State Code'] = districts_df['State Code'].astype(int)
        
        # Convert input dataframe columns to integers for matching
        df_for_mapping = df.copy()
        df_for_mapping['FOD Sub-Region'] = df_for_mapping['FOD Sub-Region'].astype(int)
        df_for_mapping['District'] = df_for_mapping['District'].astype(int)
        
        # Extract state code from NSS-Region based on string length
        def extract_state_code(nss_region):
            nss_str = str(nss_region)
            if len(nss_str) == 3:
                return nss_str[:2]  # Take left 2 characters for 3-char string
            elif len(nss_str) == 2:
                return nss_str[:1]  # Take leftmost character for 2-char string
            else:
                return nss_str  # Return as is for other cases
        
        df_for_mapping['NSS-Region'] = df_for_mapping['NSS-Region'].apply(extract_state_code).astype(int)
        
        # Create mapping dictionary manually to handle potential duplicates
        mapping_dict = {}
        duplicate_keys = set()
        
        # First, identify duplicate keys to handle them appropriately
        for _, row in districts_df.iterrows():
            key = f"{row['Sub-region Code']}_{row['District Code']}_{row['State Code']}"
            if key in mapping_dict:
                duplicate_keys.add(key)
            mapping_dict[key] = {
                'State Name': row['State Name'], 
                'District Name': row['District Name'],
                'Region Name': row['Region Name'],
                'Sub-region Name': row['Sub-region Name']
            }
        
        # Log duplicate keys if any
        if duplicate_keys:
            print(f"Warning: Found {len(duplicate_keys)} duplicate mapping keys in districts.csv")
            with open('logs/duplicate_district_keys.log', 'w') as log_file:
                log_file.write("Duplicate District Mapping Keys:\n")
                log_file.write("Key,State,District,Region,Sub-region\n")
                for key in duplicate_keys:
                    log_file.write(f"{key},{mapping_dict[key]['State Name']},{mapping_dict[key]['District Name']},{mapping_dict[key]['Region Name']},{mapping_dict[key]['Sub-region Name']}\n")
        
        print(f"Lookup dictionary created with {len(mapping_dict)} entries")
        
        # Create mapping key in input dataframe
        df_for_mapping['mapping_key'] = df_for_mapping['FOD Sub-Region'].astype(str) + '_' + df_for_mapping['District'].astype(str) + '_' + df_for_mapping['NSS-Region'].astype(str)
        
        # Perform the mapping using vectorized operations
        total_records = len(df)
        print(f"Mapping {total_records} records...")
        
        # Initialize new columns with default values
        df['state'] = "Unknown State"
        df['district'] = "Unknown District"
        df['region'] = "Unknown Region"
        df['sub_region'] = "Unknown Sub-region"
        
        # Track mapped and unmapped records
        unmapped_records = set(df_for_mapping['mapping_key'])
        
        # Apply the mapping using the dictionary (faster than applying row by row)
        for key, values in mapping_dict.items():
            mask = df_for_mapping['mapping_key'] == key
            if mask.any():
                df.loc[mask, 'state'] = values['State Name']
                df.loc[mask, 'district'] = values['District Name']
                df.loc[mask, 'region'] = values['Region Name']
                df.loc[mask, 'sub_region'] = values['Sub-region Name']
                # Remove from unmapped set
                if key in unmapped_records:
                    unmapped_records.remove(key)
        
        # Find unmapped keys for logging
        unmapped_keys = []
        for key in unmapped_records:
            if '_' in key:
                sub_region, district, state = key.split('_')
                unmapped_keys.append((sub_region, district, state))
        
        # Calculate mapping statistics
        mapped_count = total_records - df[df['state'] == "Unknown State"].shape[0]
        failed_mappings = total_records - mapped_count
        
        # Log unmapped keys
        with open('logs/district_mapping_failures.log', 'w') as log_file:
            log_file.write("Failed District Mappings:\n")
            log_file.write("Sub-region Code,District Code,State Code\n")
            for sub_region, district, state in unmapped_keys:
                log_file.write(f"{sub_region},{district},{state}\n")
        
        # Print mapping statistics
        print(f"District mapping complete: {total_records} total records processed")
        print(f"  Successfully mapped: {mapped_count} records")
        print(f"  Failed to map: {failed_mappings} records")
        
        return df
        
    except Exception as e:
        print(f"Error mapping district codes: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return df

def main():
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    print("Reading household data...")
    # Read household data
    household_df = pd.read_csv('raw/tus106hh.csv')
    
    print(f"Household data shape: {household_df.shape}")
    print(f"Household data columns: {household_df.columns.tolist()}")
    
    print("\nReading person data...")
    # Read person data
    person_df = pd.read_csv('raw/TUS106PER.csv')
    
    print(f"Person data shape: {person_df.shape}")
    print(f"Person data columns: {person_df.columns.tolist()}")
    
    # Identify join keys
    join_keys = ['Schedule ID', 'FSU Serial No.', 'Schedule', 'survey year', 'Sector', 'NSS-Region', 'District', 'Stratum', 'Sub-Stratum', 'Sub-Round', 'FOD Sub-Region', 'Sample hhld. No.']
    
    print(f"\nJoining datasets on keys: {join_keys}")
    
    # Merge datasets
    merged_df = pd.merge(
        person_df,
        household_df,
        on=join_keys,
        how='left',
        suffixes=('_person', '_household')
    )
    
    print(f"Merged data shape: {merged_df.shape}")

    # Create a new column for the person ID
    merged_df['person_id'] =  merged_df['FSU Serial No.'].astype(str) + merged_df['Sample hhld. No.'].astype(str) + '_' + merged_df['Person serial no.'].astype(str)

    # Apply mappings to convert code values to human-readable descriptions
    if "District" in merged_df.columns:
        merged_df = map_district_codes(merged_df)
    if "Gender" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "Gender", GENDER_MAPPING)
    if "highest level of education" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "highest level of education", EDUCATION_MAPPING)
    if "religion" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "religion", RELIGION_MAPPING)
    if "Social group " in merged_df.columns:  # Note the space at the end
        merged_df = map_codes_to_descriptions(merged_df, "Social group ", SOCIAL_GROUP_MAPPING)
    if "marital status" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "marital status", MARITAL_STATUS_MAPPING)
    if "usual principal activity status (code)" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "usual principal activity status (code)", PRINCIPAL_ACTIVITY_MAPPING)
    if "industry of work: 2-digit of NIC 2008" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "industry of work: 2-digit of NIC 2008", INDUSTRY_CODE_MAPPING)
    if "3-digit activity code" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "3-digit activity code", ACTIVITY_CODE_MAPPING)
    if "enterprise type" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "enterprise type", ENTERPRISE_TYPE_MAPPING)
    if "where the activity was performed" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "where the activity was performed", ACTIVITY_LOCATION_MAPPING)
    if "unpaid/paid status of activity" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "unpaid/paid status of activity", PAYMENT_STATUS_MAPPING)
    if "day of week" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "day of week", DAY_OF_WEEK_MAPPING)
    if "type of the day" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "type of the day", DAY_TYPE_MAPPING)
    if "response code" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "response code", RESPONSE_CODE_MAPPING)
    if "Relation to head" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "Relation to head", RELATION_TO_HEAD_MAPPING)
    
    # Apply mapping to additional columns for full dataset
    if "Land possessed as on date of survey(code)" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "Land possessed as on date of survey(code)", LAND_POSSESSED_MAPPING)
    if "Gender of the informant" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "Gender of the informant", GENDER_MAPPING)
    if "Response Code" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "Response Code", RESPONSE_CODE_HOUSEHOLD_MAPPING)
    if "Survey Code" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "Survey Code", SURVEY_CODE_MAPPING)
    if "Reason for substitution of original household" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "Reason for substitution of original household", REASON_FOR_SUBSTITUTION_MAPPING)
    if "Primary source of energey for cooking" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "Primary source of energey for cooking", PRIMARY_ENERGY_COOKING_MAPPING)
    if "Primary source of energey for lighting" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "Primary source of energey for lighting", PRIMARY_ENERGY_LIGHTING_MAPPING)
    if "Type of washing of clothes" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "Type of washing of clothes", CLOTHES_WASHING_TYPE_MAPPING)
    if "Type of sweeping of floor" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "Type of sweeping of floor", FLOOR_SWEEPING_TYPE_MAPPING)
    if "Dwelling unit" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "Dwelling unit", DWELLING_UNIT_MAPPING)
    if "Type of structure of the dwelling unit" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "Type of structure of the dwelling unit", DWELLING_STRUCTURE_TYPE_MAPPING)
    if "Is there any member in the household aged 5 years and above who needs special care" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "Is there any member in the household aged 5 years and above who needs special care", SPECIAL_CARE_MAPPING)
    if "Is there any care giver available among the household members for caring the person(s)" in merged_df.columns:
        merged_df = map_codes_to_descriptions(merged_df, "Is there any care giver available among the household members for caring the person(s)", CAREGIVER_AVAILABLE_MAPPING)

    # Drop columns that are not needed
    merged_df = merged_df.drop(columns=['age.1', 'age', 'District', 'Serial no.of member', 'NSC_household', 'MULT_household'])

    # Make person_id the first column
    merged_df = merged_df[['person_id'] + [col for col in merged_df.columns if col != 'person_id']]
    
    # Rename columns
    merged_df.rename(columns={"State": "state",
                              "Gender": "gender",
                              "Age": "age",
                              "marital status": "marital_status",
                              "highest level of education": "education",
                              "religion": "religion",
                              "Social group ": "social_group",
                              "Household size": "household_size",
                              "usual monthly consumer expenditure E: [A+B+C+(D+E)/12]": "monthly_expenditure",
                              "usual principal activity status (code)": "principal_activity",
                              "industry of work: 2-digit of NIC 2008": "industry",
                              "day of week": "day_of_week",
                              "type of the day": "day_type",
                              "srl. No of activity": "activity_serial_no",
                              "time from (HH:MM)": "time_from",
                              "time to (HH:MM)": "time_to",
                              "whether performed multiple activity in the time slot": "multiple_activity",
                              "whether simultaneous activity": "simultaneous_activity",
                              "whether a major activity": "is_major_activity",
                              "3-digit activity code": "activity_code",
                              "where the activity was performed": "activity_location",
                              "unpaid/paid status of activity": "payment_status",
                              "response code": "response_code",
                              "Relation to head": "relation_to_head",
                              "enterprise type": "enterprise_type",
                              # Additional column renames for full dataset
                              "Schedule ID": "schedule_id",
                              "FSU Serial No.": "fsu_serial_no",
                              "Schedule": "schedule",
                              "survey year": "survey_year",
                              "Sector": "sector",
                              "NSS-Region": "nss_region",
                              "Stratum": "stratum",
                              "Sub-Stratum": "sub_stratum",
                              "Sub-Round": "sub_round",
                              "FOD Sub-Region": "fod_sub_region",
                              "Sample hhld. No.": "sample_household_no",
                              "Person serial no.": "person_serial_no",
                              "srl. No of member": "member_serial_no",
                              "Serial number of the informant": "informant_serial_no",
                              "Gender of the informant": "informant_gender",
                              "NSC_person": "nsc",
                              "MULT_person": "mult",
                              "Informant Sl.No.": "informant_sl_no",
                              "Response Code": "response_code_household",
                              "Survey Code": "survey_code",
                              "Reason for substitution of original household": "household_substitution_reason",
                              "Land possessed as on date of survey(code)": "land_possessed",
                              "usual consumer expenditure in a month for household purposes out of purchase (A)": "monthly_expenditure_purchase",
                              "imputed value of usual consumption in a month from home grown stock (B)": "monthly_consumption_home_grown",
                              "imputed value of usual consumption in a month from wages in kind, free collection, gifts, etc (C )": "monthly_consumption_in_kind",
                              "expenditure on purchase of items like clothing, footwear etc. during last 365 days (D)": "yearly_expenditure_clothing",
                              "expenditure on purchase of household durable during last 365 days (E)": "yearly_expenditure_durables",
                              "Primary source of energey for cooking": "primary_energy_cooking",
                              "Primary source of energey for lighting": "primary_energy_lighting",
                              "Type of washing of clothes": "clothes_washing_type",
                              "Type of sweeping of floor": "floor_sweeping_type",
                              "Dwelling unit": "dwelling_unit",
                              "Type of structure of the dwelling unit": "dwelling_structure_type",
                              "Is there any member in the household aged 5 years and above who needs special care": "has_member_needing_care",
                              "Is there any care giver available among the household members for caring the person(s)": "has_caregiver_available",
                              "Time to canvass(minutes)": "time_to_canvass_minutes"}, inplace=True)

    # Reorder columns
    print("\nReordering columns...")
    
    # Set the column order as specified
    column_order = ['person_id', 'state', 'district', 'gender', 'age', 'marital_status', 
                   'education', 'religion', 'social_group', 'household_size', 
                   'monthly_expenditure', 'principal_activity', 'industry', 
                   'activity_serial_no', 'time_from', 'time_to', 'multiple_activity', 
                   'simultaneous_activity', 'is_major_activity', 'activity_code', 
                   'activity_location', 'payment_status', 'enterprise_type',
                   'relation_to_head', 'day_of_week', 'day_type', 'response_code',
                   # Additional columns for full dataset
                   'region', 'sub_region', 'land_possessed', 'monthly_expenditure_purchase', 
                   'monthly_consumption_home_grown', 'monthly_consumption_in_kind', 
                   'yearly_expenditure_durables', 'yearly_expenditure_clothing',
                   'primary_energy_cooking', 'primary_energy_lighting', 'clothes_washing_type',
                   'floor_sweeping_type', 'dwelling_unit', 'dwelling_structure_type',
                   'has_member_needing_care', 'has_caregiver_available',
                   'informant_serial_no', 'informant_gender', 'informant_sl_no',
                   'time_to_canvass_minutes', 'member_serial_no', 'person_serial_no',
                   'response_code_household', 'survey_code', 'household_substitution_reason',
                   'schedule_id', 'fsu_serial_no', 'schedule', 'survey_year', 'sector',
                   'nss_region', 'stratum', 'sub_stratum', 'sub_round', 'fod_sub_region',
                   'sample_household_no', 'nsc', 'mult']
    
    # Ensure all columns in column_order exist in dataframe
    existing_columns = [col for col in column_order if col in merged_df.columns]
    
    # Get all columns that are not in the specified order
    remaining_columns = [col for col in merged_df.columns if col not in column_order]
    
    # Reorder columns with specified columns first, then any remaining columns
    merged_df = merged_df[existing_columns + remaining_columns]

    # Save the datasets as parquet
    print("\nSaving parquet files...")

    # Save the merged dataframe with all fields
    merged_df.to_parquet('data/individual_daily_schedule_full.parquet')
    
    # Drop columns that are not needed
    merged_df = merged_df.drop(columns=['schedule_id', 'fsu_serial_no', 'schedule', 'survey_year', 'sector', 
                                      'nss_region', 'stratum', 'sub_stratum', 'sub_round', 
                                      'fod_sub_region', 'sample_household_no'])
    merged_df = merged_df.drop(columns=['nsc', 'mult', 'informant_serial_no', 'informant_gender', 'informant_sl_no', 
                                      'time_to_canvass_minutes', 'member_serial_no', 'person_serial_no',
                                      'survey_code', 'household_substitution_reason'])
    merged_df = merged_df.drop(columns=['dwelling_structure_type', 'dwelling_unit', 'floor_sweeping_type', 
                                      'clothes_washing_type', 'primary_energy_lighting', 'primary_energy_cooking', 
                                      'yearly_expenditure_durables', 'yearly_expenditure_clothing', 
                                      'monthly_consumption_in_kind', 'monthly_consumption_home_grown', 
                                      'monthly_expenditure_purchase', 'land_possessed'])
    merged_df = merged_df.drop(columns=['response_code_household', 'has_member_needing_care', 'has_caregiver_available',
                                      'region', 'sub_region'])

    # Save the merged dataframe
    merged_df.to_parquet('data/individual_daily_schedule.parquet')
    
    print("Done! Parquet files saved to data/")

if __name__ == "__main__":
    main()
