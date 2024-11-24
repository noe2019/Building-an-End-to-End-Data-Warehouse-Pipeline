import kagglehub
import json
import pandas as pd
from sqlalchemy import create_engine

# Database configuration
DB_CONNECTION = {
    'server': 'DR-FOUOTSA\\SQLEXPRESS01',
    'database': 'DataPipelineDB',
    'trusted_connection': 'yes'
}

# 1. Download dataset from Kaggle
def download_dataset():
    """Download the dataset from Kaggle using kagglehub."""
    path = kagglehub.dataset_download("bitrook/us-county-historical-demographics")
    print("Path to dataset files:", path)
    return path

# 2. Load JSON data
def load_json_from_kaggle(path):
    """Load JSON data from the downloaded Kaggle dataset."""
    json_file_path = f"{path}/us_county_demographics.json"
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    return data

# 3. Transform JSON to DataFrame
def json_to_dataframe(data):
    """Transform JSON data into a pandas DataFrame."""
    # Ensure data is a list
    if not isinstance(data, list):
        raise ValueError("Expected JSON data to be a list of records.")

    records = []

    for record in data:
        base_info = {
            "zipcode": record.get("zipcode"),
            "major_city": record.get("major_city"),
            "state": record.get("state"),
            "lat": record.get("lat"),
            "lng": record.get("lng"),
            "county": record.get("county"),
            "population_2010_census": record.get("population_by_gender", {}).get("summary", {}).get("total", {}).get("2010_census"),
            "population_2019": record.get("population_by_gender", {}).get("summary", {}).get("total", {}).get("2019"),
            "median_age_total_2019": record.get("median_age", {}).get("total", {}).get("2019"),
            "median_age_male_2019": record.get("median_age", {}).get("male", {}).get("2019"),
            "median_age_female_2019": record.get("median_age", {}).get("female", {}).get("2019"),
        }
        
        # Population age group details (e.g., 0-4 years, 5-9 years, etc.)
        age_groups = [
            "0_4", "5_9", "10_14", "15_19", "20_24", "25_29",
            "30_34", "35_39", "40_44", "45_49", "50_54", "55_59",
            "60_64", "65_69", "70_74", "75_79", "80_84", "85_Plus"
        ]
        for group in age_groups:
            group_data = record.get("population_by_age", {}).get("total", {}).get(group, {})
            base_info[f"population_{group}_2019"] = group_data.get("2019")
            base_info[f"population_{group}_2010_census"] = group_data.get("2010_census")
        
        # Append the flattened record
        records.append(base_info)
    
    return pd.DataFrame(records)

# 4. Save DataFrame to SQL Server
def save_to_sql(df, table_name, db_config):
    """Save the DataFrame to MS SQL Server."""
    # Construct connection string for trusted connection
    connection_string = (
        f"mssql+pyodbc://@{db_config['server']}/{db_config['database']}?"
        f"driver=ODBC+Driver+17+for+SQL+Server&trusted_connection={db_config['trusted_connection']}"
    )
    try:
        engine = create_engine(connection_string)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data successfully saved to table: {table_name}")
    except Exception as e:
        print(f"Error saving to SQL: {e}")

# 5. Main Execution
if __name__ == "__main__":
    # Step 1: Download Kaggle dataset
    dataset_path = download_dataset()
    
    # Step 2: Load JSON data
    json_data = load_json_from_kaggle(dataset_path)
    
    # Step 3: Transform JSON data to DataFrame
    try:
        df = json_to_dataframe(json_data)
        print(df.head())  # Preview the data
    except ValueError as e:
        print(f"Error transforming JSON to DataFrame: {e}")
    
    # Step 4: Save DataFrame to SQL Server
    table_name = "USCountyDemographicsDetailed"
    save_to_sql(df, table_name, DB_CONNECTION)