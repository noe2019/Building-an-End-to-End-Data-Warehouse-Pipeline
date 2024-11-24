import requests
import pandas as pd
import pyodbc
import os

# Database connection parameters
DB_CONNECTION = {
    'server': r'Your-server-name\Instance',
    'database': 'Your-database-name'
}

# URL for the Consumer Complaint Database CSV file
CSV_URL = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"

def download_csv_file(url, output_file):
    """
    Download the CSV file from the given URL and save it locally.
    """
    try:
        print("Downloading CSV file...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(output_file, 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
        print(f"File downloaded: {output_file}")
    except requests.RequestException as e:
        print(f"Error downloading CSV file: {e}")

def extract_and_read_csv(zip_file):
    """
    Extract and load the CSV data from the ZIP file.
    """
    try:
        print("Extracting CSV file...")
        extracted_file = pd.read_csv(zip_file, compression='zip', low_memory=False)
        print("CSV file extracted and read.")
        return extracted_file
    except Exception as e:
        print(f"Error extracting and reading CSV file: {e}")
        return None

def clean_data(row):
    """
    Clean and validate data for insertion into SQL Server.
    """
    try:
        # Validate or cast data types
        return {
            "complaint_id": int(row.get("Complaint ID")) if row.get("Complaint ID") else None,
            "product": str(row.get("Product")) if row.get("Product") else None,
            "issue": str(row.get("Issue")) if row.get("Issue") else None,
            "company": str(row.get("Company")) if row.get("Company") else None,
            "state": str(row.get("State")) if row.get("State") else None,
            "submitted_via": str(row.get("Submitted via")) if row.get("Submitted via") else None,
            "date_received": pd.to_datetime(row.get("Date received"), errors='coerce').date() if row.get("Date received") else None,
        }
    except Exception as e:
        print(f"Error cleaning row: {row} - {e}")
        return None

def store_data_in_sql(data, connection_params):
    """
    Store the data in MS SQL Server using Windows Authentication.
    """
    conn = None
    try:
        print("Connecting to the database...")
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={connection_params['server']};"
            f"DATABASE={connection_params['database']};"
            "Trusted_Connection=yes;"
        )
        cursor = conn.cursor()

        print("Inserting data into the database...")
        for _, row in data.iterrows():
            cleaned_row = clean_data(row)
            if cleaned_row:
                cursor.execute("""
                    INSERT INTO Complaints (id, product, issue, company, state, submitted_via, date_received)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    cleaned_row["complaint_id"],
                    cleaned_row["product"],
                    cleaned_row["issue"],
                    cleaned_row["company"],
                    cleaned_row["state"],
                    cleaned_row["submitted_via"],
                    cleaned_row["date_received"],
                ))

        conn.commit()
        print("Data stored successfully.")
    except Exception as e:
        print(f"Error storing data in SQL Server: {e}")
    finally:
        if conn:
            conn.close()
def main():
    # Define file paths
    zip_file = "complaints.csv.zip"

    # Download and process the CSV file
    download_csv_file(CSV_URL, zip_file)
    data = extract_and_read_csv(zip_file)

    if data is not None:
        # Preprocess the data (optional cleanup or filtering can be added here)
        print("Processing data for storage...")
        data = data[['Complaint ID', 'Product', 'Issue', 'Company', 'State', 'Submitted via', 'Date received']]

        # Store data in SQL Server
        store_data_in_sql(data, DB_CONNECTION)
    else:
        print("No data to store.")

    # Clean up downloaded files
    if os.path.exists(zip_file):
        os.remove(zip_file)
        print("Temporary files cleaned up.")

if __name__ == "__main__":
    main()