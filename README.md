## **Building an End-to-End Data Pipeline with MS SQL Server**
## **Overview**
This project outlines building an end-to-end data pipeline using **MS SQL Server**. The pipeline covers collecting data from two sources, cleaning and merging the data, and scheduling automatic updates to maintain data freshness. The two (2) data sources: (1)[Consumer Financial Protection Bureau (CFPB) API](https://catalog.data.gov/dataset/consumer-complaint-database); (2) [Demographic Data](https://www.kaggle.com/datasets/bitrook/us-county-historical-demographics?select=us_county_demographics.json).

## **Step 1: Setting Up MS SQL Server**

### **1.1 Install MS SQL Server**
1. Download and install **MS SQL Server** from [Microsoft's official site](https://www.microsoft.com/sql-server/).
2. Install **SQL Server Management Studio (SSMS)** to manage your databases.

### **1.2 Create Databases**
- Launch SSMS.
- Connect to your MS SQL Server instance.
- Right-click **Databases** > **New Database** > Name your database (e.g., `DataPipelineDB`).

## **Step 2: Data Collection**

### **2.1 Data Source 1:**
To collect data from the Consumer Financial Protection Bureau (CFPB) API and store it in an MS SQL Server database, you can use a Python script. This script will:

1. Fetch data from the API.
2. Parse the JSON response.
3. Insert the data into an MS SQL Server database.

Here is a complete Python script for this purpose:

### Prerequisites
- Install Python 3.x and `ODBC Driver 17 for SQL Server` on your system
- Install the required Python libraries:
  ```bash
  pip install requests pyodbc
  ```
- Create the `Complaints` Table in the `DataPipelineDB` database:
 ```sql
  CREATE TABLE Complaints (
      id INT PRIMARY KEY,
      product NVARCHAR(255),
      issue NVARCHAR(MAX),
      company NVARCHAR(255),
      state NVARCHAR(50),
      submitted_via NVARCHAR(255),
      date_received DATE
  );
  ``` 

## Authentication Options
There are three (3) MS SQL SERVER authentication options: **Windows Authentication**, **SQL Server Authentication**, and **Mixed Mode Authentication**.

### 1. **Windows Authentication**
Use the current Windows user to connect. Update `DB_CONNECTION`:
```python
DB_CONNECTION = {
    'server': r'YourServerName\Instance',
    'database': 'YourDatabaseName'
}
```
Use `Trusted_Connection=yes` in the connection string.

**Use Case**: For environments where users are managed via Active Directory.

**Setup**: No configuration changes if SQL Server is set to "Windows Authentication Mode".

### 2. **SQL Server Authentication**
Authenticate with a SQL Server username and password:
```python
DB_CONNECTION = {
    'server': r'YourServerName\Instance',
    'database': 'YourDatabaseName',
    'username': 'YourUsername',
    'password': 'YourPassword'
}
```

**Use Case**: When access is needed without a Windows user (e.g., cross-platform applications).

**Setup**: 
1. Enable "Mixed Mode Authentication" in SSMS: 
   - Right-click Server → **Properties** → **Security** → Select "SQL Server and Windows Authentication Mode".
2. Create a SQL Server login:
   ```sql
   CREATE LOGIN YourUsername WITH PASSWORD = 'YourPassword';
   CREATE USER YourUsername FOR LOGIN YourUsername;
   EXEC sp_addrolemember 'db_datareader', YourUsername;
   EXEC sp_addrolemember 'db_datawriter', YourUsername;
   ```

### 3. **Mixed Mode Authentication**
Supports both Windows and SQL Server Authentication.

**Use Case**: For flexibility to allow both approaches.

**Setup**: 
1. Enable "SQL Server and Windows Authentication Mode" in SSMS as above.
2. Use either Windows or SQL credentials as needed.

## Usage
1. Configure `DB_CONNECTION` based on your authentication choice.
2. Run the script:
   ```bash
   python script.py
   ```
This project uses the `Windows Authentication Mode`. The `SQL Server Authentication Mode` is generally used in industry settings.

### Python Script

```python
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
```

### Script Explanation

1. **Fetching Data**: The `fetch_data` function retrieves data from the API and handles any connection errors.
2. **Data Parsing**: The script parses the JSON response and formats it for database insertion.
3. **Database Insertion**: The `store_data_in_sql` function establishes a connection to MS SQL Server using `pyodbc` and inserts data into the specified table.
4. **Execution**: The `main` function orchestrates the data fetching and storing process.

### Note
- The script assumes the API response structure is consistent with the example provided. Adjust fields as necessary.

### **2.2 Data Source 2: FROM KAGGLE API**
```python
import kagglehub
import pandas as pd
import os
import pyodbc
import time

def download_with_retries(dataset_handle, max_retries=3, wait_time=10):
    """
    Download Kaggle dataset with retry logic.
    """
    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1} of {max_retries}: Downloading dataset...")
            dataset_path = kagglehub.dataset_download(dataset_handle)
            print("Dataset downloaded successfully!")
            return dataset_path
        except Exception as e:
            print(f"Download failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print("Max retries reached. Exiting.")
                raise

# Step 1: Download dataset with retry logic
try:
    dataset_handle = "bitrook/us-county-historical-demographics"
    dataset_path = download_with_retries(dataset_handle)
except Exception as e:
    print(f"Failed to download dataset: {e}")
    exit()

# Step 2: Locate CSV file in the downloaded dataset folder
csv_file = None
for file in os.listdir(dataset_path):
    if file.endswith(".csv"):
        csv_file = os.path.join(dataset_path, file)
        break

if not csv_file:
    print("No CSV file found in the downloaded dataset.")
    exit()

# Step 3: Load dataset into a Pandas DataFrame
print("Loading dataset into a DataFrame...")
data = pd.read_csv(csv_file)

# Step 4: Clean column names
print("Cleaning dataset...")
data.columns = [col.strip().replace(" ", "_") for col in data.columns]

# Step 5: Database connection parameters for MS SQL Server
DB_CONNECTION = {
    'server': 'Your-server-name\Instance',
    'database': 'Your-database-name',
    'trusted_connection': 'yes'
}

# Step 6: Connect to SQL Server and create "Demographic" table
try:
    print("Connecting to MS SQL Server database...")
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DB_CONNECTION['server']};"
        f"DATABASE={DB_CONNECTION['database']};"
        f"Trusted_Connection={DB_CONNECTION['trusted_connection']};"
    )
    cursor = conn.cursor()

    # Create the table if it does not exist
    print("Creating table 'Demographic' if it doesn't exist...")
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Demographic' AND xtype='U')
    CREATE TABLE Demographic (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        County_Name NVARCHAR(255),
        State NVARCHAR(255),
        Year INT,
        Population INT,
        Median_Age FLOAT,
        Median_Income FLOAT
    );
    """
    cursor.execute(create_table_query)
    print("Table 'Demographic' is ready.")

    # Insert data into the table
    print("Inserting data into the 'Demographic' table...")
    for _, row in data.iterrows():
        insert_query = """
        INSERT INTO Demographic (County_Name, State, Year, Population, Median_Age, Median_Income)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_query, (
            row.get("County_Name"),
            row.get("State"),
            row.get("Year"),
            row.get("Population"),
            row.get("Median_Age"),
            row.get("Median_Income")
        ))

    conn.commit()
    print("Data inserted successfully.")

except Exception as e:
    print(f"Database error: {e}")

finally:
    if conn:
        cursor.close()
        conn.close()
        print("Database connection closed.")

# Cleanup: Optional
print("Cleaning up temporary files...")
if os.path.exists(dataset_path):
    for file in os.listdir(dataset_path):
        os.remove(os.path.join(dataset_path, file))
    os.rmdir(dataset_path)
print("Script completed.")
```
## **Step 3: Data Cleaning**

### **3.1 Cleaning Operations**
- Standardize column names across both tables.
- Handle missing or null values.
- Deduplicate rows if necessary.

Example SQL cleaning queries:
```sql
-- Rename columns for consistency
EXEC sp_rename 'Source1Table.OldColumnName', 'NewColumnName', 'COLUMN';

-- Handle null values
UPDATE Source1Table
SET ColumnName = 'DefaultValue'
WHERE ColumnName IS NULL;

-- Remove duplicates
WITH CTE AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY UniqueColumn ORDER BY ID) AS RowNum
    FROM Source1Table
)
DELETE FROM CTE WHERE RowNum > 1;
```

## **Step 4: Data Merging**

### **4.1 Combine Data from Both Sources**
- Use **SQL JOIN** to merge the data based on a common key.

Example:
```sql
SELECT 
    s1.ID, 
    s1.ColumnA, 
    s2.ColumnB 
INTO MergedTable
FROM Source1Table s1
JOIN Source2Table s2
ON s1.ID = s2.ForeignID;
```

- Validate the merged data:
   ```sql
   SELECT * FROM MergedTable;
   ```

## **Step 5: Automating Updates**

### **5.1 Create a SQL Job in SSMS**
1. Open **SQL Server Agent** in SSMS.
2. Right-click **Jobs** > **New Job**.
3. Configure:
   - **Step 1**: Define the data update script.
     ```sql
     -- Example: Refresh data in MergedTable
     DELETE FROM MergedTable;
     INSERT INTO MergedTable
     SELECT s1.ID, s1.ColumnA, s2.ColumnB
     FROM Source1Table s1
     JOIN Source2Table s2
     ON s1.ID = s2.ForeignID;
     ```
   - **Step 2**: Schedule the job (e.g., daily at 1 AM).

4. Save and enable the job.

## **Step 6: Monitoring**
- Use the **SQL Server Agent Logs** and query execution plans to monitor and optimize performance.
