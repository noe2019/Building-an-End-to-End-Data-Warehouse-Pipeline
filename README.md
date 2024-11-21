## **Building an End-to-End Data Pipeline with MS SQL Server**
## **Overview**
This tutorial outlines building an end-to-end data pipeline using **MS SQL Server**. The pipeline covers collecting data from two sources, cleaning and merging the data, and scheduling automatic updates to maintain data freshness. The two (2) data sources: (1)[Consumer Financial Protection Bureau (CFPB) API](https://catalog.data.gov/dataset/consumer-complaint-database); (2) [Demographic Data](https://www.kaggle.com/datasets/bitrook/us-county-historical-demographics?select=us_county_demographics.json).

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

- Install the required Python libraries:
  ```bash
  pip install requests pyodbc
  ```

- Set up an MS SQL Server database and create a table to store the data. For example:
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

### Python Script

```python
import requests
import pyodbc

# Define the API URL
API_URL = "https://cfpb.github.io/api/ccdb/api.html"

# Database connection parameters
DB_CONNECTION = {
    'server': 'your_server_name',  # Replace with your server name
    'database': 'your_database_name',  # Replace with your database name
    'username': 'your_username',  # Replace with your username
    'password': 'your_password',  # Replace with your password
}

def fetch_data(api_url):
    """
    Fetch data from the API.
    """
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def store_data_in_sql(data, connection_params):
    """
    Store the data in MS SQL Server.
    """
    try:
        # Establish the database connection
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={connection_params['server']};"
            f"DATABASE={connection_params['database']};"
            f"UID={connection_params['username']};"
            f"PWD={connection_params['password']}"
        )
        cursor = conn.cursor()

        # Insert data into the table
        for record in data:
            cursor.execute("""
                INSERT INTO Complaints (id, product, issue, company, state, submitted_via, date_received)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                record.get("complaint_id"),
                record.get("product"),
                record.get("issue"),
                record.get("company"),
                record.get("state"),
                record.get("submitted_via"),
                record.get("date_received"),
            ))

        # Commit the transaction
        conn.commit()
        print("Data stored successfully.")

    except Exception as e:
        print(f"Error storing data in SQL Server: {e}")

    finally:
        # Close the connection
        conn.close()

def main():
    # Fetch data from the API
    print("Fetching data from API...")
    api_data = fetch_data(API_URL)

    if not api_data:
        print("No data retrieved from the API.")
        return

    # Extract relevant data
    records = api_data.get("hits", {}).get("hits", [])
    formatted_data = [
        {
            "complaint_id": record["_id"],
            "product": record["_source"].get("product"),
            "issue": record["_source"].get("issue"),
            "company": record["_source"].get("company"),
            "state": record["_source"].get("state"),
            "submitted_via": record["_source"].get("submitted_via"),
            "date_received": record["_source"].get("date_received"),
        }
        for record in records
    ]

    # Store data in SQL Server
    print("Storing data in MS SQL Server...")
    store_data_in_sql(formatted_data, DB_CONNECTION)

if __name__ == "__main__":
    main()
```

### Script Explanation

1. **Fetching Data**: The `fetch_data` function retrieves data from the API and handles any connection errors.
2. **Data Parsing**: The script parses the JSON response and formats it for database insertion.
3. **Database Insertion**: The `store_data_in_sql` function establishes a connection to MS SQL Server using `pyodbc` and inserts data into the specified table.
4. **Execution**: The `main` function orchestrates the data fetching and storing process.

### Notes
- Replace placeholders (`your_server_name`, `your_database_name`, etc.) with actual values.
- The script assumes the API response structure is consistent with the example provided. Adjust fields as necessary.
- Ensure the `ODBC Driver 17 for SQL Server` is installed on your system.

### **2.2 Data Source 2: REST API**
```python
import pandas as pd
from sqlalchemy import create_engine
import json

# Step 1: Load the dataset
file_path = "path_to_your_file/us_county_demographics.json"  # Replace with the actual file path
try:
    # Load the JSON file into a pandas DataFrame
    with open(file_path, 'r') as f:
        data_json = json.load(f)
    data = pd.DataFrame(data_json)
    print("Dataset loaded successfully!")
except Exception as e:
    print(f"Error loading dataset: {e}")
    exit()

# Step 2: Define the SQL Server connection details
server = 'your_server_name'  # Replace with your server name, e.g., 'localhost\SQLEXPRESS'
database = 'your_database_name'  # Replace with your database name
username = 'your_username'  # Replace with your SQL Server username
password = 'your_password'  # Replace with your SQL Server password

# Connection string for SQL Server
connection_string = (
    f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
)

# Step 3: Create the SQLAlchemy engine
try:
    engine = create_engine(connection_string)
    print("Successfully connected to SQL Server!")
except Exception as e:
    print(f"Error connecting to SQL Server: {e}")
    exit()

# Step 4: Write data to SQL Server as a table
table_name = 'us_county_demographics'  # Define your target table name
try:
    data.to_sql(table_name, con=engine, if_exists='replace', index=False)  # Save to table
    print(f"Data successfully saved to the table '{table_name}' in SQL Server.")
except Exception as e:
    print(f"Error writing data to SQL Server: {e}")

# Step 5: Close the connection
engine.dispose()
print("Connection closed.")
```

### Key Notes:
1. **JSON Parsing**: The script explicitly reads the JSON file using Python's `json` module and converts it into a `pandas.DataFrame`. Ensure the JSON structure is compatible for direct conversion into a tabular format.

2. **Table Creation**:
   - The `if_exists='replace'` parameter in `to_sql` ensures that if the table already exists, it will be dropped and recreated. Change it to `if_exists='append'` to add data to an existing table without deleting it.

3. **MS SQL Server Connection**:
   - Install the ODBC Driver for SQL Server if not already installed: [ODBC Driver 17 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).
   - Ensure your database and credentials are properly configured in SQL Server.

4. **Verification in SQL Server**:
   - Once the script runs successfully, you can query the new table in SQL Server to ensure data has been imported:
     ```sql
     SELECT TOP 10 * FROM us_county_demographics;
     ```

5. **Error Handling**:
   - The script includes error handling for JSON file loading, database connection issues, and table creation failures to assist in debugging.

### Example Result
When the script is executed, the table `us_county_demographics` will be created in the specified database, with data from the JSON file stored in tabular format. The columns will match the keys in the JSON data, and each row will represent an entry in the JSON dataset.
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

## **Summary**
1. Import data from **CSV files** and **REST APIs**.
2. Clean and standardize the data using SQL queries.
3. Merge the datasets into a unified table.
4. Automate updates with SQL Server Agent.
