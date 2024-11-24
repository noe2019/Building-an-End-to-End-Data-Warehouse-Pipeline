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

`python Complaints.py`

### Script Explanation

1. **Fetching Data**: The `fetch_data` function retrieves data from the API and handles any connection errors.
2. **Data Parsing**: The script parses the JSON response and formats it for database insertion.
3. **Database Insertion**: The `store_data_in_sql` function establishes a connection to MS SQL Server using `pyodbc` and inserts data into the specified table.
4. **Execution**: The `main` function orchestrates the data fetching and storing process.

The snapshot of the corresponding data is provided below:
  ![Dataset 1](Capture20241122140053.png)

### **2.2 Data Source 2: FROM KAGGLE API**
Run`python USDemographics.py`
## **Step 3: Data Cleaning**

### **3.1 Cleaning Operations**
- Standardize column names across both tables.
- Handle missing or null values.
- Deduplicate rows if necessary.

SQL cleaning queries:
`sql MergeSources.sql`

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
