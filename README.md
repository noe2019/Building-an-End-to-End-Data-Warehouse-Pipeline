## **Building an End-to-End Data Pipeline with MS SQL Server**
## **Overview**
This tutorial outlines building an end-to-end data pipeline using **MS SQL Server**. The pipeline covers collecting data from two sources, cleaning and merging the data, and scheduling automatic updates to maintain data freshness. The two (2) data sources: (1)[A Consumer Complaints Data](https://catalog.data.gov/dataset/consumer-complaint-database); (2) [Demographic Data](https://www.kaggle.com/datasets/bitrook/us-county-historical-demographics?select=us_county_demographics.json).

## **Step 1: Setting Up MS SQL Server**

### **1.1 Install MS SQL Server**
1. Download and install **MS SQL Server** from [Microsoft's official site](https://www.microsoft.com/sql-server/).
2. Install **SQL Server Management Studio (SSMS)** to manage your databases.

### **1.2 Create Databases**
- Launch SSMS.
- Connect to your MS SQL Server instance.
- Right-click **Databases** > **New Database** > Name your database (e.g., `DataPipelineDB`).

## **Step 2: Data Collection**

### **2.1 Data Source 1: CSV File**
1. **Load Data to SQL Server**:
   - Use the **Import Flat File Wizard** in SSMS:
     - Right-click on your database.
     - Choose **Tasks > Import Flat File**.
     - Select the CSV file.
     - Define schema mapping for your table.

2. **Verify Imported Data**:
   ```sql
   SELECT * FROM ComplainTable;
   ```

### **2.2 Data Source 2: REST API**
1. **Fetch API Data in Python**:
   - Use Python to query the REST API and load data into MS SQL Server.
   - Example:
     ```python
     import requests
     import pyodbc

     # REST API request
     response = requests.get("https://api.example.com/data")
     data = response.json()

     # MS SQL Server connection
     conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                           'SERVER=your_server;'
                           'DATABASE=DataPipelineDB;'
                           'UID=your_user;'
                           'PWD=your_password')
     cursor = conn.cursor()

     # Insert API data
     for record in data:
         cursor.execute("INSERT INTO Source2Table (col1, col2) VALUES (?, ?)", record['field1'], record['field2'])

     conn.commit()
     conn.close()
     ```

2. **Verify Imported Data**:
   ```sql
   SELECT * FROM Source2Table;
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

## **Summary**
1. Import data from **CSV files** and **REST APIs**.
2. Clean and standardize the data using SQL queries.
3. Merge the datasets into a unified table.
4. Automate updates with SQL Server Agent.
