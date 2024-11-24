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
The heading of the resulting dataset looks like this:
![Snapshot of Dataset 2](Capture20241122135736.png)
## **Step 4: Data Merging**

### **4.1 Combine Data from Both Sources**
- Use **SQL JOIN** to merge the data based on a common key.
- 
```sql
-- Step 1: Create CleanedDemographics Table
-- Create a new table with a different name
CREATE TABLE CleanedDemographics_New (
    zipcode INT,
    major_city NVARCHAR(255),
    state NVARCHAR(50),
    lat FLOAT DEFAULT 0, -- Replace NULL with 0
    lng FLOAT DEFAULT 0, -- Replace NULL with 0
    county NVARCHAR(255),
    population_2010_census INT,
    population_2019 INT,
    population_0_4_2019 INT,
    population_5_9_2019 INT,
    population_10_14_2019 INT,
    population_15_19_2019 INT,
    population_20_24_2019 INT,
    RowID INT
);

-- Insert data into the new table
INSERT INTO CleanedDemographics_New (zipcode, major_city, state, lat, lng, county, 
                                     population_2010_census, population_2019, 
                                     population_0_4_2019, population_5_9_2019, 
                                     population_10_14_2019, population_15_19_2019, 
                                     population_20_24_2019, RowID)
SELECT 
    zipcode,
    major_city,
    state,
    ISNULL(lat, 0) AS lat,
    ISNULL(lng, 0) AS lng,
    county,
    population_2010_census,
    population_2019,
    population_0_4_2019,
    population_5_9_2019,
    population_10_14_2019,
    population_15_19_2019,
    population_20_24_2019,
    ROW_NUMBER() OVER (PARTITION BY zipcode ORDER BY major_city) AS RowID
FROM USCountyDemographicsDetailed
WHERE state IS NOT NULL AND county IS NOT NULL;

-- Step 2: Create CleanedComplaints Table
CREATE TABLE CleanedComplaints (
    id INT,
    product NVARCHAR(255),
    issue NVARCHAR(255),
    company NVARCHAR(255),
    state NVARCHAR(50),
    submitted_via NVARCHAR(50),
    date_received DATE
);

-- Populate the CleanedComplaints table
INSERT INTO CleanedComplaints (id, product, issue, company, state, submitted_via, date_received)
SELECT 
    id,
    product,
    issue,
    company,
    state,
    submitted_via,
    CAST(date_received AS DATE)
FROM Complaints
WHERE state IS NOT NULL AND product IS NOT NULL;

-- Step 3: Remove Duplicates
-- For demographics
WITH CTE_Duplicates AS (
    SELECT 
        RowID,
        ROW_NUMBER() OVER (PARTITION BY zipcode, major_city, state, county ORDER BY RowID) AS RowNum
    FROM CleanedDemographics
)
DELETE FROM CleanedDemographics
WHERE RowID IN (
    SELECT RowID FROM CTE_Duplicates WHERE RowNum > 1
);

-- For complaints
WITH CTE_ComplaintDuplicates AS (
    SELECT 
        id,
        ROW_NUMBER() OVER (PARTITION BY product, issue, company, state ORDER BY id) AS RowNum
    FROM CleanedComplaints
)
DELETE FROM CleanedComplaints
WHERE id IN (
    SELECT id FROM CTE_ComplaintDuplicates WHERE RowNum > 1
);

-- Step 4: Merge Datasets
-- Example: Merge by state
SELECT 
    d.zipcode,
    d.major_city,
    d.state AS demographics_state,
    d.county,
    c.product,
    c.issue,
    c.company,
    c.state AS complaints_state,
    c.date_received
FROM CleanedDemographics d
LEFT JOIN CleanedComplaints c
ON d.state = c.state
WHERE d.state IS NOT NULL;