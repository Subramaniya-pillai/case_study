# Case Study: Azure → Snowflake with Snowpark → Power BI

##  Scenario

You’re the **data engineer** at *ItTechGenie Retail*.  
Each month, the sales team uploads new CSV files containing sales data to an **Azure Storage Container**.  
Your job is to automate the following data pipeline:

1. **Upload the CSV** file to Azure Storage.
2. **Ingest the data into Snowflake** using **Snowpark**.
3. **Transform and model** it into a proper Snowflake table.
4. **Visualize** the processed data in **Power BI** for business users.

---

##  Step 1: Upload CSV to Azure Storage

The sales team drops the `sales.csv` file into the following container:

```
Storage Account: manistorage21  
Container: manicontainer  
Path: azure://manistorage21.blob.core.windows.net/manicontainer/sales.csv
```

You verified the upload from the **Azure Portal** under *Storage Accounts → Containers → manicontainer*.

---

##  Step 2: Connect Azure Storage to Snowflake

In **Snowflake**, you set up a **Storage Integration** and a **Stage** to connect directly to the Azure container.

### SQL Code in Snowflake

```sql
-- Step 1: Create Storage Integration
CREATE OR REPLACE STORAGE INTEGRATION azure_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = AZURE
ENABLED = TRUE
AZURE_TENANT_ID = 'your tenant id'
STORAGE_ALLOWED_LOCATIONS = ('azure://manistorage21.blob.core.windows.net/manicontainer/');

-- Step 2: Create External Stage
CREATE OR REPLACE STAGE azure_stage_sas
URL = 'azure://manistorage21.blob.core.windows.net/manicontainer/'
CREDENTIALS = (
    AZURE_SAS_TOKEN = 'your token'
)
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);

-- Step 3: Verify Stage Files
LIST @azure_stage_sas;

-- Step 4: Basic Data Checks
SELECT * FROM SALES_DATA_TRANSFORMED LIMIT 10;
DESCRIBE TABLE SALES_DATA_TRANSFORMED;
SELECT COUNT(*) FROM SALES_DATA_TRANSFORMED;
```

 *This confirms that Snowflake can access your Azure files directly using the SAS token.*

---

##  Step 3: Transform Data using Snowpark in Databricks

Next, you used **Databricks** as the compute engine to interact with Snowflake using **Snowpark for Python**.

### 3.1. Install Snowpark

```bash
%pip install "snowflake-snowpark-python[pandas]"
```

### 3.2. Create a Snowflake Session

```python
import os
from snowflake.snowpark import Session

connection_parameters = {
    "account": "dd58593.central-india.azure",
    "user": "vineeth21",
    "password": "your_password",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "HEXAWARE",
    "schema": "PUBLIC"
}

session = Session.builder.configs(connection_parameters).create()
print("Database:", session.get_current_database())
print("Schema:", session.get_current_schema())
```

---

### 3.3. Define CSV Schema and Load Data

```python
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from snowflake.snowpark.functions import to_date, col

csv_schema = StructType([
    StructField("ORDERID", StringType()),
    StructField("ORDERDATE", DateType()),
    StructField("MONTHOFSALE", StringType()),
    StructField("CUSTOMERID", StringType()),
    StructField("CUSTOMERNAME", StringType()),
    StructField("COUNTRY", StringType()),
    StructField("REGION", StringType()),
    StructField("CITY", StringType()),
    StructField("CATEGORY", StringType()),
    StructField("SUBCATEGORY", StringType()),
    StructField("QUANTITY", IntegerType()),
    StructField("DISCOUNT", FloatType()),
    StructField("SALES", FloatType()),
    StructField("PROFIT", FloatType())
])

df = session.read.schema(csv_schema).option("PARSE_HEADER", True).csv("@azure_stage_sas/sales.csv")
df.show(5)
df.print_schema()
```

---

### 3.4. Data Transformation Logic

```python
from snowflake.snowpark.functions import col, to_date, year, month

df_transformed = (
    df
    .with_column("ORDERDATE", to_date(col("ORDERDATE"), "YYYY-MM-DD"))
    .filter((col("SALES") > 0) & (col("PROFIT") > 0))
    .with_column("PROFIT_MARGIN", col("PROFIT") / col("SALES"))
    .with_column("DISCOUNTED_SALES", col("SALES") * (1 - col("DISCOUNT")))
    .with_column("SALE_YEAR", year(col("ORDERDATE")))
    .with_column("SALE_MONTH", month(col("ORDERDATE")))
)

df_transformed.show(5)
df_transformed.print_schema()
```

---

### 3.5. Save Transformed Data to Snowflake

```python
df_transformed.write.save_as_table("SALES_DATA_TRANSFORMED")

print(" Data successfully loaded to Snowflake table: SALES_DATA_TRANSFORMED")
print("Total rows:", df_transformed.count())
```

---

##  Step 4: Power BI Visualization

Finally, connect **Power BI Desktop** to **Snowflake** using the **Snowflake Connector**:

1. Go to **Get Data → Snowflake**.
2. Enter your **Server** (e.g., `dd58593.central-india.azure.snowflakecomputing.com`).
3. Select **HEXAWARE** database and **PUBLIC** schema.
4. Choose the table **SALES_DATA_TRANSFORMED**.
5. Load data and start building visuals.

### Example Dashboards
-  **Total Sales & Profit by Region**
-  **Top Customers by Discounted Sales**
-  **Monthly Sales Trends**
-  **Profit Margin Analysis**

---

##  Final Data Pipeline Summary

| Step | Tool | Purpose |
|------|------|----------|
| 1 | **Azure Storage** | Upload raw monthly CSV files |
| 2 | **Snowflake** | Set up integration and staging |
| 3 | **Snowpark (Python in Databricks)** | Transform, clean, and enrich data |
| 4 | **Power BI** | Visualize business insights |

---

## 🚀 Key Takeaways

- **Snowpark** allows you to perform **server-side data transformations** without moving data outside Snowflake.
- **Azure + Snowflake** integration provides a **secure, scalable ingestion** pipeline.
- **Power BI** connects directly to Snowflake for **real-time analytics**.
- This workflow supports **automation** for new monthly CSV uploads, enabling faster reporting cycles for ItTechGenie Retail.

---

