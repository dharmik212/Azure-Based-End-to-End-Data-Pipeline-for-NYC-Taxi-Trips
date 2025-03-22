# 🚖 Azure Taxi Data Pipeline (Bronze → Silver → Gold)

This project demonstrates an **end-to-end Data Engineering pipeline** built using **Azure Services** to process **Taxi Data from an API**. The pipeline follows a **Medallion Architecture (Bronze, Silver, Gold)** and enables **data versioning, time-travel, and reporting with Delta Lake**.

---

## **🛠️ Tech Stack**
- **Azure Data Factory** (Ingestion)
- **Azure Data Lake Gen2** (Storage)
- **Databricks** (Transformation & Delta Lake)


---

## **📌 Architecture**
**Pipeline Diagram** <img width="940" alt="Project architecture" src="https://github.com/user-attachments/assets/4f158a9d-412c-4299-ad1d-9259021c4c64" />  

---

## **📂 Pipeline Breakdown**
### **🔹 Ingestion Layer (Bronze)**
- Used **Azure Data Factory V2** to ingest data from a **Taxi Data API**.
- Created **pipelines using `ForEach`** activity to handle multiple API links dynamically.
- Stored raw **Parquet files in the Bronze Layer** (`/mnt/bronze`).

### **🔸 Transformation Layer (Silver)**
- Processed data in **Databricks Notebooks**:
  - Used **Spark transformations** like `split()`, `date formatting`, and `column renaming`.
  - Saved transformed data in **Silver Layer (`/mnt/silver`)**.

### **🏅 Serving Layer (Gold)**
- Converted **Parquet files to Delta Tables** for:
  - **Versioning** (`DESCRIBE HISTORY`)
  - **Time Travel** (`SELECT * FROM table VERSION AS OF <version_number>`)
- Stored final **optimized & aggregated data** in **Gold Layer (`/mnt/gold`)**.

---

## **💾 Delta Table Commands Used**
```sql
-- Create Delta Table
CREATE TABLE gold.trip_zone_delta
USING DELTA
LOCATION '/mnt/gold/trip_zone_delta';

-- Check Delta Table Versions
DESCRIBE HISTORY gold.trip_zone_delta;

-- Time Travel (Retrieve old version)
SELECT * FROM gold.trip_zone_delta VERSION AS OF 1;
