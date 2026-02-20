# 🚀 Data Pipeline: E-commerce Streaming & Medallion Architecture

## 🛠️ Prerequisites

To run and test this project, you will need one of the following environments configured:

* **Development Environment:** VS Code with the Databricks extension installed and configured via **Databricks Connect**.
* **Execution Environment:** An active Databricks account (the **Community/Free Edition** can be used for logic testing, although full support for DLT and Bundles is available in **Premium/Enterprise** instances).
* **Databricks CLI:** Installed and authenticated to perform Asset Bundle (DABs) deployment.

## 1. Project Description
This project implements an end-to-end data pipeline focused on e-commerce, utilizing **Delta Live Tables (DLT)** for real-time processing (Streaming). The system is designed to scale from raw data ingestion to the delivery of refined layers for analysis, ensuring data integrity and governance.

## 2. Objective
The primary goal is to demonstrate the implementation of the **Medallion Architecture** and **Change Data Capture (CDC)** within Databricks, automating infrastructure creation via **Databricks Asset Bundles (DABs)** and ensuring data quality at every stage of the flow.

## 3. Medallion Architecture
Data organization follows the Medallion standard, ensuring that quality increases as data flows through the layers:

![Medallion Architecture](./Images/Medallion_architecture.png)

| Layer | Purpose | Data Quality (Expectations) |
| :--- | :--- | :--- |
| **Bronze** | Raw ingestion via Auto Loader | **WARN**: Monitor inconsistencies |
| **Silver** | Validation, cleansing, and enrichment | **DROP**: Automatically remove invalid data |
| **Gold** | Business KPIs and final aggregations | **FAIL**: Strict and critical validation |

### 3.1 Unity Catalog
**Unity Catalog** serves as the unified governance layer for the Lakehouse.

![Unity Catalog](./Images/Unity_catalog.png)

It enables centralized management of data lineage, access control (ACLs), and auditing of all data assets (tables, volumes, and functions) at the metastore level.

## 4. Technologies Used
* **Databricks Delta Live Tables (DLT):** Declarative orchestration and pipeline governance.
* **Spark Structured Streaming:** Scalable real-time processing.
* **Unity Catalog:** Unified governance and data security.
* **Python & SQL:** Development of transformations and business logic.
* **Change Data Capture (CDC):** Efficient capture of changes in dimensional tables (Customers, Products).
* **Auto Loader:** Incremental and efficient file ingestion into the Bronze layer.
* **Databricks Asset Bundles (DABs):** Infrastructure as Code (IaC) tool for deployment and CI/CD.

## 📊 Monitoring and Execution

Once the processes are started, you can monitor the execution status directly within the Databricks interface. Below are the expected visualizations for each stage:

### Job Execution (Orchestration)
The Job coordinates bundle execution and triggers the data flows.
![Job Execution](./Images/log_job.png)

### Medallion Pipeline (Streaming DLT)
Visualization of the dependency graph (DAG) processing Bronze and Silver layer data in real-time.
![Medallion Pipeline](./Images/pipeline_medallion.png)

### CDC Pipeline (Change Data Capture)
Change synchronization flow to keep dimensional tables up to date.
![CDC Pipeline](./Images/pipeline_cdc.png)

---

## 5. Project Structure
```text
src/
├── pipelines/
│   ├── bronze/          # Initial ingestion scripts (Raw)
│   ├── cdc/             # CDC processing logic
│   └── silver/          # Transformations and enrichment
├── setup/               # Infrastructure and permissions scripts
│   ├── run.py           # Initial setup orchestrator
│   └── ...
└── utils/               # Utilities and data generator
    └── data_generator.py
```

## 6. How to Run the Project
Follow the steps below to configure the environment and perform the deployment

### Step 1: Clone the Repository
```
git clone https://github.com/GabrielGalani/databricks-lakeflow-streamlab.git
cd databricks-lakeflow-streamlab
```

### Step 2: Initial Configuration (Setup)
Run the setup script to prepare the Unity Catalog (schemas, volumes, and permissions):
```python
./src/setup/run.py
```

### Step 3: Deploy via Databricks Asset Bundles
Ensure the Databricks CLI is configured and run the deploy command to automatically create the resources (Workflows and DLT):

Step 4: Pipeline Execution
With the deployment complete, execute the pipelines in the following order of dependency:
- Data Generator: To populate volumes with initial data.
- Medallion: Ingestion and transformation of Bronze and Silver layers.
- CDC: Updating dimensions based on captured changes.
