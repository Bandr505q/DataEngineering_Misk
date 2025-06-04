🏁 Final Project: Build an End-to-End ETL Pipeline 🎯 Objective: Design and implement a simple ETL (Extract, Transform, Load) pipeline using real-world data. The pipeline should extract data from a raw source, clean and transform it, and load it into a target destination (e.g., a CSV file, SQLite database, or simulated data warehouse). Automation using Airflow is encouraged for intermediate students.

🧩 Project Overview: Scenario (make up your own) You work as a junior data engineer at a retail analytics company. Your task is to build a data pipeline that processes daily sales data from an e-commerce site and makes it available for business analysts to use in dashboards and reports.

🗂️ Suggested Tech Stack: Python for data handling and scripting

Pandas for transformations

SQL / SQLite for loading structured data

Apache Spark for scalable data processing (if dataset is large)

Airflow for automation (optional but encouraged)

Jupyter Notebook for documentation & development

🧪 Dataset Sources Choose one public dataset from any of the following sources (or bring your own if approved by the instructor):

Kaggle Datasets

Data.gov

Our World in Data

OpenML

https://data.gov.sa/

https://open.data.gov.sa/ar/home

https://dp.stats.gov.sa/dashboard

Note : Preferably the data has more than 500 rows and 5 columns or more

📋 Requirements:

🔹 Phase 1: Extract Load raw data from a CSV, JSON, etc... or public API (use requests or pandas.read_*)

Validate schema and shape of raw data

Log the extraction timestamp and source

🔹 Phase 2: Transform Clean the dataset:

Handle missing values

Convert data types

Remove duplicates

Apply business logic:

Calculate KPIs like revenue = price * quantity

Create new columns (e.g., customer segment, product category)

Normalize or aggregate data if needed (e.g., group by region or date)

Use Pandas (and optionally Spark) to perform these transformations

🔹 Phase 3: Load Load the cleaned data into:

SQLite database (recommended for simplicity) OR

Write to a .csv or .parquet file (simulate a data lake)

Write an SQL script or use SQLAlchemy to create and populate target tables

🔹 (Optional) Phase 4: Automate Create an Airflow DAG that automates the ETL process:

extract() task

transform() task

load() task

Use BashOperator or PythonOperator

🗃️ Deliverables: etl_pipeline.ipynb or .py script

Cleaned dataset (CSV/DB)

Project README file or markdown explanation within notebook

SQL schema or .db file (if using SQLite)

Optional: etl_dag.py if using Airflow

🧠 Evaluation Criteria:

Extraction successfully loads raw data from source 10
Transformation Cleaning, formatting, adding logic columns, and documentation 20
Loading Final dataset correctly loaded into target format or DB 10
Optional Automation Airflow DAG runs successfully (optional but gives bonus) +5
Code & Structure Clean, modular code with reusable functions, comments, and clear logic 5
Documentation Clear README/markdown explanation of each step and choices 5
Presentation Final short walkthrough or explanation during in-class demo 5
Total 55 (+5 bonus)
