# This is InstaCart ETL project version 1 (InstaCart-ETL-V1) implemented using Databricks.
Below are all the code components included in this project.
## config.yaml
This file contains all the configurations required for the ETL process.

## set_dbr_infra.py
This script sets up the Databricks infrastructure required for the ETL process and contains classes that can be re-used.

## source_to_bronze.py
This script reads the source data files and creates bronze tables by overwriting the existing data.

## bronze_to_silver.py
This script reads from the bronze tables, applies necessary transformations, and creates silver tables by overwriting the existing data. It does not support upsert and hence does not include an update date.

## silver_to_gold.py
This script aggregates data from the silver tables to create a gold table that shows the most reordered products per aisle and department. It performs a full load every time.

## databricks.yml
This file is used to configure Databricks jobs to orchestrate the ETL process.Its downloaded from Databricks after creating jobs for each of the above scripts.

## instacart analysis.ipynb
This Jupyter notebook contains profiling analysis and visualizations of the final gold dataset to derive insights about the most reordered products per aisle and department.