# Create Volume and Schema names in Databricks space programmatically
1. create a config file calling out volumes, folder and schema under unity catalog
2. Create a python file to create databricks volumes as per config
3. Same for schema
4. use this as common to be used for other projects with config as input
5. import schema and volume names from config in other scripts
# Create tables from files
1. Move file from raw to processing before creating table
2. Use above to create tables one by one from files, add audit columns

> 1. Source_filename - string
>
> 2. create_date - date
> 
> 3. last_update_date - date (only for silver for upsert logic) - skipped for now in bronze as no upsert logic

# Profiling on above tables 
Manual profiling reveals below:
1. No nulls in any column in any file
2. No duplicates in any file
3. No data type issues

# Silver Requirements

1. Enforce a quality rule to include only order with eval set = prior
2. Ensure all product have an aisle and department
3. Add audit columns - source_filename, create_date <!--, last_update_date-->
4. Use appropriate data types
5. Only include order_product__prior, product,aisle,department and orders tables
6. Silver tables are overwritten <!--should follow upsert load strategy eventually-->
7. one dim - product details with aisle and department names
<!--8. Enforce above via a data contract-->

# Gold Requirements
1. Aggregated and overwritten - most reordered products per aisle and department
2. Include aisle and department names
3. Include reorder count
> fact - most reordered products per aisle and department


