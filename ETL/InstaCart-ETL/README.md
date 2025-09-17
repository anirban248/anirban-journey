InstaCart-ETL
================
This project InstaCart-ETL is a comprehensive data engineering 
solution designed to extract, transform, and load (ETL) data 
from the Instacart dataset into a structured format suitable for analysis. 
The project leverages Python and SQL to perform data extraction, 
transformation, and loading processes, ensuring data integrity and consistency.
This project utilizes the following technologies:
- Python: For data extraction, transformation, and loading processes.
- SQL: For database interactions and data manipulation.
- Databricks(community edition): For scalable data processing and analysis.
- GitHub: For version control and collaboration.
- Kaggle: For sourcing the Instacart dataset.

## Source Data
The source data for this project is the Instacart dataset, which includes 
information about customer orders, products, and departments. The dataset is available 
in CSV format in Kaggle and can be found [here](https://www.kaggle.com/datasets/psparks/instacart-market-basket-analysis)

## Requirements

1. The end dataset should show the most reordered products per aisle and department.
2. The ETL process should be able to handle large volumes of data efficiently.
3. The data should be cleaned and transformed to ensure consistency and accuracy.
4. The ETL process should be modular and reusable for future datasets.
5. The project should include documentation and comments for clarity.
6. The project should be deployable on Databricks for scalable processing.

## Variations
This project is implemented in two ways:
> 1. **InstaCart-ETL-V1** - simple implementation of ETL process
>> 1. 
>> 2. Read from files in and create broze tables by overwriting
>> 3. Read from bronze tables, apply transformations and create silver tables by overwriting, 
does not support upsert and hence no update date
>> 4. Silver tables to include facts and dims
>> 5. Aggregated gold to show reordered products per aisle and department with full load everytime
>> 6. Import config for each script
>> 7. No error handling
>> 8. No logging
>> 9. No unit tests
>> 10. use databricks jobs to orchestrate
>> 11. create git folder structure using copier
>> 12. deploy to public repo via CI/CD

> 2. **InstaCart-ETL-V2** - advanced implementation of ETL process *(upcoming)*
>> 1. Introduce incremental load and upsert logic in all bronze and silver
>> 2. Introduce error handling and logging
>> 3. Introduce unit tests
>> 4. Introduce data contracts and validation before loading to silver, pipeline to read and validate from contract
>> 5. Reject from processing if data quality checks fail, create reject tables with notification
>> 6. Use databricks asset bundles to deploy and orchestrate via jobs
>> 7. Use databricks parameters to use common elements like catalog and schema
>> 8. run tests via CI/CD before deployment to public repo

## Project Structure
The project is organized into the following directories and files:
```
├───code
│   ├───contracts
│   ├───src
│   │   └───common
│   └───tests
└───docs
```
- `code/contracts`: Optional - Contains data contract definitions for data validation.
- `code/src`: Contains the main ETL scripts and modules.
- `code/src/common`: Contains common utility functions and modules used across the ETL process.
- `code/tests`: Optional - Contains unit tests for the ETL scripts and modules.
- `docs`: Contains project documentation and design documents.

The above structure has been generated using [Copier](https://copier.readthedocs.io/en/latest/) with a custom template.



<!--refine design doc, update project structure,
move scripts to respective folders, 
test git actions with proper commit messages-->