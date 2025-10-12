# Logging mechanism and error handling
1. Use try and except blocks to catch errors
2. Log start and end of each major step
3. Log errors with stack trace for debugging
4. Use a logging library to manage log levels and outputs

# Unit tests
1. bronze: test file count and table counts both should match
2. gold: test final output counts - number of products, aisles, departments

# Read files from raw
1. read full files from raw (autoloader does not work for this use case as files are not landing in directory but is a full file)
2. overwrite bronze tables from files ingested in the current run

# Execute pipeline by enforcing data contract 

## Bronze Data Contract
The contract is stored under `code/contracts/bronze/bronze_datacontract.yml` and contains the expected schema for each bronze table.

## Bronze Data Contract Enforcement
1. Test bronze schema against data contract during deployment (in the real scenario this contract should be provided by data owners and present in upper environments)
2. Deployment should be done via the databricks YML and git actions CI/CD pipeline
3. If failed, halt the pipeline and notify
> Note: For databricks community edition it can be only done via CI/CD due to environment limitations which means this can be tested only at deployments
## Silver Data Contract (this is not a real scenario as silver is never directly served to end users, however this is done to demonstrate the concept)

Silver contract stored at `code/contracts/bronze/silver_datacontract.yml` and contains the expected schema for each silver table.

## Silver Data Contract Enforcement
1. Pipeline to read silver data contract and check silver tables against the contract
2. Check fct_orders does not contain any null product id
3. Fail pipeline if schema check fails, however for data quality checks create quarantine table and reject non-conforming records
4. Notify stakeholders of failure

## Silver pipeline steps
1. If passed, proceed to load silver tables
2. Data Quality checks against silver table as outlined in silver contract
3. Create quarantine table for non-conforming records
4. Reject non-conforming records to quarantine
5. Load confirming records to silver tables

# CI/CD Pipeline
1. Use GitHub actions to create CI/CD pipeline
2. On push to main branch and a particular pattern whl, trigger the pipeline
3. Deploy on databricks using service principal and databricks CLI
4. Run tests post deployment(use unit tests created)
5. Notify via email on success or failure