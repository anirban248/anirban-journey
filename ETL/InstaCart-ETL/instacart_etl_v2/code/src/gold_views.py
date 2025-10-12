from databricks.connect import DatabricksSession
import yaml
spark = DatabricksSession.builder.serverless(True).getOrCreate()
with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)
db_schema = config['catalog']+'.'+config['schema']

#refreshing to creating a materialized view
creation_sql = f"""
    CREATE OR REPLACE TABLE {db_schema}.mv_reordered_products
    AS SELECT
      product_name,
      aisle_name,
      department,
      COUNT(reordered) AS total_reordered
    FROM {db_schema}.fct_orders A
    join {db_schema}.dim_products B
    ON A.product_id = B.product_id
    where A.reordered = 1
    GROUP BY A.product_id,
    B.product_name,
    B.aisle_name,
    B.department
    """
spark.sql(creation_sql)