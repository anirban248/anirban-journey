from databricks.connect import DatabricksSession
from pyspark.sql.functions import current_timestamp
import yaml

spark = DatabricksSession.builder.serverless(True).getOrCreate()
with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)
db_schema = config['catalog']+'.'+config['schema']

#creating product dimension table
prd_query = f"""select A.product_id,A.product_name,a.aisle_id,b.aisle as aisle_name,a.department_id,c.department,current_timestamp() as created_date
        from {db_schema}.products A 
         join {db_schema}.aisles B on
        A.aisle_id =B.aisle_id 
        join {db_schema}.departments C on A.department_id=C.department_id"""

df_products = spark.sql(prd_query)
df_products.write.mode("overwrite").saveAsTable(f"{db_schema}.dim_products")

#creating orders fact table for only prior orders
ord_query = f"""select A.order_id,A.user_id,A.order_number,A.order_dow,A.order_hour_of_day,A.days_since_prior_order,B.product_id, B.add_to_cart_order,B.reordered,
current_timestamp() as created_date from {db_schema}.orders A join
{db_schema}.order_products__prior B 
on A.order_id=B.order_id
join {db_schema}.dim_products C 
on B.product_id=C.product_id
where A.eval_set='prior' """

df_orders = spark.sql(ord_query)
df_orders.write.mode("overwrite").saveAsTable(f"{db_schema}.fct_orders")