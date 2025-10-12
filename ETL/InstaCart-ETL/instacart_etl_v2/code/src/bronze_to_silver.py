from databricks.connect import DatabricksSession
from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable
import yaml,logging

spark = DatabricksSession.builder.serverless(True).getOrCreate()
logging.basicConfig(level=logging.INFO)

with open('./common/config.yml', 'r') as f:
    config = yaml.safe_load(f)
db_schema = config['catalog']+'.'+config['schema']

logging.info(f"db_schema is {db_schema}")


#creating product dimension table
try:
    product_query = f"""select A.product_id as product_id,A.product_name as product_name,
                a.aisle_id as aisle_id,b.aisle as aisle_name,
                a.department_id as department_id,c.department as department,
                current_timestamp() as created_date,
                md5(concat(A.product_id,A.product_name,a.aisle_id,b.aisle,a.department_id,c.department)) as product_hash_key
                from {db_schema}.products A 
                join {db_schema}.aisles B on
                A.aisle_id =B.aisle_id 
                join {db_schema}.departments C on A.department_id=C.department_id"""

    df_products = spark.sql(product_query)

    dim_products = DeltaTable.forName(spark,f"{db_schema}.dim_products")

    (dim_products.alias('target').merge(df_products.alias('source'),
                    "source.product_id = target.product_id ").
                    whenMatchedUpdate(condition="source.product_hash_key != target.product_hash_key",set = {"product_name":"source.product_name",
                                            "aisle_id":"source.aisle_id",
                                            "aisle_name":"source.aisle_name",
                                            "department_id":"source.department_id",
                                            "department":"source.department",
                                            "last_modified_date":"source.created_date",
                                            "product_hash_key":"source.product_hash_key"}).
                    whenNotMatchedInsert(values={"product_id":"source.product_id",
                                            "product_name":"source.product_name",
                                            "aisle_id":"source.aisle_id",
                                            "department_id":"source.department_id",
                                            "department":"source.department",
                                            "aisle_name":"source.aisle_name",
                                            "created_date":"source.created_date",
                                            "product_hash_key":"source.product_hash_key",
                                            "last_modified_date":"source.created_date"
                                            }).
                    execute())
except Exception as e:
    logging.error(f"Error in creating product dimension table {e}")
    raise ValueError(f"Error in creating product dimension table {e}")

#creating orders fact table for only prior orders
try:
    ord_query = f"""select A.order_id,
                    A.user_id,
                    A.order_number,
                    A.order_dow,
                    A.order_hour_of_day,
                    A.days_since_prior_order,
                    B.product_id, 
                    B.add_to_cart_order,
                    B.reordered,
                    current_timestamp() as created_date,
                    md5(concat(A.order_id,A.user_id,A.order_number,A.order_dow,A.order_hour_of_day,A.days_since_prior_order,B.product_id,B.add_to_cart_order,B.reordered)) as order_hash_key
                    from {db_schema}.orders A join
                    {db_schema}.order_products__prior B 
                    on A.order_id=B.order_id
                    join {db_schema}.dim_products C 
                    on B.product_id=C.product_id
                    where A.eval_set='prior' """

    df_orders = spark.sql(ord_query)
    fct_orders = DeltaTable.forName(spark,f"{db_schema}.fct_orders")

    (fct_orders.alias('target').merge(df_orders.alias('source'),
                "source.order_id = target.order_id and source.product_id = target.product_id and source.user_id = target.user_id").
                whenMatchedUpdate(condition="source.order_hash_key != target.order_hash_key",set = {"user_id":"source.user_id",
                                        "order_number":"source.order_number",
                                        "order_dow":"source.order_dow",
                                        "order_hour_of_day":"source.order_hour_of_day",
                                        "days_since_prior_order":"source.days_since_prior_order",
                                        "product_id":"source.product_id",
                                        "add_to_cart_order":"source.add_to_cart_order",
                                        "reordered":"source.reordered",
                                        "last_modified_date":"source.created_date",
                                        "order_hash_key":"source.order_hash_key"}).
                whenNotMatchedInsert(values={"order_id":"source.order_id",
                                        "user_id":"source.user_id",
                                        "order_number":"source.order_number",
                                        "order_dow":"source.order_dow",
                                        "order_hour_of_day":"source.order_hour_of_day",
                                        "days_since_prior_order":"source.days_since_prior_order",
                                        "product_id":"source.product_id",
                                        "add_to_cart_order":"source.add_to_cart_order",
                                        "reordered":"source.reordered",
                                        "created_date":"source.created_date",
                                        "order_hash_key":"source.order_hash_key",
                                        "last_modified_date":"source.created_date"
                                        }).
                execute())
except Exception as e:
    logging.error(f"Error in creating orders fact table {e}")
    raise ValueError(f"Error in creating orders fact table {e}")