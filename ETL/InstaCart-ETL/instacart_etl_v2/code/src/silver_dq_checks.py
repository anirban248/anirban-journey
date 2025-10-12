import yaml
import logging

logging.basicConfig(level=logging.INFO)

with open('./common/config.yml', 'r') as f:
  config = yaml.safe_load(f)

db_schema = config.get('catalog') + '.' + config.get('schema')


with open('../contracts/silver_datacontract.yml','r') as c:
    contracts = yaml.safe_load(c)

for t,c in contracts.get('models').items():
    val = c.get('quality')
    if val:
        try:
            result = spark.sql(val.get('query')).collect()[0][0]
            assert result == val.get('mustBe')
            logging.info(f"quality check passed for {t}")
        except:
            logging.error(f"quality check failed for {t} - expected count mismatched ")
            error_records = spark.sql("SELECT * FROM data_engineering.instacart_pipeline.fct_orders WHERE product_id is null")
            error_audit = spark.sql("SELECT coalesce(max(batch_id),0) + 1 as batch_id,current_timestamp() as runtimestamp,'null product id' as error_message FROM data_engineering.instacart_pipeline.error_fct_orders")
            final_df = error_audit.join(error_records, how="cross")
            final_df.write.mode('append').saveAsTable('data_engineering.instacart_pipeline.error_fct_orders')
            spark.sql("delete from data_engineering.instacart_pipeline.fct_orders where product_id is null")
    else: 
        pass
        
    
    
