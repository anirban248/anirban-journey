import yaml
import logging

logging.basicConfig(level=logging.INFO)

with open('./common/config.yml', 'r') as f:
  config = yaml.safe_load(f)

db_schema = config.get('catalog') + '.' + config.get('schema')


with open('../contracts/silver_datacontract.yml','r') as c:
    contracts = yaml.safe_load(c)

#unpacking tables
for t,s in  contracts.get('models').items():
    contract_tbl = db_schema + '.' + t
    logging.info(contract_tbl)
    contract_dtype={}
#unpacking respective column and datatypes
    for c,dt in s.get('fields').items():
      contract_dtype[c]=dt.get('type')
    
    logging.info("contract columns and datatypes")
    logging.info(contract_dtype)
#checking delta table and respective columns and datatypes
    try:
      delta_schema = dict(spark.table(contract_tbl).dtypes)
      logging.info("delta columns and datatypes")
      logging.info(delta_schema)
    except:
      logging.error("Delta table not found")
      raise ValueError("Delta table not found")

    try:
      assert contract_dtype == delta_schema
      logging.info("Schema matched")
    except:
      logging.error("Schema mismatch")
      raise ValueError("Schema mismatch")
 

    
   

