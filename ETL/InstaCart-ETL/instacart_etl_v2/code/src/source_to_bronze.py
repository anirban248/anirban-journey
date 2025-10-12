from databricks.connect import DatabricksSession
from pyspark.sql.functions import lit, current_timestamp
from common.set_dbr_infra import create_schema_vol
import os, shutil, logging


spark = DatabricksSession.builder.serverless(True).getOrCreate()

logging.basicConfig(level=logging.INFO)

try:
    infra = create_schema_vol('./common/config.yml')
    logging.info(f"infra details is {infra}")
except Exception as e:
    logging.exception(f"Error creating infra: {e}")

db_schema = infra.get("files").get("catalog") + "." + infra.get("files").get("schema")
logging.info(f"db schema is {db_schema}")

raw = infra.get("files").get("paths")[0]
logging.info(f"raw path:{raw}")

processed = infra.get("files").get("paths")[1]
logging.info(f"processed path:{processed}")

checkpoint = infra.get("processing").get("paths")[0]
logging.info(f"checkpoint path:{checkpoint}")

log = infra.get("processing").get("paths")[1]
logging.info(f"log path:{log}")
logging.info(f"directory content:{os.listdir(raw)}")

for file in os.listdir(raw):
    try:
        if file.endswith(".csv"):
            logging.info(f"Processing file: {file}")
            abs_path = os.path.join(raw, file)
            table = db_schema + '.' + file.replace('.csv', '')
            df = spark.read.\
            option("header", True).\
            option("quote", '"').\
            option("escape", '"').\
            option("inferSchema", True).\
            csv(abs_path)

            df_metadata = df.select("*",lit(abs_path).alias("source_filename"), current_timestamp().alias("created_date"))

            df_metadata.write.format("delta").\
            mode("overwrite").\
            option("mergeSchema", "true").\
            saveAsTable(table)

            shutil.move(abs_path, processed)

        else:
            logging.WARNING(f"Skipping file as not csv: {file}")
    except Exception as e:
        logging.exception(f"Error processing file: {file} - {e}")
