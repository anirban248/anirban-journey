from databricks.connect import DatabricksSession
from pyspark.sql.functions import lit, current_timestamp
from set_dbr_infra import create_schema_vol
import os, shutil

spark = DatabricksSession.builder.serverless(True).getOrCreate()

infra = create_schema_vol('config.yml')

db_schema = infra.get("files").get("catalog") + "." + infra.get("files").get("schema")
print(db_schema)

raw = infra.get("files").get("paths")[0]
print(raw)

processed = infra.get("files").get("paths")[1]
print(processed)

checkpoint = infra.get("processing").get("paths")[0]
print(checkpoint)

log = infra.get("processing").get("paths")[1]
print(log)

for file in os.listdir(raw):
    if file.endswith(".csv"):
        print(f"Processing file: {file}")
        abs_path = os.path.join(raw, file)
        table = db_schema + '.' + file.replace('.csv', '')
        df = spark.read.option("header", True).option("quote", '"').option("escape", '"').option("inferSchema", True).csv(abs_path)
        df_metadata = df.select("*",lit(abs_path).alias("source_filename"), current_timestamp().alias("created_date"))
        df_metadata.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(table)
        shutil.move(abs_path, processed)

    else:
        print(f"Skipping file as not csv: {file}")
