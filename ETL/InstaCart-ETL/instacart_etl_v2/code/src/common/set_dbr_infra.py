from databricks.connect import DatabricksSession
from pyspark.sql.utils import AnalysisException
import os
import yaml


spark = DatabricksSession.builder.serverless(True).getOrCreate()

class CreateSchema:
    def __init__(self, catalog, schema):
        self.catalog = catalog
        self.schema = schema

    def create_schema(self):
        """Create schema and return schema details"""
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema} COMMENT 'schema created programmatically'")
            print(f"Created schema {self.catalog}.{self.schema}")
            return {
                "catalog": self.catalog,
                "schema": self.schema,
                "full_path": f"{self.catalog}.{self.schema}"
            }
        except AnalysisException as e:
            print(f"Error creating schema: {e}")
            return None

class CreateVolumeDirectories(CreateSchema):
    def __init__(self, catalog, schema, volume):
        super().__init__(catalog, schema)
        self.volume = volume
        self.created_paths = []

    def create_volume(self):
        """Create volume and return volume details"""
        schema_info = self.create_schema()
        if schema_info:
            try:
                spark.sql(f"CREATE VOLUME IF NOT EXISTS {self.catalog}.{self.schema}.{self.volume} COMMENT 'volume created programmatically'")
                print(f"Created volume {self.catalog}.{self.schema}.{self.volume}")
                return {
                    **schema_info,
                    "volume": self.volume,
                    "volume_path": f"{self.catalog}.{self.schema}.{self.volume}"
                }
            except AnalysisException as e:
                print(f"Error creating volume: {e}")
                return None
        return None

    def create_directories(self, path):
        """Create directory and return path details"""
        full_path = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{path}"
        try:
            if not os.path.exists(full_path):
                os.makedirs(full_path)
                print(f"Directory {full_path} created")
            else:
                print(f"Directory {full_path} already exists")
            self.created_paths.append(full_path)
            return {"directory_path": full_path}
        except Exception as e:
            print(f"Error creating directory {full_path}: {e}")
            return None

    def get_created_paths(self):
        """Return all created paths"""
        return {
            "catalog": self.catalog,
            "schema": self.schema,
            "volume": self.volume,
            "paths": self.created_paths
        }

def create_schema_vol(config_file="config.yml"):
    """Create schema and volumes from config, return all created resources"""
    created_resources = {"files": None, "processing": None}

    try:
        with open(config_file, 'r') as ymlfile:
            cfg = yaml.safe_load(ymlfile)

        # Create files volume and directories
        files_vol = CreateVolumeDirectories(cfg['catalog'], cfg['schema'], cfg['volume_for_files']['vol_name'])
        vol_info = files_vol.create_volume()
        if vol_info:
            for k, v in cfg['volume_for_files'].items():
                if k != 'vol_name':
                    files_vol.create_directories(v)
            created_resources["files"] = files_vol.get_created_paths()

        # Create processing volume and directories
        proc_vol = CreateVolumeDirectories(cfg['catalog'], cfg['schema'], cfg['volume_for_processing']['vol_name'])
        vol_info = proc_vol.create_volume()
        if vol_info:
            for k, v in cfg['volume_for_processing'].items():
                if k != 'vol_name':
                    proc_vol.create_directories(v)
            created_resources["processing"] = proc_vol.get_created_paths()

        return created_resources

    except Exception as e:
        print(f"Error processing config file: {e}")
        return None

if __name__ == "__main__":
    create_schema_vol()