import yaml
from pyspark.sql import SparkSession

def load_config(config_path):
    """Load YAML configuration"""
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

def create_spark_session(app_name, master="local[*]"):
    """Create and return a Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .getOrCreate()

def load_data(spark, config):
    """Load CSV data as DataFrames based on the provided config"""
    return {
        "charges": spark.read.csv(config['input']['charges'], header=True, inferSchema=True),
        "damages": spark.read.csv(config['input']['damages'], header=True, inferSchema=True),
        "endorse": spark.read.csv(config['input']['endorse'], header=True, inferSchema=True),
        "primary_person": spark.read.csv(config['input']['primary_person'], header=True, inferSchema=True),
        "restrict": spark.read.csv(config['input']['restrict'], header=True, inferSchema=True),
        "units": spark.read.csv(config['input']['units'], header=True, inferSchema=True),
    }
