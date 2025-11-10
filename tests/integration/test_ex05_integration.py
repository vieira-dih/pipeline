import importlib
from pyspark.sql import SparkSession

def _spark_iceberg() -> SparkSession:
    return (
        SparkSession.builder
        .appName("it-ex05")
        .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lab.type", "hive")
        .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083")
        .getOrCreate()
    )

def test_ex05_insert_and_count():
    m = importlib.import_module("exercicios.ex05")
    spark = _spark_iceberg()
    n = m.ex05_insert_and_count(spark)
    assert n >= 3
    spark.stop()
