import importlib
from pyspark.sql import SparkSession

def _spark_iceberg() -> SparkSession:
    # O container GRADER injeta os JARs e confs via env/entrypoint
    return (
        SparkSession.builder
        .appName("it-ex04")
        .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lab.type", "hive")  # usando Hive metastore
        .config("spark.sql.catalog.lab.uri", "thrift://hive-metastore:9083")
        .getOrCreate()
    )

def test_ex04_create_namespace_and_table():
    m = importlib.import_module("exercicios.ex04")
    spark = _spark_iceberg()
    m.ex04_create_namespace_and_table(spark)

    tables = spark.sql("SHOW TABLES IN lab.db").collect()
    names = {r.tableName for r in tables}
    assert "pessoas" in names

    spark.stop()
