import importlib
from pyspark.sql import SparkSession

def test_ex01_create_df():
    spark = SparkSession.builder.master("local[2]").appName("unit-ex01").getOrCreate()
    m = importlib.import_module("exercicios.ex01")
    df = m.ex01_create_df(spark)

    assert set(df.columns) == {"id", "nome"}
    data = {(r["id"], r["nome"]) for r in df.collect()}
    assert len(data) == 3
    spark.stop()
