import importlib, os, tempfile
from pyspark.sql import SparkSession

def test_ex03_read_csv_roundtrip():
    spark = SparkSession.builder.master("local[2]").appName("unit-ex03").getOrCreate()

    ex02 = importlib.import_module("exercicios.ex02")
    ex03 = importlib.import_module("exercicios.ex03")

    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "ex3")
        ex02.ex02_save_csv(spark, out)
        df = ex03.ex03_read_csv(spark, out)

        assert set(df.columns) == {"id", "nome"}
        assert df.count() >= 1
    spark.stop()