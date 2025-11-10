import importlib, os, tempfile
from pyspark.sql import SparkSession

def test_ex02_save_csv():
    spark = SparkSession.builder.master("local[2]").appName("unit-ex02").getOrCreate()
    m = importlib.import_module("exercicios.ex02")
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "ex2")
        m.ex02_save_csv(spark, out)

        # Deve existir pelo menos 1 part file e _SUCCESS
        files = os.listdir(out)
        assert any(f.endswith(".csv") or f.startswith("part-") for f in files)
        assert "_SUCCESS" in files
    spark.stop()
