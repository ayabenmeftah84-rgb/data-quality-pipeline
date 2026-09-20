from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
df = spark.createDataFrame([(1, "a"), (2, None)], ["id", "val"])
df.show()
spark.stop()
