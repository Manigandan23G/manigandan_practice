from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("coalesce").getOrCreate()

data = [(None, None,None),
        (None, 'b', 'c'),
        (2, None, 'e')]
schema = ['A', 'B', 'C']

df = spark.createDataFrame(data,schema)
df.show()
df = df.withColumn("A",coalesce(col("A"),col("B"),col("C"),lit(-1)))
df.show()