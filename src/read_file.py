from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("readfile").getOrCreate()

path = "../resource/bank.csv"
bank_df = spark.read.option("header", True).csv(path)

bank_df.show()
bank_df.cache()
bank_df.persist()
