from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, length, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("readfile").getOrCreate()

path = "../resource/bank.csv"
bank_df = spark.read.option("header", True).csv(path)


def secret_number(s):
    s = s
    l = len(s)
    s1 = len(s[0:l - 4])
    s2 = ""
    for i in range(0, s1):
        s2 = "*" + s2
    s2 = s2 + s[l - 4:l]
    return s2


hidden_values = udf(secret_number, StringType())

bank_df = bank_df.withColumn("Acc_number", lit("9878654500985246"))
hidden_df = bank_df.withColumn("Masked-number", hidden_values(col("Acc_number")))
hidden_df.show()
