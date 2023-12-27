import datetime
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, col, current_date, split, datediff

spark = SparkSession.builder.appName("login_details").getOrCreate()

data = [
    (1, 101, 'login', '2023-12-29 08:30:00'),
    (2, 102, 'click', '2023-09-06 12:45:00'),
    (3, 101, 'click', '2023-12-25 14:15:00'),
    (4, 103, 'login', '2023-09-08 09:00:00'),
    (5, 102, 'logout', '2023-09-09 17:30:00'),
    (6, 101, 'click', '2023-12-25 11:20:00'),
    (7, 103, 'click', '2023-09-11 10:15:00'),
    (8, 102, 'click', '2023-09-12 13:10:00')
]

login_df = spark.createDataFrame(data=data, schema=["log_id", "user_id", "user_activity", "time_stamp"])
date_df = login_df.withColumn("time_stamp", date_format(col("time_stamp"), "yyyy-MM-dd"))
today_date_df = date_df.withColumn("today_date", current_date())
days_diff = today_date_df.withColumn("days", datediff(col("time_stamp"),col("today_date")))
days_diff.show()
count_df = days_diff.filter(col("days")>=-7).groupBy(col("user_id")).count()
count_df.show()
