from pyspark.sql import SparkSession
from pyspark.sql.functions import StructType,StringType,lower
from pyspark.sql.types import StructField,IntegerType
import re

spark = SparkSession.builder.appName("coalesce").getOrCreate()

schema = StructType([
    StructField("StudentName",StringType(),True),
    StructField("Percentage %",StringType(),True),
    StructField("123Address",StringType(),True),
    StructField("age Employee",IntegerType(),True)
])

data = [("kayal","90","Chennai",24),
        ("Ram","97","Kerala",46),
        ("Raja","85","Chennai",23),
        ("Madhu","70","Bangalore",27),
        ("Shiva","57","Kerala",35),
        ("Ravan","65","Chennai",77)]

stu_df = spark.createDataFrame(data,schema)
df_cols = stu_df.columns
new_cols = []
for i in df_cols:
    i = i.replace(" ","_").replace("%","").replace("_","")
    i = re.sub('([a-z0-9])([A-Z])',r'\1_\2',i).lower()
    i = re.sub(r'(\d)(?=\d)',r'\1_', i)
    new_cols.append(i)
print(new_cols)
updated_stu_df = stu_df.toDF(*new_cols)
updated_stu_df.show()
updated_stu_df = updated_stu_df.withColumnRenamed("1_2_3_address","address")
updated_stu_df.write.mode("overwrite").partitionBy("address").format("delta").save("dbfs:/FileStore/shared_uploads/manigandan.mj@diggibyte.com/address")
