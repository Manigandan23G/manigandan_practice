from pyspark.sql.functions import StructType,LongType,StringType
from pyspark.sql.types import StructField
from delta import *

schema = StructType([StructField("age",LongType(),True),
                     StructField("city",StringType(),True),
                     StructField("friends",ArrayType(
                         StructType([StructField("hobbies",ArrayType(StringType()),True),
                                     StructField("name",StringType(),True)])
                                                    ),True),
                     StructField("id",LongType(),True),
                     StructField("name",StringType(),True)])
df =  spark.read.schema(schema).json("dbfs:/FileStore/Manigandan/source_files/users_100.json")
df.write.mode("overwrite").partitionBy("city").parquet("dbfs:/FileStore/Manigandan/source_files/partitionby1")
df.rdd.getNumPartitions()
df.display()
df1 = df.select(df.id,df.name,df.age,df.city,explode(df.friends).alias("exld_frd"))
df2 = df1.select(df1.id,df1.name,df1.age,df1.city,col("exld_frd.hobbies").alias("hobbies"),col("exld_frd.name").alias("frd_name"))
path = "dbfs:/FileStore/Manigandan/source_files/silver/"
q_path = "dbfs:/FileStore/Manigandan/quarantine"
df_list = ["id","frd_name"]
merge_col = ["id"]
table_name = "user_2"
db_name = "mani"
data = [(100,"kayal",25,"Africa",["kal"],"mani"),(None,"meera",22,"a",["lal"],"ni"),(101,"meera",22,"a",["lal"],None),(None,"Dhilshat",27,"Guindy",["pal"],None)]
df4 = spark.createDataFrame(data = data, schema = ["id","name","age","city","hobbies","frd_name"])


result_df = df2.union(df4)
result_df.display()
def get_not_null_records(df,df_list):
    # df5 = df.filter(col("id").isNull() | col("frd_name").isNull())
    check_not_null = " & ".join(f'''((col("{col}").isNotNull()) | (col("{col}") != ""))''' for col in df_list)
    df_not_null = df.filter(eval(check_not_null))
    return df_not_null
def get_null_records(df,q_path,df_list):
    # df5 = df.filter(col("id").isNull() | col("frd_name").isNull())
    check_null = " | ".join(f'''((col("{col}").isNull()) | (col("{col}") == ""))''' for col in df_list)
    df_null = df.filter(eval(check_null))
    if(df_null.count() > 0):
        df_null.write.format("delta").mode("overwrite").option("path", q_path).saveAsTable("not_null_and_nul_check")
    return df_null
def save_as_table(df,q_path,path,df_list,table_name,db_name,merge_col):
    null_records = get_null_records(df,q_path,df_list)
    df_not_null = get_not_null_records(result_df,df_list)

    base_path = path + f"{table_name}"
    if not DeltaTable.isDeltaTable(spark, f"{base_path}"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        df_not_null.write.mode("overwrite") \
        .format("delta") \
        .option("path", base_path) \
        .saveAsTable(f"{db_name}.{table_name}")
    else:
        deltaTable = DeltaTable.forPath(spark, f"{base_path}")
        matchKeys = " AND ".join("old." + col + " = new." + col for col in merge_col)
        deltaTable.alias("old") \
        .merge(df_not_null.alias("new"), matchKeys) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    return df_not_null

save_as_table(result_df,q_path,path,df_list,table_name,db_name,merge_col)
df = DeltaTable.forPath(spark, "dbfs:/FileStore/Manigandan/quarantine")
df2 = spark.read.format("delta").load("dbfs:/FileStore/Manigandan/quarantine")
df1 = df.toDF()
df2.display()
