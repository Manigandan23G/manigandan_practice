# Import packages
from pyspark.sql.functions import StructType,StringType,lower
from pyspark.sql.types import StructField,IntegerType
import re
from delta.tables import DeltaTable

# schema and data
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

# creating dataframe
stu_df = spark.createDataFrame(data,schema)

# optimizing table and z-ordering
%sql
OPTIMIZE student_table ZORDER BY percentage

# cache
cache select * from student_table

# re-order column
alter table student_table change column student_name first
alter table student_table change column student_name after percentage

