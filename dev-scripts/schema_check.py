from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from datetime import datetime, date
import pandas as pd

spark = SparkSession.builder.getOrCreate()

data1 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema1 = StructType([
    StructField("firstname",StringType(),True),
    StructField("middlename",StringType(),True),
    StructField("lastname",StringType(),True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
  ])

data2 = [("James","","Smith","36636",3000,"M"),
    ("Michael","Rose","","40288",4000,"M"),
    ("Robert","","Williams","42114",4000,"M"),
    ("Maria","Anne","Jones","39192",4000,"F"),
    ("Jen","Mary","Brown","",-1,"F")
  ]

schema2 = StructType([
    StructField("firstname",StringType(),True),
    StructField("middlename",StringType(),True),
    StructField("lastname",StringType(),True),
    StructField("id", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("gender", StringType(), True)
  ])


df_work = spark.createDataFrame(data=data1, schema=schema1)

df_work2 = spark.createDataFrame(data=data2, schema=schema2)

df_work.printSchema()
df_work.schema

df_work2.printSchema()
df_work2.schema

if df_work.schema == df_work2.schema:
    print("Easy")
else:
    raise ValueError("Check DWh for changes")