from src.utils.spark import sparkenv
from pyspark.sql import functions as sf
from pyspark.sql import Window
import time

spark = sparkenv()

## Create Test dataframe on dim_date



test_ids = spark.read.parquet("s3://tennis-app-ck/dwh/prod/dimdate")
test_ids.show()


## Test for monotonically increasing id with row_number()

startime  = time.time()

test_sk = test_ids.withColumn("date_sk_tmp", sf.monotonically_increasing_id())
test_sk.show()

window = Window.orderBy(test_sk.date_sk_tmp)

final_sk = test_sk.withColumn("date_sk", sf.row_number().over(window))

final_sk.columns

fin_sk = final_sk.select('date_sk', 'date', 'date_string', 'day', 'day_name', 'day_short_name', 'month', 'month_name', 'month_short_name', 'year', 'week')

fin_sk.show()

endtime  = time.time()

runtime = endtime - startime

print(f"Runtime for mono is {runtime}")

## Test for MD5 --> Solution choosen

startime_md  = time.time()

md_sk = test_ids.withColumn("md_sk", sf.md5(test_ids.date_string))

md_sk.show()
md_sk.printSchema()

endtime_md = time.time()

runtime_md = endtime_md - startime_md

print(f"Runtime for mono is {runtime_md}")