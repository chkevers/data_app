from src.utils.spark import sparkenv

spark = sparkenv()

df_test = spark.read.parquet("s3://tennis-app-ck/clean/atp_matches_2021/")
df_test.show()

for i in df_test.schema:
    print(i.name)
