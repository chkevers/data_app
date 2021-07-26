from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window


"""Start Spark Session"""
spark = SparkSession.builder.getOrCreate()

df_base = spark.read.csv("s3://tennis-app-ck/raw/atp_matches_2020.csv", header=True)
df_filter = df_base.filter(f.col("tourney_id")=="2020-8888")
# df_sort = df_filter.sort(f.col("match_num"))
df_sort2 = df_filter.select("*", f.row_number().over(Window.partitionBy("tourney_id").orderBy(f.col("match_num").desc(), f.col("winner_id").desc())).alias("row_num")).show()
# df_unq = df_sort.dropDuplicates(['tourney_id']).show()
df_unq = df_sort2.filter(f.col("row_num") == 1).drop(f.col("row_num")).show()