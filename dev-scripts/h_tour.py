from src.utils.spark import sparkenv
from pyspark.sql import functions as sf



spark = sparkenv()

cols = ["tourney_id", "tourney_name", "surface", "tourney_level"]

df_full_table = spark.read.parquet("s3://tennis-app-ck/clean/atp_matches_2020/")
df_full_table.show()

df_tour = df_full_table.select(sf.md5("tourney_id").alias("tourney_sk"), *cols).distinct()
df_tour.show()

h_tour = df_tour.select("tourney_sk", "tourney_id", sf.current_date().alias("load_date"),sf.lit("SINGLE").alias("SOURCE")).distinct()
h_tour.show(truncate=False)
h_tour.count()


(
    h_tour.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(path="s3://tennis-app-ck/raw_dv/prod/h_tour", name="h_tour")

 )


