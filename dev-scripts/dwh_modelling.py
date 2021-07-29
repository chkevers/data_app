from src.utils.spark import sparkenv
from pyspark.sql import functions as sf

"""Define spark session"""
spark = sparkenv()

"""Read s3 parquet file"""

df_full_fact = spark.read.parquet("s3://tennis-app-ck/clean/atp_matches_2020/")
df_full_fact.show()

dimtour_cols = ["tourney_id", "tourney_name", "surface", "tourney_level"]
dimtour = df_full_fact.select(dimtour_cols).distinct().orderBy("tourney_id")
dimtour.show()
dimtour = dimtour.withColumn("tourney_level_desc", sf.when(dimtour.tourney_level == "A", "Other tour-level event")
                                                     .when(dimtour.tourney_level == "M", "Masters 1000")
                                                     .otherwise("To be defined"))

dimtour.show()
