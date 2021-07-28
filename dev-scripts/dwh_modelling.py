from src.utils.spark import sparkenv

"""Define spark session"""
spark = sparkenv()

"""Read s3 parquet file"""

df_full_fact = spark.read.parquet("s3://tennis-app-ck/clean/atp_matches_2020/")
df_full_fact.show()


dimtour_cols = ["tourney_id", "tourney_name", "surface", "tourney_level"]
dimtour = df_full_fact.select(dimtour_cols).distinct().orderBy("tourney_id")
dimtour.show()