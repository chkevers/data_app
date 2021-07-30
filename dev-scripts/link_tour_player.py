from src.utils.spark import sparkenv
from pyspark.sql import functions as sf
from h_player import h_player
from h_tour import h_tour

spark = sparkenv()

df_full_link = spark.read.parquet("s3://tennis-app-ck/clean/atp_matches_2020/")
df_full_link.show()

l_tour_player_staging = df_full_link.select("tourney_id","winner_id", "loser_id")
l_tour_player_staging.show()

l_tour_player_staging2 = l_tour_player_staging.join(h_tour.select("tourney_sk","tourney_id"), on="tourney_id", how="left").drop("tourney_id")
l_tour_player_staging2.show()

l_tour_player_staging3 = l_tour_player_staging2.join(h_player.withColumnRenamed("player_sk","win_sk").select("win_sk","player_id"), l_tour_player_staging2["winner_id"] == h_player["player_id"], how='left').drop("player_id","winner_id")
l_tour_player_staging3.show()

l_tour_player_staging4 = l_tour_player_staging3.join(h_player.withColumnRenamed("player_sk","los_sk").select("los_sk", "player_id"), l_tour_player_staging3["loser_id"] == h_player["player_id"], how='left').drop("player_id","loser_id")
l_tour_player_staging4.show()

df_combine_stage1 = l_tour_player_staging4.selectExpr("tourney_sk","win_sk as player_sk")
df_combine_stage2 = l_tour_player_staging4.selectExpr("tourney_sk","los_sk as player_sk")
df_final_stage = df_combine_stage1.union(df_combine_stage2).distinct()
df_final_stage.show()

l_tour_player = df_final_stage.select(sf.md5(sf.concat("tourney_sk","player_sk")).alias("TourPlayer_HK"),'*', sf.current_date().alias("load_date"),sf.lit("SINGLE").alias("SOURCE"))
l_tour_player.show(truncate=False)
l_tour_player.printSchema()

(
    l_tour_player.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(path="s3://tennis-app-ck/raw_dv/prod/l_tour_player", name="l_tour_player")

 )