from src.utils.spark import sparkenv
from pyspark.sql import functions as sf


spark = sparkenv()

cols_win = {"winner_id": "player_id", "winner_name":"player_name", "winner_hand":"player_hand", "winner_ht":"player_ht"}
cols_lose = {"loser_id": "player_id", "loser_name":"player_name", "loser_hand":"player_hand", "loser_ht":"player_ht"}

win_list = [f'{old} as {new}' for old, new in cols_win.items()]
lose_list = [f'{old} as {new}' for old, new in cols_lose.items()]



df_full_table = spark.read.parquet("s3://tennis-app-ck/clean/atp_matches_2020/")
df_full_table.show()

df_win = df_full_table.selectExpr(win_list)
df_lose = df_full_table.selectExpr(lose_list)
df_player = df_win.union(df_lose)
df_player = df_player.select(sf.md5("player_id").alias("player_sk"),'*').distinct()
df_player.show()

h_player = df_player.select("player_sk","player_id",sf.current_date().alias("load_date"),sf.lit("SINGLE").alias("SOURCE"))
h_player.show()
h_player.printSchema()


(
    h_player.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(path="s3://tennis-app-ck/raw_dv/prod/h_player", name="h_player")

 )