from pyspark.sql import SparkSession
from src.utils.operations_s3 import list_objects
from pathlib import Path

# logger = logging.getLogger('py4j')
"""Start Spark Session"""
spark = (SparkSession
         .builder
         .config("spark.driver.core", "2")
         .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.driver.userClassPathFirst", "true")
         .config("spark.driver.memory", "4g")
         .config("spark.executor.userClassPathFirst", "true")
         .config("spark.master", "local[2]")
         .config("spark.eventLog.enabled", "true")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .getOrCreate()
         )
# spark.stop()

"""Get spark session configuration"""

for i in spark.sparkContext.getConf().getAll():
    print(i)


"""Ingest csv files from raw layer and write them to clean layer"""
def ingest_csv():
    for csv in list_objects('tennis-app-ck', 'raw/'):
        path = csv.replace("raw", "clean").split('.')[0]
        name = path.split('/')[-1]
        df = spark.read.csv(csv, header=True)

        (
            df.write
                .mode("overwrite")
                .format("parquet")
                .saveAsTable(name=name, path=path)
        )

ingest_csv()


df_rep = spark.read.csv("s3://tennis-app-ck/raw/atp_rankings_90s.csv", header=True)
df_rep.show()
        (
            df_rep.coalesce(1)
                .write
                .mode("overwrite")
                .format("parquet")
                .saveAsTable(name="atp_rankings_90s", path="s3://tennis-app-ck/clean/atp_rankings_90s")
        )
