from src.utils.spark import sparkenv

spark = sparkenv()

df_test = spark.read.parquet("s3://tennis-app-ck/clean/atp_matches_2021/")
df_test.show()

(df_test.write
        .option("path", "s3://tennis-app-ck/dwh/dev/full_facts")
        .mode("overwrite")
        .format("parquet")
        .saveAsTable("dwh_dev.full_facts")
 )


# spark.sql("show databases").show()
spark.catalog.listDatabases()


### Testing weird solution

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
         .enableHiveSupport()
         .getOrCreate()
         )