from pyspark.sql import SparkSession


def sparkenv():

    spark = (SparkSession
             .builder
             .config("spark.driver.core", "2")
             .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.driver.userClassPathFirst", "true")
             .config("spark.driver.memory", "6g")
             .config("spark.executor.userClassPathFirst", "true")
             .config("spark.master", "local[2]")
             .config("spark.eventLog.enabled", "true")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             #.enableHiveSupport()
             .getOrCreate()
             )

    return spark

