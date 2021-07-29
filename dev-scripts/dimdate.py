from datetime import datetime, timedelta, time, timezone
from pyspark.sql import DataFrame, SparkSession
from src.utils.spark import sparkenv
from pyspark.sql.functions import (date_format,dayofmonth,from_unixtime,month, to_date, year, weekofyear)


spark = sparkenv()


def create_date_dimension(spark: SparkSession, start: datetime, end: datetime) -> DataFrame:
    if spark is None or type(spark) is not SparkSession:
        raise ValueError("A valid SparkSession instance must be provided")

    if type(start) is not datetime:
        raise ValueError("Start date must be a datetime.datetime object")

    if type(end) is not datetime:
        raise ValueError("End date must be a datetime.datetime object")

    if start >= end:
        raise ValueError("Start date must be before the end date")

    if start.tzinfo is None:
        start = datetime.combine(start.date(), time(0, 0, 0), tzinfo=timezone.utc)

    if end.tzinfo is None:
        end = datetime.combine(end.date(), time(0, 0, 0), tzinfo=timezone.utc)

    end = end + timedelta(days=1)

    return (
        spark.range(start=start.timestamp(), end=end.timestamp(), step=24 * 60 * 60)
             .withColumn("date", to_date(from_unixtime("id")))
             .withColumn("date_string", date_format("date", "yyyyMMdd"))
             .withColumn("day", dayofmonth("date"))
             .withColumn("day_name", date_format("date", "EEEE"))
             .withColumn("day_short_name", date_format("date", "EEE"))
             .withColumn("month", month("date"))
             .withColumn("month_name", date_format("date", "MMMM"))
             .withColumn("month_short_name", date_format("date", "MMM"))
             .withColumn("year", year("date"))
             .withColumn("week", weekofyear("date"))
             .drop("id")
    )

dimdate = create_date_dimension(spark, datetime(1950, 1, 1), datetime(2050,1,1))
dimdate.show()
dimdate.printSchema()

(
    dimdate.write
        .mode("overwrite")
        .format("parquet")
        .saveAsTable(path="s3://tennis-app-ck/dwh/prod/dimdate", name="dimdate")

 )