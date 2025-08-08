"""User Silver Layer Transformation."""
from __future__ import annotations
import os, logging
from pyspark.sql import functions as F
from pyspark.sql.types import *
from processing.common.spark_session import get_spark

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BRONZE_PATH = os.getenv("BRONZE_USER_PATH", "./data/bronze/yelp_academic_dataset_user.json")
SILVER_PATH = os.getenv("SILVER_USER_PATH", "./data/silver/user")
BATCH_ID = os.getenv("BATCH_ID", "manual_0001")
SOURCE_NAME = os.getenv("SOURCE_NAME", "yelp_raw")


def load_user_data(path: str, infer_schema: bool = False):
    spark = get_spark()
    if infer_schema:
        return spark.read.json(path)
    schema = StructType([
        StructField("user_id", StringType()),
        StructField("name", StringType()),
        StructField("review_count", IntegerType()),
        StructField("yelping_since", StringType()),
        StructField("useful", IntegerType()),
        StructField("funny", IntegerType()),
        StructField("cool", IntegerType()),
        StructField("elite", StringType()),
        StructField("fans", IntegerType()),
        StructField("average_stars", DoubleType()),
        StructField("compliment_hot", IntegerType()),
        StructField("compliment_more", IntegerType()),
        StructField("compliment_profile", IntegerType()),
        StructField("compliment_cute", IntegerType()),
        StructField("compliment_list", IntegerType()),
        StructField("compliment_note", IntegerType()),
        StructField("compliment_plain", IntegerType()),
        StructField("compliment_cool", IntegerType()),
        StructField("compliment_funny", IntegerType()),
        StructField("compliment_writer", IntegerType()),
        StructField("compliment_photos", IntegerType())
    ])
    return spark.read.json(path, schema=schema)


def basic_clean(df):
    return (df
            .withColumn("name", F.trim(F.lower(F.col("name"))))
            .withColumn("elite_years", F.size(F.split(F.col("elite"), ",")))
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_batch_id", F.lit(BATCH_ID))
            .withColumn("_source", F.lit(SOURCE_NAME)))


def validate(df):
    return df.filter(F.length("user_id") > 0)


def write_delta(df, path: str):
    (df.write.format("delta")
        .option("mergeSchema", "true")
        .mode("overwrite")
        .save(path))


def write_delta_incremental(df, path: str):
    spark = df.sparkSession
    from delta.tables import DeltaTable
    if not os.path.exists(path):
        (df.write.format("delta")
            .option("mergeSchema", "true")
            .mode("overwrite")
            .save(path))
        return
    tbl = DeltaTable.forPath(spark, path)
    (tbl.alias('t').merge(df.alias('s'), 't.user_id = s.user_id')
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


def run_pipeline():
    logging.info("Loading user bronze data")
    df = load_user_data(BRONZE_PATH)
    logging.info(f"User bronze count: {df.count()}")
    df = basic_clean(df)
    df = validate(df)
    logging.info(f"Final user rows: {df.count()}")
    write_delta_incremental(df, SILVER_PATH)
    logging.info("User silver write complete")

if __name__ == "__main__":
    run_pipeline()
