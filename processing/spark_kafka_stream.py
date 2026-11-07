import logging
import time
import os
import re
import sys
import snowflake.connector

from pathlib import Path
from dotenv import load_dotenv

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)


# ==========================================
# 1. Logging Configuration
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# ==========================================
# 2. Project Configuration
# ==========================================

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
from config.snowflake_config import SNOWFLAKE_CONFIG
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "weather-data"

CHECKPOINT_DIR = str(
    BASE_DIR / "checkpoints" / "weather-stream"
)


# ==========================================
# 3. Load Snowflake Configuration
# ==========================================

load_dotenv(BASE_DIR / "config" / "snowflake.env")

key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

if not key_path or not passphrase:
    raise ValueError("Snowflake private key configuration is missing.")

with open(key_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=passphrase.encode(),
        backend=default_backend()
    )

private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

pem_private_key = re.sub(
    r"-*(BEGIN|END) PRIVATE KEY-*\n",
    "",
    private_key_bytes.decode("utf-8")
).replace("\n", "")


sf_options = {
    "sfURL": os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com",
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "sfRole": os.getenv("SNOWFLAKE_ROLE"),
    "pem_private_key": pem_private_key
}


# ==========================================
# 4. Weather Data Schema
# ==========================================

weather_schema = StructType([
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("apparent_temperature", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("weather_code", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("extracted_at", StringType(), True)
])


# ==========================================
# 5. Create Spark Session
# ==========================================

spark = (
    SparkSession.builder
    .appName("WeatherKafkaSnowflakeStreaming")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
            "net.snowflake:spark-snowflake_2.12:3.2.2-spark_3.5",
            "net.snowflake:snowflake-jdbc:4.0.2"
        ])
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# 6. Read Data From Kafka
# ==========================================

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)


# ==========================================
# 7. Parse JSON Data
# ==========================================

weather_stream = (
    kafka_stream
    .selectExpr("CAST(value AS STRING) AS json_data")
    .select(
        from_json(col("json_data"), weather_schema).alias("weather")
    )
    .select("weather.*")
)


# ==========================================
# 8. Transform Timestamp Columns
# ==========================================

weather_transformed = (
    weather_stream
    .withColumn(
        "timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm")
    )
    .withColumn(
        "extracted_at",
        to_timestamp(
            col("extracted_at"),
            "yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS]XXX"
        )
    )
    .select(
        "city",
        "latitude",
        "longitude",
        "temperature",
        "humidity",
        "apparent_temperature",
        "precipitation",
        "weather_code",
        "wind_speed",
        "pressure",
        "timestamp",
        "extracted_at"
    )
)


# ==========================================
# 9. Write Each Micro-Batch to Snowflake
# ==========================================

def write_to_snowflake(batch_df, batch_id):

    if batch_df.isEmpty():
        logger.info("Batch %s is empty. Skipping.", batch_id)
        return

    logger.info("Processing batch %s.", batch_id)
    batch_start_time = time.perf_counter()
    connection = None
    cursor = None

    try:
        # Step 1: Remove duplicates inside the current batch
        dedup_start = time.perf_counter()

        batch_df = batch_df.dropDuplicates([
            "city",
            "timestamp",
            "extracted_at"
        ])
        dedup_time = time.perf_counter() - dedup_start

        logger.info(
           "Batch %s deduplication completed in %.2f seconds.",
           batch_id,
           dedup_time
        )

        # Step 2: Write batch to staging table
        staging_start = time.perf_counter()
        logger.info("Writing batch %s to staging.", batch_id)

        (
            batch_df.write
            .format("net.snowflake.spark.snowflake")
            .options(**sf_options)
            .option("dbtable", "WEATHER_DATA_STAGING")
            .mode("append")
            .save()
        )
        staging_time = time.perf_counter() - staging_start

        logger.info(
            "Batch %s staging write completed in %.2f seconds.",
            batch_id,
            staging_time
        )

        # Step 3: Connect to Snowflake
        connection = snowflake.connector.connect(
            **SNOWFLAKE_CONFIG
        )

        cursor = connection.cursor()

        # Step 4: Merge staging into main table
        merge_sql = """
        MERGE INTO WEATHER_DB.WEATHER_SCHEMA.WEATHER_DATA AS TARGET

        USING (
            SELECT *
            FROM WEATHER_DB.WEATHER_SCHEMA.WEATHER_DATA_STAGING

            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY CITY, TIMESTAMP, EXTRACTED_AT
                ORDER BY EXTRACTED_AT
            ) = 1

        ) AS SOURCE

        ON TARGET.CITY = SOURCE.CITY
        AND TARGET.TIMESTAMP = SOURCE.TIMESTAMP
        AND TARGET.EXTRACTED_AT = SOURCE.EXTRACTED_AT

        WHEN NOT MATCHED THEN INSERT (
            CITY,
            LATITUDE,
            LONGITUDE,
            TEMPERATURE,
            HUMIDITY,
            APPARENT_TEMPERATURE,
            PRECIPITATION,
            WEATHER_CODE,
            WIND_SPEED,
            PRESSURE,
            TIMESTAMP,
            EXTRACTED_AT
        )

        VALUES (
            SOURCE.CITY,
            SOURCE.LATITUDE,
            SOURCE.LONGITUDE,
            SOURCE.TEMPERATURE,
            SOURCE.HUMIDITY,
            SOURCE.APPARENT_TEMPERATURE,
            SOURCE.PRECIPITATION,
            SOURCE.WEATHER_CODE,
            SOURCE.WIND_SPEED,
            SOURCE.PRESSURE,
            SOURCE.TIMESTAMP,
            SOURCE.EXTRACTED_AT
        );
        """
        merge_start = time.perf_counter()
        cursor.execute(merge_sql)
        merge_time = time.perf_counter() - merge_start

        logger.info(
            "Batch %s MERGE completed in %.2f seconds.",
            batch_id,
            merge_time
        )
        logger.info(
            "Batch %s successfully merged into main table.",
            batch_id
        )

        # Step 5: Cleanup staging after successful merge
        cleanup_start = time.perf_counter()
        cursor.execute("""
            DELETE FROM WEATHER_DB.WEATHER_SCHEMA.WEATHER_DATA_STAGING
        """)
        cleanup_time = time.perf_counter() - cleanup_start

        logger.info(
            "Batch %s staging cleanup completed in %.2f seconds.",
            batch_id,
            cleanup_time
        )

    except Exception:
        logger.exception(
            "Failed to process batch %s.",
            batch_id
        )
        raise

    finally:
        if cursor:
            cursor.close()

        if connection:
            connection.close()

# ==========================================
# 10. Start Streaming Query
# ==========================================

query = (
    weather_transformed.writeStream
    .foreachBatch(write_to_snowflake)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="60 seconds")
    .start()
)


logger.info(
    "Weather Kafka to Snowflake Streaming started successfully."
)

query.awaitTermination()
