import os
import re

from pathlib import Path
from dotenv import load_dotenv

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from pyspark.sql import SparkSession


# Load environment variables
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / "config" / "snowflake.env")


# Read encrypted private key
key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

with open(key_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=passphrase.encode(),
        backend=default_backend()
    )


# Convert key to unencrypted PEM in memory
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


# Snowflake connection options
sf_options = {
    "sfURL": os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com",
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "sfRole": os.getenv("SNOWFLAKE_ROLE"),
    "pem_private_key": pem_private_key
}


spark = (
    SparkSession.builder
    .appName("SparkSnowflakeConnectionTest")
    .master("local[*]")
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

try:
    print("Testing Spark to Snowflake connection...")

    result = (
        spark.read
        .format("net.snowflake.spark.snowflake")
        .options(**sf_options)
        .option(
            "query",
            """
            SELECT
                CURRENT_USER() AS USER_NAME,
                CURRENT_ROLE() AS ACTIVE_ROLE,
                CURRENT_WAREHOUSE() AS WAREHOUSE_NAME,
                CURRENT_DATABASE() AS DATABASE_NAME
            """
        )
        .load()
    )

    result.show(truncate=False)

    print("Spark connected to Snowflake successfully!")

finally:
    spark.stop()
