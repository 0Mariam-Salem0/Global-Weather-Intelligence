from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SnowflakeConnectorTest")
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

print("\n================================")
print("Spark Version:", spark.version)
print("Snowflake Connector Loaded")
print("================================\n")
spark.stop()
