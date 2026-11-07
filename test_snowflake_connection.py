import snowflake.connector
from config.snowflake_config import SNOWFLAKE_CONFIG

connection = None
cursor = None

try:
    print("Connecting to Snowflake...")

    connection = snowflake.connector.connect(**SNOWFLAKE_CONFIG)

    print("Connection successful!")

    cursor = connection.cursor()

    cursor.execute("""
        SELECT
            CURRENT_USER(),
            CURRENT_ROLE(),
            CURRENT_WAREHOUSE(),
            CURRENT_DATABASE(),
            CURRENT_SCHEMA()
    """)

    result = cursor.fetchone()

    print("\nSnowflake Connection Details:")
    print("User:", result[0])
    print("Role:", result[1])
    print("Warehouse:", result[2])
    print("Database:", result[3])
    print("Schema:", result[4])

finally:
    if cursor:
        cursor.close()

    if connection:
        connection.close()

    print("\nConnection closed.")
