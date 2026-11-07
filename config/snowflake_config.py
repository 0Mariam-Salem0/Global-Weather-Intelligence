import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = BASE_DIR / "config" / "snowflake.env"

load_dotenv(ENV_FILE)

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "private_key_file": os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"),
    "private_key_password": os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
    "authenticator": "SNOWFLAKE_JWT"
}

missing = [
    key for key, value in SNOWFLAKE_CONFIG.items()
    if not value
]

if missing:
    raise ValueError(
        f"Missing Snowflake configuration: {missing}"
    )

print("Snowflake configuration loaded successfully.")
