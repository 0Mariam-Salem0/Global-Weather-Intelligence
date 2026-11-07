import json
import logging
import time

from kafka import KafkaProducer
from ingestion.weather_api import extract_all_weather


# ==========================================
# 1. Configuration
# ==========================================

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "weather-data"

INGESTION_INTERVAL = 60


# ==========================================
# 2. Logging
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ==========================================
# 3. Create Kafka Producer
# ==========================================

def create_producer():

    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5
    )


# ==========================================
# 4. Send Weather Data
# ==========================================

def send_weather_data(producer, weather_records):

    sent_count = 0

    for record in weather_records:

        try:
            future = producer.send(
                KAFKA_TOPIC,
                value=record
            )

            metadata = future.get(timeout=15)

            logging.info(
                "Sent %s | Temperature: %s°C | Partition: %s | Offset: %s",
                record["city"],
                record["temperature"],
                metadata.partition,
                metadata.offset
            )

            sent_count += 1

        except Exception:
            logging.exception(
                "Failed to send weather data for %s",
                record.get("city", "Unknown")
            )

    producer.flush()

    logging.info(
        "Successfully sent %s out of %s records.",
        sent_count,
        len(weather_records)
    )


# ==========================================
# 5. Continuous Ingestion
# ==========================================

def main():

    producer = create_producer()

    logging.info("Continuous Weather Ingestion Started.")

    try:

        while True:

            cycle_start = time.time()

            logging.info("Starting a new weather extraction cycle.")

            weather_records = extract_all_weather()

            if weather_records:
                send_weather_data(producer, weather_records)
            else:
                logging.warning("No weather records extracted.")

            elapsed_time = time.time() - cycle_start

            sleep_time = max(
                0,
                INGESTION_INTERVAL - elapsed_time
            )

            logging.info(
                "Next extraction in %.1f seconds.",
                sleep_time
            )

            time.sleep(sleep_time)

    except KeyboardInterrupt:

        logging.info("Stopping Weather Ingestion...")

    finally:

        producer.flush()
        producer.close()

        logging.info("Kafka Producer Closed Successfully.")


if __name__ == "__main__":
    main()
