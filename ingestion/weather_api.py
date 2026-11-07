import requests
import logging
from datetime import datetime, timezone

# ==========================================
# 1. Configuration
# ==========================================

URL = "https://api.open-meteo.com/v1/forecast"

cities = {
    "Cairo": {"latitude": 30.0444, "longitude": 31.2357},
    "London": {"latitude": 51.5072, "longitude": -0.1276},
    "Tokyo": {"latitude": 35.6762, "longitude": 139.6503},
    "New York": {"latitude": 40.7128, "longitude": -74.0060},
}


# ==========================================
# 2. Logging
# ==========================================

logger = logging.getLogger(__name__)


# ==========================================
# 3. Extract Weather Data
# ==========================================

def get_weather(city, coordinates):

    params = {
        "latitude": coordinates["latitude"],
        "longitude": coordinates["longitude"],
        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "precipitation",
            "weather_code",
            "wind_speed_10m",
            "surface_pressure"
        ],
        "timezone": "UTC"
    }

    response = requests.get(URL, params=params, timeout=30)
    response.raise_for_status()

    result = response.json()

    weather = {
        "city": city,
        "latitude": coordinates["latitude"],
        "longitude": coordinates["longitude"],
        "temperature": result["current"]["temperature_2m"],
        "humidity": result["current"]["relative_humidity_2m"],
        "apparent_temperature": result["current"]["apparent_temperature"],
        "precipitation": result["current"]["precipitation"],
        "weather_code": result["current"]["weather_code"],
        "wind_speed": result["current"]["wind_speed_10m"],
        "pressure": result["current"]["surface_pressure"],
        "timestamp": result["current"]["time"],
        "extracted_at": datetime.now(timezone.utc).isoformat()
    }

    return weather


# ==========================================
# 4. Extract All Cities
# ==========================================

def extract_all_weather():

    weather_records = []

    for city, coordinates in cities.items():

        try:
            data = get_weather(city, coordinates)
            weather_records.append(data)

            logger.info(
                "Successfully extracted weather for %s",
                city
            )

        except requests.RequestException:
            logger.exception(
                "Failed to extract weather for %s",
                city
            )

    return weather_records
