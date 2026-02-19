import logging
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

from config.config import (
    BASE_URL,
    YEAR,
    STATION_LIMIT,
    OUTPUT_FILE,
    REQUEST_TIMEOUT,
    MAX_RETRIES
)

logger = logging.getLogger(__name__)

class DataExtractor:

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"X-API-Key": api_key}

    def _make_request(self, url, params=None):
        """Make HTTP request with retry logic and rate limit handling"""
        retries = 0

        while retries < MAX_RETRIES:
            try:
                response = requests.get(
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=REQUEST_TIMEOUT
                )

                if response.status_code == 200:
                    return response.json()
                if response.status_code == 404:
                    logger.warning(f"No data found at {url} with params {params}")
                    return {"results": []}
                if response.status_code == 429:
                    wait_time = 2 ** retries
                    logger.warning(f"Rate limited. Waiting {wait_time}s.")
                    time.sleep(wait_time)
                    retries += 1
                    continue
                else:
                    logger.error(
                        f"HTTP {response.status_code} - {response.text}"
                    )
                    return None
            except requests.RequestException as e:
                logger.error(f"Request failed: {e}")
                time.sleep(2 ** retries)
            
            retries += 1
        
        logger.error(f"Max retries ({MAX_RETRIES}) exceeded for {url}")
        return None

    def _generate_month_ranges(self, year):
        ranges = []
        start = datetime(year, 1, 1)

        while start.year == year:
            if start.month == 12:
                end = datetime(year, 12, 31, 23, 59, 59)
            else:
                end = datetime(year, start.month + 1, 1) - timedelta(seconds=1)
            
            ranges.append((
                start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                end.strftime("%Y-%m-%dT%H:%M:%SZ")
            ))

            start = end + timedelta(seconds=1)
        
        return ranges

    def fetch_locations(self):
        """Fetch monitoring locations/stations"""
        logger.info("Fetching locations....")
        url = f"{BASE_URL}/locations"
        params = {"limit": STATION_LIMIT}

        data = self._make_request(url, params)
        if not data:
            raise RuntimeError("Failed to fetch locations")
        
        return data.get("results", [])
    
    def fetch_measurements(self, sensor_id):
        """Fetch measurements for a specific sensor with pagination"""
        url = f"{BASE_URL}/sensors/{sensor_id}/measurements/hourly"
        page = 1
        all_records = []
        
        for date_from, date_to in self._generate_month_ranges(YEAR):
            logger.info(
            f"      Fetching {date_from} - {date_to}"
            )
            page = 1
            while True:
                params = {
                    "date_from": date_from,
                    "date_to": date_to,
                    "limit": 1000,
                    "page": page,
                }

                data = self._make_request(url, params)

                if not data:
                    break

                results = data.get("results", [])

                if not results:
                    break
                    
                all_records.extend(results)

                if len(results) < 1000:
                    break

                page += 1
                time.sleep(0.2)

        return all_records

    def _normalize(self, records, location, sensor):
        """Normalize measurement records into a flat DataFrame structure"""
        rows = []

        for record in records:
            rows.append({
                "location_id": location["id"],
                "location_name": location["name"],
                "locality": location.get("locality"),
                "country": location.get("country", {}).get("name"),
                "country_code": location.get("country", {}).get("code"),
                "latitude": location.get("coordinates", {}).get("latitude"),
                "longitude": location.get("coordinates", {}).get("longitude"),
                "sensor_id": sensor["id"],
                "sensor_name": sensor["name"],
                "parameter": sensor.get("parameter", {}).get("name"),
                "parameter_display": sensor.get("parameter", {}).get("displayName"),
                "unit": sensor.get("parameter", {}).get("units"),
                "value": record.get("value"),
                "datetime_utc": record.get("datetimeLast", {}).get("utc"),
                "datetime_local": record.get("datetimeLast", {}).get("local")
            })

        return pd.DataFrame(rows)

    def run(self, locations):
        """Main pipeline execution"""
        logger.info("=" * 80)
        logger.info("Starting OpenAQ Data Extraction Pipeline")
        logger.info("=" * 80)

        # Fetch all locations
        locations = self.fetch_locations()
        logger.info(f"Found {len(locations)} locations to process")
        
        # Initialize counters
        first_write = not OUTPUT_FILE.exists()
        locations_processed = 0
        sensors_processed = 0
        sensors_with_no_data = 0
        total_measurements = 0

        # Process each location
        for idx, location in enumerate(locations, 1):
            locations_processed += 1
            location_id = location["id"]
            location_name = location.get("name", "Unknown")
            
            logger.info(f"[{idx}/{len(locations)}] Processing location: {location_name} (ID: {location_id})")

            # Fetch sensors for this location
            sensors = location.get("sensors", [])
            
            if not sensors:
                logger.warning(f"  No sensors found for location {location_id}")
                continue

            logger.info(f"  Found {len(sensors)} sensors")
            
            # Process each sensor
            for sensor in sensors:
                sensor_id = sensor["id"]
                parameter = sensor.get("parameter", {}).get("name", "unknown")
                
                logger.info(f"    Processing sensor {sensor_id} ({parameter})")
                
                # Fetch measurements
                measurements = self.fetch_measurements(sensor_id)
                
                if not measurements:
                    sensors_with_no_data += 1
                    logger.info(f"      No measurements found")
                    continue

                # Normalize and save
                df = self._normalize(measurements, location, sensor)
                
                df.to_csv(
                    OUTPUT_FILE,
                    mode='a',
                    header=first_write,
                    index=False
                )

                first_write = False
                sensors_processed += 1
                total_measurements += len(measurements)
                
                logger.info(f"      Saved {len(measurements)} measurements")

        # Final summary
        logger.info("=" * 80)
        logger.info("Extraction Complete!")
        logger.info(f"  Locations processed: {locations_processed}")
        logger.info(f"  Sensors processed: {sensors_processed}")
        logger.info(f"  Sensors with no data: {sensors_with_no_data}")
        logger.info(f"  Total measurements: {total_measurements}")
        logger.info(f"  Output file: {OUTPUT_FILE}")
        logger.info("=" * 80)