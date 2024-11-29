import requests
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from geopy.geocoders import Nominatim
from datetime import datetime, timedelta
import sqlite3
from datetime import datetime
import os

def get_location_by_ip():
    response = requests.get("https://ipinfo.io")
    data = response.json()
    location = data.get("loc", "Unknown")
    latitude, longitude = location.split(",") if location != "Unknown" else (None, None)
    return latitude, longitude

def get_location_details(lat, lon):
    geolocator = Nominatim(user_agent="IIA")
    location = geolocator.reverse(f"{lat},{lon}")
    state = location.raw['address'].get('state')
    district = location.raw['address'].get('state_district')
    return location.address, state, district

def setup_openmeteo_client():
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)

def get_forecast_data(lat, lon, openmeteo):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
	"latitude": {lat},
	"longitude": {lon},
	"current": ["temperature_2m", "relative_humidity_2m", "apparent_temperature", "precipitation", "rain", "showers", "snowfall", "weather_code", "cloud_cover", "pressure_msl", "surface_pressure", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m"],
	"hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation_probability", "precipitation", "rain", "showers", "snowfall", "snow_depth", "weather_code", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "visibility", "evapotranspiration", "vapour_pressure_deficit", "wind_speed_10m", "wind_speed_80m", "wind_speed_120m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m", "wind_gusts_10m", "temperature_80m", "temperature_120m", "temperature_180m", "soil_temperature_0cm", "soil_temperature_6cm", "soil_temperature_18cm", "soil_temperature_54cm", "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm", "soil_moisture_9_to_27cm", "soil_moisture_27_to_81cm"],
	"daily": ["temperature_2m_max", "temperature_2m_min", "apparent_temperature_max", "apparent_temperature_min", "sunrise", "sunset", "uv_index_max", "uv_index_clear_sky_max", "precipitation_sum", "rain_sum", "showers_sum", "snowfall_sum", "precipitation_hours", "precipitation_probability_max", "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum", "et0_fao_evapotranspiration"],
	"timezone": "auto"
}
    return openmeteo.weather_api(url, params=params)[0]

def get_historical_data(lat, lon, openmeteo, days_back):
    end_date = (datetime.now()-timedelta(days=1)).date()
    start_date = end_date - timedelta(days=days_back)
    
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
	"latitude": {lat},
	"longitude": {lon},
	"start_date": start_date,
	"end_date": end_date,
	"hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation_probability", "precipitation", "rain", "showers", "pressure_msl", "surface_pressure", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "visibility", "evapotranspiration", "wind_speed_10m", "wind_speed_80m", "wind_speed_120m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m", "wind_gusts_10m", "temperature_80m", "temperature_120m", "temperature_180m", "soil_temperature_0cm", "soil_temperature_6cm", "soil_temperature_18cm", "soil_temperature_54cm", "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm", "soil_moisture_9_to_27cm", "soil_moisture_27_to_81cm"],
	"daily": ["temperature_2m_max", "temperature_2m_min", "apparent_temperature_max", "apparent_temperature_min"],
	"timezone": "GMT"
}
    return openmeteo.weather_api(url, params=params)[0]

def process_forecast_data(response):
    # print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    # print(f"Elevation {response.Elevation()} m asl")
    # print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    # print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process current data
    current = response.Current()
    current_vars = [
        "temperature_2m", "relative_humidity_2m", "apparent_temperature",
        "precipitation", "rain", "showers", "snowfall", "weather_code",
        "cloud_cover", "pressure_msl", "surface_pressure", "wind_speed_10m",
        "wind_direction_10m", "wind_gusts_10m"
    ]
    
    # print("\nCurrent Weather:")
    # print(f"Current time {current.Time()}")
    # for i, var in enumerate(current_vars):
        # print(f"Current {var}: {current.Variables(i).Value()}")

    # Process hourly data
    hourly = response.Hourly()
    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )}

    # Add all hourly variables to the dataframe
    hourly_vars = [
        "temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature",
        "precipitation_probability", "precipitation", "rain", "showers", "snowfall",
        "snow_depth", "weather_code", "cloud_cover", "cloud_cover_low", "cloud_cover_mid",
        "cloud_cover_high", "visibility", "evapotranspiration", "vapour_pressure_deficit",
        "wind_speed_10m", "wind_speed_80m", "wind_speed_120m", "wind_speed_180m",
        "wind_direction_10m", "wind_direction_80m", "wind_direction_120m", "wind_direction_180m",
        "wind_gusts_10m", "temperature_80m", "temperature_120m", "temperature_180m",
        "soil_temperature_0cm", "soil_temperature_6cm", "soil_temperature_18cm",
        "soil_temperature_54cm", "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm",
        "soil_moisture_3_to_9cm", "soil_moisture_9_to_27cm", "soil_moisture_27_to_81cm"
    ]
    
    for i, var in enumerate(hourly_vars):
        hourly_data[var] = hourly.Variables(i).ValuesAsNumpy()

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    # print("\nHourly Forecast Data:")
    # print(hourly_dataframe)

    # Process daily data
    daily = response.Daily()
    daily_data = {"date": pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    )}

    daily_vars = [
    "temperature_2m_max", "temperature_2m_min", "apparent_temperature_max",
    "apparent_temperature_min", "sunrise", "sunset", "uv_index_max",
    "uv_index_clear_sky_max", "precipitation_sum", "rain_sum", "showers_sum",
    "snowfall_sum", "precipitation_hours", "precipitation_probability_max",
    "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant",
    "shortwave_radiation_sum", "et0_fao_evapotranspiration"
    ]

    for i, var in enumerate(daily_vars):
        daily_data[var] = daily.Variables(i).ValuesAsNumpy()

    daily_dataframe = pd.DataFrame(data=daily_data)
    # print("\nDaily Forecast Data:")
    # print(daily_dataframe)
    
    return hourly_dataframe, daily_dataframe

def process_historical_data(response):
    # print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    # print(f"Elevation {response.Elevation()} m asl")
    # print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    # print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process hourly historical data
    hourly = response.Hourly()
    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )}

    historical_hourly_vars = [
        "temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature",
        "precipitation_probability", "precipitation", "rain", "showers", "pressure_msl",
        "surface_pressure", "cloud_cover", "cloud_cover_low", "cloud_cover_mid",
        "cloud_cover_high", "visibility", "evapotranspiration", "wind_speed_10m",
        "wind_speed_80m", "wind_speed_120m", "wind_speed_180m", "wind_direction_10m",
        "wind_direction_80m", "wind_direction_120m", "wind_direction_180m",
        "wind_gusts_10m", "temperature_80m", "temperature_120m", "temperature_180m",
        "soil_temperature_0cm", "soil_temperature_6cm", "soil_temperature_18cm",
        "soil_temperature_54cm", "soil_moisture_0_to_1cm", "soil_moisture_1_to_3cm",
        "soil_moisture_3_to_9cm", "soil_moisture_9_to_27cm", "soil_moisture_27_to_81cm"
    ]

    for i, var in enumerate(historical_hourly_vars):
        hourly_data[var] = hourly.Variables(i).ValuesAsNumpy()

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    # print("\nHistorical Hourly Data:")
    # print(hourly_dataframe)

    # Process daily historical data
    daily = response.Daily()
    daily_data = {"date": pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    )}

    historical_daily_vars = [
        "temperature_2m_max", "temperature_2m_min",
        "apparent_temperature_max", "apparent_temperature_min"
    ]

    for i, var in enumerate(historical_daily_vars):
        daily_data[var] = daily.Variables(i).ValuesAsNumpy()

    daily_dataframe = pd.DataFrame(data=daily_data)
    # print("\nHistorical Daily Data:")
    # print(daily_dataframe)
    
    return hourly_dataframe, daily_dataframe
def create_database():
    """Create SQLite database and tables in a database folder"""
    # Create database directory if it doesn't exist
    os.makedirs('database', exist_ok=True)
    
    # Connect to database in the database folder
    conn = sqlite3.connect('database/weather_data.db')
    cursor = conn.cursor()
    
    # Create location table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS location (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        address TEXT,
        state TEXT,
        district TEXT,
        latitude REAL,
        longitude REAL,
        elevation REAL,
        timezone TEXT,
        timezone_abbreviation TEXT,
        utc_offset_seconds INTEGER
    )
    ''')
    
    # Create current_weather table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS current_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_id INTEGER,
        timestamp DATETIME,
        temperature_2m REAL,
        relative_humidity_2m REAL,
        apparent_temperature REAL,
        precipitation REAL,
        rain REAL,
        showers REAL,
        snowfall REAL,
        weather_code INTEGER,
        cloud_cover REAL,
        pressure_msl REAL,
        surface_pressure REAL,
        wind_speed_10m REAL,
        wind_direction_10m REAL,
        wind_gusts_10m REAL,
        FOREIGN KEY (location_id) REFERENCES location (id)
    )
    ''')
    
    # Create hourly_forecast table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS hourly_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_id INTEGER,
        timestamp DATETIME,
        is_forecast BOOLEAN,
        temperature_2m REAL,
        relative_humidity_2m REAL,
        dew_point_2m REAL,
        apparent_temperature REAL,
        precipitation_probability REAL,
        precipitation REAL,
        rain REAL,
        showers REAL,
        snowfall REAL,
        snow_depth REAL,
        weather_code INTEGER,
        cloud_cover REAL,
        cloud_cover_low REAL,
        cloud_cover_mid REAL,
        cloud_cover_high REAL,
        visibility REAL,
        evapotranspiration REAL,
        vapour_pressure_deficit REAL,
        pressure_msl REAL,
        surface_pressure REAL,
        wind_speed_10m REAL,
        wind_speed_80m REAL,
        wind_speed_120m REAL,
        wind_speed_180m REAL,
        wind_direction_10m REAL,
        wind_direction_80m REAL,
        wind_direction_120m REAL,
        wind_direction_180m REAL,
        wind_gusts_10m REAL,
        temperature_80m REAL,
        temperature_120m REAL,
        temperature_180m REAL,
        soil_temperature_0cm REAL,
        soil_temperature_6cm REAL,
        soil_temperature_18cm REAL,
        soil_temperature_54cm REAL,
        soil_moisture_0_to_1cm REAL,
        soil_moisture_1_to_3cm REAL,
        soil_moisture_3_to_9cm REAL,
        soil_moisture_9_to_27cm REAL,
        soil_moisture_27_to_81cm REAL,
        FOREIGN KEY (location_id) REFERENCES location (id)
    )
    ''')
    
    # Create daily_weather table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_id INTEGER,
        date DATE,
        is_forecast BOOLEAN,
        temperature_2m_max REAL,
        temperature_2m_min REAL,
        apparent_temperature_max REAL,
        apparent_temperature_min REAL,
        sunrise DATETIME,
        sunset DATETIME,
        uv_index_max REAL,
        uv_index_clear_sky_max REAL,
        precipitation_sum REAL,
        rain_sum REAL,
        showers_sum REAL,
        snowfall_sum REAL,
        precipitation_hours REAL,
        precipitation_probability_max REAL,
        wind_speed_10m_max REAL,
        wind_gusts_10m_max REAL,
        wind_direction_10m_dominant REAL,
        shortwave_radiation_sum REAL,
        et0_fao_evapotranspiration REAL,
        FOREIGN KEY (location_id) REFERENCES location (id)
    )
    ''')
    
    conn.commit()
    return conn

def store_location_data(conn, address, state, district, lat, lon, elevation, timezone, timezone_abbr, utc_offset):
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO location (address, state, district, latitude, longitude, elevation, 
                         timezone, timezone_abbreviation, utc_offset_seconds)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (address, state, district, lat, lon, elevation, timezone, timezone_abbr, utc_offset))
    conn.commit()
    return cursor.lastrowid

def store_current_weather(conn, location_id, current_data):
    cursor = conn.cursor()
    cursor.execute('''
    INSERT INTO current_weather (
        location_id, timestamp, temperature_2m, relative_humidity_2m, 
        apparent_temperature, precipitation, rain, showers, snowfall,
        weather_code, cloud_cover, pressure_msl, surface_pressure,
        wind_speed_10m, wind_direction_10m, wind_gusts_10m
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (location_id, datetime.fromtimestamp(current_data.Time()), *[
        current_data.Variables(i).Value() for i in range(14)
    ]))
    conn.commit()

def store_hourly_data(conn, location_id, hourly_df, is_forecast=True):
    # Rename 'date' column to 'timestamp' to match database schema
    hourly_df = hourly_df.rename(columns={'date': 'timestamp'})
    hourly_df['location_id'] = location_id
    hourly_df['is_forecast'] = is_forecast
    hourly_df.to_sql('hourly_weather', conn, if_exists='append', index=False)

def store_daily_data(conn, location_id, daily_df, is_forecast=True):
    # Rename 'date' column to match database schema
    daily_df = daily_df.rename(columns={'date': 'date'})  # In this case the name is the same, but included for consistency
    daily_df['location_id'] = location_id
    daily_df['is_forecast'] = is_forecast
    daily_df.to_sql('daily_weather', conn, if_exists='append', index=False)


    
def main():
    # Get location
    lat, lon = get_location_by_ip()
    address, state, district = get_location_details(lat, lon)
    
    print(f"Location: {address}")
    print(f"State: {state}")
    print(f"District: {district}")
    
    # Setup OpenMeteo client
    openmeteo = setup_openmeteo_client()
    
    # Create database connection
    conn = create_database()
    
    # Get and store forecast data
    print("\n=== Forecast Data ===")
    forecast_response = get_forecast_data(lat, lon, openmeteo)
    
    # Store location data
    location_id = store_location_data(
        conn, address, state, district, 
        forecast_response.Latitude(), forecast_response.Longitude(),
        forecast_response.Elevation(), 
        forecast_response.Timezone(), forecast_response.TimezoneAbbreviation(),
        forecast_response.UtcOffsetSeconds()
    )
    
    # Process and store forecast data
    store_current_weather(conn, location_id, forecast_response.Current())
    forecast_hourly, forecast_daily = process_forecast_data(forecast_response)
    store_hourly_data(conn, location_id, forecast_hourly, is_forecast=True)
    store_daily_data(conn, location_id, forecast_daily, is_forecast=True)

    import sys

    print("\nHow many days of historical data would you like?")
    sys.stdout.flush()  # Ensure the output is printed before input
    days_back = int(input())
    print(f"\n=== Historical Data (Past {days_back} days) ===")


    historical_response = get_historical_data(lat, lon, openmeteo, days_back)
    historical_hourly, historical_daily = process_historical_data(historical_response)
    store_hourly_data(conn, location_id, historical_hourly, is_forecast=False)
    store_daily_data(conn, location_id, historical_daily, is_forecast=False)
    
    # Close database connection
    conn.close()
    
    print("[bold cyan]Weather data has been successfully extracted![\bold cyan]")


if __name__ == "__main__":
    main()