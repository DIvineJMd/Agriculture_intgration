import sqlite3
import pandas as pd
import os

def create_transformed_database():
    """Create and set up the transformed weather database"""
    # Create transformed_database directory if it doesn't exist
    os.makedirs('Transformed_database', exist_ok=True)
    
    conn = sqlite3.connect('WareHouse/weather_data.db')
    cursor = conn.cursor()
    
    # Create transformed weather table with combined metrics
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transformed_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state TEXT,
        district TEXT,
        date DATE,
        is_forecast BOOLEAN,
        temperature_max REAL,
        temperature_min REAL,
        humidity_avg REAL,
        precipitation_sum REAL,
        soil_moisture_surface REAL,
        soil_moisture_deep REAL,
        soil_temperature_surface REAL,
        soil_temperature_deep REAL,
        wind_speed_max REAL,
        evapotranspiration REAL,
        growing_condition_score REAL
    )
    ''')
    
    conn.commit()
    return conn

def transform_weather_data(source_conn, target_conn):
    """Transform and combine weather metrics into agricultural insights"""
    
    # Join daily weather with location and hourly soil data
    query = '''
    WITH daily_metrics AS (
        SELECT 
            l.state,
            l.district,
            dw.date,
            dw.is_forecast,
            dw.temperature_2m_max as temp_max,
            dw.temperature_2m_min as temp_min,
            dw.precipitation_sum,
            dw.rain_sum,
            dw.precipitation_hours,
            dw.wind_speed_10m_max,
            dw.et0_fao_evapotranspiration,
            AVG(hw.soil_moisture_0_to_1cm) as surface_moisture,
            AVG(hw.soil_moisture_27_to_81cm) as deep_moisture,
            AVG(hw.soil_temperature_0cm) as surface_temp,
            AVG(hw.soil_temperature_54cm) as deep_temp,
            AVG(hw.relative_humidity_2m) as avg_humidity
        FROM daily_weather dw
        JOIN location l ON dw.location_id = l.id
        LEFT JOIN hourly_weather hw ON dw.location_id = hw.location_id 
            AND date(hw.timestamp) = dw.date
        GROUP BY l.state, l.district, dw.date, dw.is_forecast
    )
    SELECT 
        state,
        district,
        date,
        is_forecast,
        temp_max as temperature_max,
        temp_min as temperature_min,
        avg_humidity as humidity_avg,
        precipitation_sum,
        rain_sum,
        precipitation_hours,
        surface_moisture as soil_moisture_surface,
        deep_moisture as soil_moisture_deep,
        surface_temp as soil_temperature_surface,
        deep_temp as soil_temperature_deep,
        wind_speed_10m_max as wind_speed_max,
        et0_fao_evapotranspiration as evapotranspiration,
        -- Calculate growing condition score based on available metrics
        ROUND(
            (CASE 
                WHEN temp_max BETWEEN 20 AND 30 THEN 1.0 
                WHEN temp_max BETWEEN 15 AND 35 THEN 0.7
                ELSE 0.3 
            END +
            CASE 
                WHEN precipitation_sum > 0 OR rain_sum > 0 THEN 1.0
                ELSE 0.5
            END +
            CASE 
                WHEN surface_moisture > 0.3 THEN 1.0
                WHEN surface_moisture > 0.1 THEN 0.6
                ELSE 0.3
            END +
            CASE 
                WHEN avg_humidity BETWEEN 60 AND 80 THEN 1.0
                WHEN avg_humidity BETWEEN 40 AND 90 THEN 0.7
                ELSE 0.4
            END) / 4.0 * 100, 
        2) as growing_condition_score
    FROM daily_metrics
    ORDER BY state, district, date
    '''
    
    # Read from source database and write to transformed database
    df = pd.read_sql_query(query, source_conn)
    df.to_sql('transformed_weather', target_conn, if_exists='replace', index=False)

def create_weather_views(conn):
    cursor = conn.cursor()
    
    # Create daily weather summary view combining forecast and historical data
    cursor.execute('''
    CREATE VIEW IF NOT EXISTS vw_daily_weather_summary AS
    SELECT 
        l.state,
        l.district,
        dw.date,
        dw.is_forecast,
        dw.temperature_2m_max,
        dw.temperature_2m_min,
        dw.precipitation_sum,
        dw.rain_sum,
        dw.precipitation_hours,
        dw.wind_speed_10m_max,
        dw.et0_fao_evapotranspiration
    FROM daily_weather dw
    JOIN location l ON dw.location_id = l.id
    ''')
    
    # Create hourly soil conditions view
    cursor.execute('''
    CREATE VIEW IF NOT EXISTS vw_soil_conditions AS
    SELECT 
        l.state,
        l.district,
        hw.timestamp,
        hw.is_forecast,
        hw.soil_temperature_0cm,
        hw.soil_temperature_6cm,
        hw.soil_temperature_18cm,
        hw.soil_temperature_54cm,
        hw.soil_moisture_0_to_1cm,
        hw.soil_moisture_1_to_3cm,
        hw.soil_moisture_3_to_9cm,
        hw.soil_moisture_9_to_27cm,
        hw.soil_moisture_27_to_81cm
    FROM hourly_weather hw
    JOIN location l ON hw.location_id = l.id
    ''')
    
    # Create daily aggregated soil conditions
    cursor.execute('''
    CREATE VIEW IF NOT EXISTS vw_daily_soil_conditions AS
    SELECT 
        state,
        district,
        date(timestamp) as date,
        is_forecast,
        round(avg(soil_temperature_0cm), 2) as avg_surface_temp,
        round(avg(soil_temperature_54cm), 2) as avg_deep_temp,
        round(avg(soil_moisture_0_to_1cm), 2) as avg_surface_moisture,
        round(avg(soil_moisture_27_to_81cm), 2) as avg_deep_moisture
    FROM vw_soil_conditions
    GROUP BY state, district, date(timestamp), is_forecast
    ''')
    
    conn.commit()

def get_weather_trends(conn, days=7):
    """Get weather trends for agricultural analysis"""
    query = f'''
    SELECT 
        state,
        district,
        date,
        round(avg(temperature_2m_max), 2) as avg_max_temp,
        round(avg(temperature_2m_min), 2) as avg_min_temp,
        round(sum(precipitation_sum), 2) as total_precipitation,
        round(avg(et0_fao_evapotranspiration), 2) as avg_evapotranspiration
    FROM vw_daily_weather_summary
    WHERE date >= date('now', '-{days} days')
    GROUP BY state, district, date
    ORDER BY state, district, date
    '''
    return pd.read_sql_query(query, conn)

def get_soil_analysis(conn, days=7):
    """Get soil condition analysis"""
    query = f'''
    SELECT 
        state,
        district,
        date,
        avg_surface_temp,
        avg_deep_temp,
        avg_surface_moisture,
        avg_deep_moisture
    FROM vw_daily_soil_conditions
    WHERE date >= date('now', '-{days} days')
    ORDER BY state, district, date
    '''
    return pd.read_sql_query(query, conn)

def main():
    # Connect to source database
    source_conn = sqlite3.connect('database/weather_data.db')
    
    # Create and connect to transformed database
    target_conn = create_transformed_database()
    
    # Transform the data
    transform_weather_data(source_conn, target_conn)
    
    # Create views for analysis
    create_weather_views(target_conn)
    
    # Close connections
    source_conn.close()
    target_conn.close()
    
    print("Weather data transformation completed successfully!")

if __name__ == "__main__":
    main()