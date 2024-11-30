import sqlite3
import pandas as pd
from datetime import datetime
import numpy as np
from geopy.geocoders import Nominatim
from rich.console import Console

console = Console()

def get_location_details(lat, lon):
    """Get location details from latitude and longitude"""
    geolocator = Nominatim(user_agent="IIA")
    location = geolocator.reverse(f"{lat},{lon}")
    state = location.raw['address'].get('state')
    district = location.raw['address'].get('state_district')
    return location.address, state, district

def get_soil_health_data(state, district=None):
    """Get soil health data for location"""
    conn = sqlite3.connect('WareHouse/soil_health_transformed.db')
    
    query = """
    SELECT 
        nitrogen_level,
        phosphorous_level,
        potassium_level,
        organic_carbon_level,
        ph_level,
        ec_level,
        npk_score,
        micro_score,
        overall_soil_health_score
    FROM soil_health
    WHERE LOWER(state) = LOWER(?)
    """
    params = [state]
    
    if district:
        query += " AND LOWER(district) = LOWER(?)"
        params.append(district)
    
    query += " GROUP BY state"  # Aggregate at state level
    
    try:
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        if df.empty:
            print(f"No soil health data found for {state}")
            return None
            
        # Return averaged values
        return {
            'overall_soil_health_score': df['overall_soil_health_score'].mean(),
            'npk_score': df['npk_score'].mean(),
            'micro_score': df['micro_score'].mean(),
            'ph_level': df['ph_level'].iloc[0],
            'ec_level': df['ec_level'].iloc[0]
        }
        
    except Exception as e:
        console.print(f"[bold red]Error getting soil health data: {str(e)}[/red bold]")
        conn.close()
        return None

def get_weather_conditions(state, district=None):
    """Get weather and soil conditions"""
    conn = sqlite3.connect('WareHouse/weather_data.db')
    
    # Normalize state name to match database
    state = state.title()
    if district:
        district = district.title()
    
    # First, let's check if we have any data at all
    check_query = """
    SELECT COUNT(*) as count, 
           MIN(date) as min_date, 
           MAX(date) as max_date,
           COUNT(DISTINCT state) as state_count,
           GROUP_CONCAT(DISTINCT state) as states
    FROM transformed_weather
    """
    check_df = pd.read_sql_query(check_query, conn)
    
    # Modified main query to handle date comparison better
    query = """
    WITH recent_weather AS (
        SELECT 
            temperature_max,
            temperature_min,
            humidity_avg,
            COALESCE(precipitation_sum, 0) as total_precipitation,
            soil_moisture_surface,
            soil_moisture_deep,
            soil_temperature_surface,
            soil_temperature_deep,
            wind_speed_max,
            evapotranspiration,
            growing_condition_score,
            is_forecast,
            date
        FROM transformed_weather
        WHERE LOWER(state) = LOWER(?)
        {}  -- Placeholder for district filter
        AND date >= date('now', '-30 days')
    )
    SELECT 
        AVG(CASE WHEN is_forecast = 0 THEN temperature_max END) as historical_temp_max,
        AVG(CASE WHEN is_forecast = 0 THEN temperature_min END) as historical_temp_min,
        AVG(CASE WHEN is_forecast = 0 THEN humidity_avg END) as historical_humidity,
        SUM(CASE WHEN is_forecast = 0 THEN total_precipitation END) as historical_precipitation,
        AVG(CASE WHEN is_forecast = 0 THEN soil_moisture_surface END) as historical_moisture_surface,
        AVG(CASE WHEN is_forecast = 0 THEN soil_moisture_deep END) as historical_moisture_deep,
        AVG(CASE WHEN is_forecast = 1 THEN temperature_max END) as forecast_temp_max,
        AVG(CASE WHEN is_forecast = 1 THEN temperature_min END) as forecast_temp_min,
        AVG(CASE WHEN is_forecast = 1 THEN humidity_avg END) as forecast_humidity,
        SUM(CASE WHEN is_forecast = 1 THEN total_precipitation END) as forecast_precipitation,
        AVG(growing_condition_score) as avg_growing_condition_score,
        COUNT(*) as record_count
    FROM recent_weather
    """
    
    district_filter = "AND LOWER(district) = LOWER(?)" if district else ""
    query = query.format(district_filter)
    params = [state, district] if district else [state]
    
    df = pd.read_sql_query(query, conn, params=params)
    
    if df.empty or df.iloc[0]['record_count'] == 0:
        print("\nDebug: No data found for the specified location")
        conn.close()
        # Return default values when no data is found
        return {
            'temperature_max': 30.0,
            'temperature_min': 20.0,
            'humidity_avg': 65.0,
            'precipitation_sum': 0.0,
            'soil_moisture_surface': 0.4,
            'soil_moisture_deep': 0.5,
            'growing_condition_score': 50.0
        }
    
    # Process and combine historical and forecast data
    result = {}
    row = df.iloc[0]
    
    # Temperature (prefer historical, fallback to forecast)
    result['temperature_max'] = row['historical_temp_max'] if pd.notnull(row['historical_temp_max']) else (
        row['forecast_temp_max'] if pd.notnull(row['forecast_temp_max']) else 30.0
    )
    result['temperature_min'] = row['historical_temp_min'] if pd.notnull(row['historical_temp_min']) else (
        row['forecast_temp_min'] if pd.notnull(row['forecast_temp_min']) else 20.0
    )
    
    # Humidity
    result['humidity_avg'] = row['historical_humidity'] if pd.notnull(row['historical_humidity']) else (
        row['forecast_humidity'] if pd.notnull(row['forecast_humidity']) else 65.0
    )
    
    # Precipitation (combine historical and forecast)
    result['precipitation_sum'] = (
        (row['historical_precipitation'] if pd.notnull(row['historical_precipitation']) else 0) +
        (row['forecast_precipitation'] if pd.notnull(row['forecast_precipitation']) else 0)
    )
    
    # Soil moisture (with defaults)
    result['soil_moisture_surface'] = row['historical_moisture_surface'] if pd.notnull(row['historical_moisture_surface']) else 0.4
    result['soil_moisture_deep'] = row['historical_moisture_deep'] if pd.notnull(row['historical_moisture_deep']) else 0.5
    
    # Growing condition score
    result['growing_condition_score'] = row['avg_growing_condition_score'] if pd.notnull(row['avg_growing_condition_score']) else (
        calculate_growing_condition_score(result)
    )
    
    conn.close()
    return result

def calculate_growing_condition_score(weather_data):
    """Calculate growing condition score from weather metrics"""
    score = 50  # Base score
    
    if 'temperature_max' in weather_data:
        temp_max = weather_data['temperature_max']
        if 20 <= temp_max <= 30:
            score += 25
        elif 15 <= temp_max <= 35:
            score += 17.5
        else:
            score += 7.5
    
    if 'soil_moisture_surface' in weather_data:
        moisture = weather_data['soil_moisture_surface']
        if moisture > 0.3:
            score += 25
        elif moisture > 0.1:
            score += 15
        else:
            score += 7.5
    
    return min(max(score, 0), 100)

def get_irrigation_data(state):
    """Get irrigation availability data"""
    conn = sqlite3.connect('WareHouse/irrigation_transformed.db')
    
    query = """
    SELECT 
        total_irrigated_area,
        canal_percentage,
        tank_percentage,
        tubewell_percentage,
        other_sources_percentage,
        irrigation_coverage_ratio,
        irrigation_growth_rate,
        water_source_diversity_score
    FROM transformed_irrigation
    WHERE LOWER(state) = LOWER(?)
    ORDER BY year DESC
    LIMIT 1
    """
    
    try:
        df = pd.read_sql_query(query, conn, params=[state])
        conn.close()
        
        if df.empty:
            print(f"No irrigation data found for {state}")
            return {
                'irrigation_coverage': 50.0,
                'growth_trend': 0.0,
                'water_availability_score': 50.0
            }
        
        return {
            'irrigation_coverage': df['irrigation_coverage_ratio'].iloc[0],
            'growth_trend': df['irrigation_growth_rate'].iloc[0],
            'water_availability_score': df['water_source_diversity_score'].iloc[0]
        }
        
    except Exception as e:
        print(f"Error getting irrigation data: {str(e)}")
        conn.close()
        return {
            'irrigation_coverage': 50.0,
            'growth_trend': 0.0,
            'water_availability_score': 50.0
        }

def get_crop_price_trends(state, crop):
    """Get historical crop price trends"""
    conn = sqlite3.connect('WareHouse/crop_prices_transformed.db')
    
    query = """
    SELECT 
        arrival_date,
        price_per_quintal,
        monthly_avg_price,
        price_trend_indicator,
        seasonal_index,
        price_volatility,
        district,
        market
    FROM transformed_crop_prices
    WHERE LOWER(state) = LOWER(?) 
    AND LOWER(commodity) = LOWER(?)
    ORDER BY arrival_date DESC
    LIMIT 365
    """
    
    try:
        df = pd.read_sql_query(query, conn, params=[state, crop])
        conn.close()
        
        if df.empty:
            print(f"No price data found for {crop} in {state}")
            return None
            
        return df
        
    except Exception as e:
        print(f"Error getting price trends: {str(e)}")
        conn.close()
        return None

def analyze_profit_potential(crop, state, district=None):
    """Analyze profit potential for a crop in a given location"""
    # Get weather conditions
    weather_data = get_weather_conditions(state, district)
    
    # Get soil health data
    soil_data = get_soil_health_data(state, district)
    
    # Get price trends
    price_data = get_crop_price_trends(state, crop)
    
    # Get irrigation data
    irrigation_data = get_irrigation_data(state)

    
    # Calculate risk factors
    risk_factors = {
        'weather': {
            'growing_conditions': weather_data.get('growing_condition_score', 50.0),
            'moisture_status': weather_data.get('soil_moisture_surface', 0.5),
            'temperature_variation': abs(weather_data.get('temperature_max', 30) - 
                                      weather_data.get('temperature_min', 20)),
            'precipitation': weather_data.get('precipitation_sum', 0)
        },
        'soil_health': {
            'score': soil_data.get('overall_soil_health_score', 50.0),
            'npk_status': soil_data.get('npk_score', 50.0),
            'micro_nutrients': soil_data.get('micro_score', 50.0),
            'ph_level': soil_data.get('ph_level', 'Neutral'),
            'ec_level': soil_data.get('ec_level', 'Non Saline')
        },
        'irrigation': {
            'coverage': irrigation_data.get('irrigation_coverage', 0),
            'growth_rate': irrigation_data.get('growth_trend', 0)
        }
    }
    
    # Calculate profit potential score (0-100)
    weather_score = risk_factors['weather']['growing_conditions']
    soil_score = risk_factors['soil_health']['score']
    irrigation_score = irrigation_data.get('irrigation_coverage', 50.0)
    
    profit_potential = (
        weather_score * 0.4 +
        soil_score * 0.35 +
        irrigation_score * 0.25
    )
    
    # Generate recommendations
    recommendations = []
    
    if soil_score < 60:
        recommendations.append("Consider soil improvement measures before planting")
    if irrigation_score < 40:
        recommendations.append("Invest in irrigation infrastructure or choose drought-resistant varieties")
    if weather_score < 50:
        recommendations.append("Consider protected cultivation methods")
    
    return {
        'crop': crop,
        'location': {'state': state, 'district': district},
        'profit_potential_score': profit_potential,
        'risk_factors': risk_factors,
        'recommendations': recommendations
    }

def main():
    # Get location details (example coordinates for Visakhapatnam)
    lat, lon = 17.6868, 83.2185
    address, state, district = get_location_details(lat, lon)
    
    print("\nLocation Details:")
    print(f"Address: {address}")
    print(f"State: {state}")
    print(f"District: {district}")
    
    # Get crop input
    crop = input("Enter the crop name you want to analyze: ").lower()
    
    # Get analysis
    analysis = analyze_profit_potential(
        crop=crop,
        state=state,
        district=district
    )
    
    # Print results
    console.print("[bold cyan]\nProfit Maximization Analysis Results:[/bold cyan]")
    print(f"Crop: {analysis['crop']}")
    print(f"Location: {analysis['location']}")
    print(f"\nProfit Potential Score: {analysis['profit_potential_score']:.2f}/100")
    
    if analysis['risk_factors']:
        console.print("[bold bright_red]\nRisk Assessment:[/bold bright_red]")
        for category, factors in analysis['risk_factors'].items():
            print(f"\n{category.title()}:")
            for factor, value in factors.items():
                print(f"- {factor}: {value}")
    
    if analysis['recommendations']:
        console.print("[bold green]\nFinancial Recommendations:[/bold green]")
        for rec in analysis['recommendations']:
            print(f"- {rec}")

if __name__ == "__main__":
    main()