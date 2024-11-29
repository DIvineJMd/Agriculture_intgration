import sqlite3
import pandas as pd
from datetime import datetime
import numpy as np

def get_soil_health_data(state, district=None):
    """Get soil health data for given location"""
    conn = sqlite3.connect('Transformed_database/soil_health_transformed.db')
    
    query = """
    SELECT *
    FROM soil_health
    WHERE state = ?
    """
    params = [state]
    
    if district:
        query += " AND district = ?"
        params.append(district)
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    if df.empty:
        return None
        
    # Separate numeric and categorical columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    # Calculate means only for numeric columns
    result = {}
    
    # Handle numeric columns
    numeric_means = df[numeric_cols].mean()
    for col in numeric_cols:
        result[col] = numeric_means[col]
    
    # Handle categorical columns (take most frequent value)
    categorical_cols = df.select_dtypes(exclude=[np.number]).columns
    categorical_modes = df[categorical_cols].mode().iloc[0]
    for col in categorical_cols:
        result[col] = categorical_modes[col]
    return result

def get_weather_conditions(state, district=None):
    """Get weather and soil conditions"""
    conn = sqlite3.connect('Transformed_database/weather_data.db')
    
    # Normalize state name to match database
    state = state.title()  # Convert "ANDHRA PRADESH" to "Andhra Pradesh"
    if district:
        district = district.title()  # Also normalize district name
    
    
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
            COALESCE(precipitation_sum, 0) + COALESCE(rain_sum, 0) as total_precipitation,
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
        WHERE state = ?
        {}  -- Placeholder for district filter
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
    
    district_filter = "AND district = ?" if district else ""
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

def get_crop_price_trends(state, crop):
    """Get historical crop price trends"""
    conn = sqlite3.connect('Transformed_database/crop_prices_transformed.db')
    state = state.title()
    df = pd.read_sql_query("""
        SELECT *
        FROM transformed_crop_prices
        WHERE state = ? AND commodity = ?
        ORDER BY arrival_date DESC
        LIMIT 365
    """, conn, params=[state, crop])
    conn.close()
    return df if not df.empty else None

def get_irrigation_data(state):
    """Get irrigation availability data"""
    conn = sqlite3.connect('Transformed_database/irrigation_transformed.db') # 
    
    df = pd.read_sql_query("""
        SELECT *
        FROM transformed_irrigation
        WHERE state = ?
        ORDER BY year DESC
        LIMIT 1
    """, conn, params=[state])
    conn.close()
    return df.iloc[0] if not df.empty else None

def analyze_profit_potential(crop, state, district=None):
    """Analyze profit potential for given crop and location"""
    
    # Initialize empty dictionaries for results
    risk_factors = {}
    profit_factors = {}
    
    # Get soil health data
    soil_data = get_soil_health_data(state, district)
    if soil_data is None:
        soil_data = get_soil_health_data(state)  # Fallback to state level
    
    # Get weather conditions
    weather_data = get_weather_conditions(state, district)
    if weather_data is None:
        weather_data = get_weather_conditions(state)  # Fallback to state level
    
    if weather_data is not None:
        # Weather risk assessment with detailed metrics
        risk_factors['weather'] = {
            'growing_conditions': weather_data.get('growing_condition_score', 0),
            'moisture_status': weather_data.get('soil_moisture_surface', 0),
            'temperature_variation': abs(
                weather_data.get('temperature_max', 0) - 
                weather_data.get('temperature_min', 0)
            ),
            'precipitation': weather_data.get('precipitation_sum', 0)
        }
    else:
        print(" No weather data found at all")
    
    # Get crop price trends
    price_trends = get_crop_price_trends(state, crop)
    
    # Get irrigation data
    irrigation_data = get_irrigation_data(state)
    
    if soil_data is not None:
        # Soil health risk assessment
        risk_factors['soil_health'] = {
            'score': soil_data.get('overall_soil_health_score', 0),
            'npk_status': soil_data.get('npk_score', 0),
            'micro_nutrients': soil_data.get('micro_score', 0),
            'ph_level': soil_data.get('ph_level', 'Unknown'),
            'ec_level': soil_data.get('ec_level', 'Unknown')
        }
    
    if price_trends is not None and not price_trends.empty:
        # Price risk and profit potential assessment
        avg_price = price_trends['price_per_quintal'].mean()
        price_volatility = price_trends['price_volatility'].mean()
        seasonal_index = price_trends['seasonal_index'].mean()
        
        profit_factors['price'] = {
            'average_price': float(avg_price),
            'price_volatility': float(price_volatility),
            'seasonal_strength': float(seasonal_index)
        }
    
    if irrigation_data is not None:
        # Irrigation risk assessment
        risk_factors['irrigation'] = {
            'coverage': float(irrigation_data.get('irrigation_coverage_ratio', 0)),
            'growth_rate': float(irrigation_data.get('irrigation_growth_rate', 0))
        }
    
    # Calculate overall profit potential score (0-100)
    profit_score = calculate_profit_score(risk_factors, profit_factors)
    
    # Determine best planting times based on seasonal index
    best_planting_times = recommend_planting_times(price_trends) if price_trends is not None else []
    
    # Generate recommendations
    recommendations = generate_recommendations(risk_factors, profit_factors)
    
    # Return complete analysis results
    return {
        'crop': crop,
        'location': {'state': state, 'district': district},
        'profit_potential_score': profit_score,
        'risk_factors': risk_factors,
        'profit_factors': profit_factors,
        'best_planting_times': best_planting_times,
        'recommendations': recommendations
    }

def calculate_profit_score(risk_factors, profit_factors):
    """Calculate overall profit potential score"""
    score = 50  # Base score
    
    # Adjust for soil health
    if 'soil_health' in risk_factors:
        soil_health = risk_factors['soil_health']
        if isinstance(soil_health['score'], (int, float)):
            score += soil_health['score'] * 0.2
        if isinstance(soil_health['npk_status'], (int, float)):
            score += soil_health['npk_status'] * 0.15
        if isinstance(soil_health['micro_nutrients'], (int, float)):
            score += soil_health['micro_nutrients'] * 0.15
    
    # Adjust for weather conditions
    if 'weather' in risk_factors:
        weather = risk_factors['weather']
        if isinstance(weather['growing_conditions'], (int, float)):
            score += weather['growing_conditions'] * 0.15
        if isinstance(weather['moisture_status'], (int, float)):
            score += weather['moisture_status'] * 0.1
    
    # Adjust for price factors
    if 'price' in profit_factors:
        price_data = profit_factors['price']
        if isinstance(price_data['seasonal_strength'], (int, float)):
            score += (price_data['seasonal_strength'] * 10) * 0.25
        if isinstance(price_data['price_volatility'], (int, float)):
            score -= (price_data['price_volatility'] * 10) * 0.1
    
    # Adjust for irrigation
    if 'irrigation' in risk_factors:
        irrigation = risk_factors['irrigation']
        if isinstance(irrigation['coverage'], (int, float)):
            score += irrigation['coverage'] * 0.1
        if isinstance(irrigation['growth_rate'], (int, float)):
            score += irrigation['growth_rate'] * 0.05
    
    return min(max(score, 0), 100)  # Ensure score is between 0 and 100

def recommend_planting_times(price_trends):
    """Determine optimal planting times based on price seasonality"""
    if price_trends is None:
        return []
    
    # Group by month and calculate average seasonal index
    price_trends['month'] = pd.to_datetime(price_trends['arrival_date']).dt.month
    monthly_indices = price_trends.groupby('month')['seasonal_index'].mean()
    
    # Find months with above-average seasonal index
    good_months = monthly_indices[monthly_indices > 1].index.tolist()
    
    # Convert month numbers to names
    month_names = {
        1: 'January', 2: 'February', 3: 'March', 4: 'April',
        5: 'May', 6: 'June', 7: 'July', 8: 'August',
        9: 'September', 10: 'October', 11: 'November', 12: 'December'
    }
    
    return [month_names[m] for m in good_months]

def generate_recommendations(risk_factors, profit_factors):
    """Generate specific recommendations based on analysis"""
    recommendations = []
    
    if 'soil_health' in risk_factors:
        soil_score = risk_factors['soil_health']['score']
        if soil_score < 60:
            recommendations.append("Consider soil improvement measures before planting")
    
    if 'weather' in risk_factors:
        growing_conditions = risk_factors['weather']['growing_conditions']
        if growing_conditions < 50:
            recommendations.append("Monitor weather conditions closely; consider delayed planting")
    
    if 'price' in profit_factors:
        if profit_factors['price']['price_volatility'] > 0.2:
            recommendations.append("High price volatility - consider forward contracts")
    
    return recommendations

if __name__ == "__main__":
    # Example usage for Andhra Pradesh, Anantapur
    analysis = analyze_profit_potential(
        crop="Maize",
        state="ANDHRA PRADESH",
        district="ANANTAPUR"
    )
    
    print("\nProfit Maximization Analysis Results:")
    print(f"Crop: {analysis['crop']}")
    print(f"Location: {analysis['location']}")
    print(f"\nProfit Potential Score: {analysis['profit_potential_score']:.2f}/100")
    
    # Print financial metrics first
    if 'profit_factors' in analysis and 'price' in analysis['profit_factors']:
        price_data = analysis['profit_factors']['price']
        print("\nFinancial Metrics:")
        print(f"- Average Market Price: ₹{price_data['average_price']:.2f} per quintal")
        print(f"- Price Volatility: {price_data['price_volatility']*100:.1f}%")
        print(f"- Seasonal Price Strength: {price_data['seasonal_strength']:.2f}")
        
        # Calculate estimated returns
        base_yield = 25  # Example base yield in quintals per acre
        estimated_yield = base_yield * (analysis['profit_potential_score']/100)
        estimated_revenue = estimated_yield * price_data['average_price']
        
        print("\nEstimated Returns (per acre):")
        print(f"- Projected Yield: {estimated_yield:.1f} quintals")
        print(f"- Potential Revenue: ₹{estimated_revenue:,.2f}")
        
        # Add volatility range
        price_range_low = price_data['average_price'] * (1 - price_data['price_volatility'])
        price_range_high = price_data['average_price'] * (1 + price_data['price_volatility'])
        print(f"- Price Range: ₹{price_range_low:,.2f} - ₹{price_range_high:,.2f} per quintal")
    
    if analysis['risk_factors']:
        print("\nRisk Assessment:")
        for category, factors in analysis['risk_factors'].items():
            print(f"\n{category.title()}:")
            for factor, value in factors.items():
                if isinstance(value, (int, float)):
                    print(f"- {factor}: {value:.2f}")
                else:
                    print(f"- {factor}: {value}")
    
    if analysis['best_planting_times']:
        print("\nBest Planting Times for Maximum Returns:", ", ".join(analysis['best_planting_times']))
    
    if analysis['recommendations']:
        print("\nFinancial Recommendations:")
        for rec in analysis['recommendations']:
            print(f"- {rec}")
        
        # Add financial-specific recommendations
        if 'profit_factors' in analysis and 'price' in analysis['profit_factors']:
            price_data = analysis['profit_factors']['price']
            if price_data['seasonal_strength'] > 1.2:
                print("- Consider storage facilities to sell during peak price periods")
            if price_data['price_volatility'] < 0.1:
                print("- Stable prices suggest good opportunity for fixed-price contracts")
            if price_data['average_price'] > 2000:  # Example threshold
                print("- High market prices indicate good profit potential with proper risk management")