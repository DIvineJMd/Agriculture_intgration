import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

def get_price_trends(crop, state, district=None, months=6):
    """Get historical price trends and statistics"""
    conn = sqlite3.connect('Transformed_database/crop_prices_transformed.db')
    district=district.title()
    # Normalize input
    state = state.title()  # "ANDHRA PRADESH" -> "Andhra Pradesh"
    crop = crop.title()    # "MAIZE" -> "Maize"
    
    query = """
    SELECT 
        arrival_date,
        price_per_quintal,
        monthly_avg_price,
        price_trend_indicator,
        seasonal_index,
        price_volatility
    FROM transformed_crop_prices
    WHERE LOWER(commodity) = LOWER(?) 
    AND LOWER(state) = LOWER(?)
    """
    params = [crop, state]
    
    if district:
        query += " AND LOWER(district) = LOWER(?)"
        params.append(district)
    
    query += " ORDER BY arrival_date DESC LIMIT ?"
    params.append(months * 30)
    
    # print(f"\nDebug: Executing query with params: {params}")
    df = pd.read_sql_query(query, conn, params=params)
    # print(df)
    if df.empty:
        print(f"Warning: No data found for {crop} in {state}")
        conn.close()
        return None
    
    result = {
        'current_price': df['price_per_quintal'].iloc[0],
        'avg_price': df['monthly_avg_price'].mean(),
        'price_trend': df['price_trend_indicator'].mean(),
        'volatility': df['price_volatility'].mean(),
        'seasonal_strength': df['seasonal_index'].std(),
        'price_range': {
            'min': df['price_per_quintal'].min(),
            'max': df['price_per_quintal'].max()
        }
    }
    
    conn.close()
    return result

def get_soil_suitability(crop, state, district=None):
    """Analyze soil suitability for the crop"""
    conn = sqlite3.connect('Transformed_database/soil_health_transformed.db')
    
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
    
    return {
        'soil_health_score': df['overall_soil_health_score'].mean(),
        'npk_status': df['npk_score'].mean(),
        'ph_level': df['ph_level'].iloc[0],
        'suitability_score': df['overall_soil_health_score'].mean() * 0.8  # Simplified suitability
    }

def get_irrigation_status(state):
    """Get irrigation availability and infrastructure details"""
    conn = sqlite3.connect('Transformed_database/irrigation_transformed.db')
    
   
    # Normalize state name
    state = state.title()
    
    query = """
    SELECT 
        total_irrigated_area,
        irrigation_coverage_ratio,
        irrigation_growth_rate,
        water_source_diversity_score
    FROM transformed_irrigation
    WHERE LOWER(state) = LOWER(?)
    ORDER BY year DESC
    LIMIT 1
    """
    
    df = pd.read_sql_query(query, conn, params=[state])
    # print(df)
    conn.close()
    
    if df.empty:
        print(f"Warning: No irrigation data found for {state}")
        return None
    
    return {
        'irrigation_coverage': df['irrigation_coverage_ratio'].iloc[0],
        'growth_trend': df['irrigation_growth_rate'].iloc[0],
        'water_availability_score': df['water_source_diversity_score'].iloc[0]
    }

def get_market_insights(crop, state, district=None):
    """Comprehensive market intelligence analysis"""
    
    # Get price trends
    price_data = get_price_trends(crop, state, district)
    if not price_data:
        price_data = {
            'current_price': 0.0,
            'avg_price': 0.0,
            'price_trend': 0.0,
            'volatility': 0.0,
            'seasonal_strength': 1.0,
            'price_range': {'min': 0.0, 'max': 0.0}
        }
        print(f"Warning: No price data available for {crop} in {state}")
    
    # Get soil suitability
    soil_data = get_soil_suitability(crop, state, district)
    if not soil_data:
        soil_data = {
            'soil_health_score': 50.0,
            'npk_status': 50.0,
            'ph_level': 'Neutral',
            'suitability_score': 50.0
        }
        print(f"Warning: No soil data available for {state}")
    
    # Get irrigation status
    irrigation_data = get_irrigation_status(state)
    if not irrigation_data:
        irrigation_data = {
            'irrigation_coverage': 50.0,
            'growth_trend': 0.0,
            'water_availability_score': 50.0
        }
        print(f"Warning: No irrigation data available for {state}")
    
    # Calculate market risk score (0-100)
    market_risk = calculate_market_risk(price_data, soil_data, irrigation_data)
    
    # Generate price forecast
    price_forecast = generate_price_forecast(price_data)
    
    # Determine best selling periods
    selling_periods = determine_selling_periods(price_data)
    
    # Generate recommendations
    recommendations = generate_recommendations(
        price_data, soil_data, irrigation_data, market_risk
    )
    
    return {
        'market_summary': {
            'current_price': price_data['current_price'],
            'price_trend': price_data['price_trend'],
            'market_risk_score': market_risk,
            'soil_suitability': soil_data['suitability_score']
        },
        'price_forecast': price_forecast,
        'best_selling_periods': selling_periods,
        'recommendations': recommendations,
        'detailed_metrics': {
            'price_metrics': price_data,
            'soil_metrics': soil_data,
            'irrigation_metrics': irrigation_data
        }
    }

def calculate_market_risk(price_data, soil_data, irrigation_data):
    """Calculate overall market risk score"""
    risk_score = 100
    
    # Price volatility impact (higher volatility = higher risk)
    risk_score -= price_data['volatility'] * 20
    
    # Soil suitability impact (lower suitability = higher risk)
    risk_score -= (100 - soil_data['suitability_score']) * 0.3
    
    # Irrigation availability impact
    risk_score -= (100 - irrigation_data['water_availability_score']) * 0.2
    
    return max(min(risk_score, 100), 0)  # Ensure score is between 0-100

def generate_price_forecast(price_data):
    """Generate price forecasts based on trends"""
    current_price = price_data['current_price']
    trend = price_data['price_trend']
    volatility = price_data['volatility']
    
    return {
        'short_term': {
            'price': current_price * (1 + trend),
            'range': {
                'low': current_price * (1 + trend - volatility),
                'high': current_price * (1 + trend + volatility)
            }
        },
        'long_term': {
            'price': current_price * (1 + trend * 2),
            'confidence': 'medium' if volatility < 0.2 else 'low'
        }
    }

def determine_selling_periods(price_data):
    """Determine optimal selling periods based on seasonal patterns"""
    seasonal_strength = price_data['seasonal_strength']
    
    if seasonal_strength > 1.2:
        return ['Peak season (based on historical data)']
    elif seasonal_strength > 1.1:
        return ['Multiple favorable periods throughout the year']
    else:
        return ['No strong seasonal pattern - monitor market conditions']

def generate_recommendations(price_data, soil_data, irrigation_data, market_risk):
    """Generate actionable recommendations"""
    recommendations = []
    
    # Price-based recommendations
    if price_data['volatility'] > 0.2:
        recommendations.append("Consider forward contracts to manage price risk")
    if price_data['seasonal_strength'] > 1.2:
        recommendations.append("Plan storage for peak price periods")
    
    # Soil-based recommendations
    if soil_data['suitability_score'] < 70:
        recommendations.append("Invest in soil improvement before increasing production")
    
    # Risk-based recommendations
    if market_risk < 60:
        recommendations.append("Diversify crop portfolio to manage risk")
    
    # Irrigation-based recommendations
    if irrigation_data['water_availability_score'] < 50:
        recommendations.append("Consider drought-resistant varieties")
    
    return recommendations

def get_location_crop_statistics(state, district=None):
    """Get price statistics for all crops in a location"""
    conn = sqlite3.connect('Transformed_database/crop_prices_transformed.db')
    
    # Normalize state name
    state = state.title()
    
    query = """
    SELECT 
        commodity as crop,
        COUNT(*) as total_records,
        ROUND(AVG(price_per_quintal), 2) as avg_price,
        ROUND(MIN(price_per_quintal), 2) as min_price,
        ROUND(MAX(price_per_quintal), 2) as max_price,
        ROUND(AVG(price_volatility) * 100, 1) as avg_volatility,
        ROUND(AVG(seasonal_index), 2) as seasonal_index
    FROM transformed_crop_prices
    WHERE LOWER(state) = LOWER(?)
    """
    params = [state]
    
    if district:
        query += " AND LOWER(district) = LOWER(?)"
        params.append(district.title())
    
    query += """
    GROUP BY commodity
    ORDER BY avg_price DESC
    """
    
    # Pass both query and params to read_sql_query
    df = pd.read_sql_query(query, conn, params=params)
    
    conn.close()
    
    if df.empty:
        print(f"No crop data found for {state}")
        return None
    
    return df

def get_available_crops(state, district=None):
    """Get list of crops with available price data"""
    conn = sqlite3.connect('Transformed_database/crop_prices_transformed.db')
    
    query = """
    SELECT DISTINCT commodity
    FROM transformed_crop_prices
    WHERE LOWER(state) = LOWER(?)
    """
    params = [state]
    
    if district:
        query += " AND LOWER(district) = LOWER(?)"
        params.append(district)
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    return df['commodity'].tolist()

def print_location_analysis(state, district=None):
    """Print comprehensive location-based crop analysis"""
    print("\nCrop Market Analysis Report")
    print("=" * 50)
    print(f"Location: {state}")
    if district:
        print(f"District: {district}")
    
    # Get available crops first
    available_crops = get_available_crops(state, district)
    if not available_crops:
        print(f"\nNo crop price data available for {state}")
        return
    
    print(f"\nAvailable Crops: {', '.join(available_crops)}")
    
    # Get crop statistics and market insights for each crop
    stats = get_location_crop_statistics(state, district)
    
    if stats is None:
        return
        
    # Print location-specific metrics first (same for all crops)
    soil_data = get_soil_suitability(None, state, district)
    irrigation_data = get_irrigation_status(state)
    
    print("\nLocation-specific Metrics")
    print("-" * 30)
    if soil_data:
        print("\nSoil Health Metrics:")
        print(f"- Overall Health Score: {soil_data['soil_health_score']:.1f}/100")
        print(f"- NPK Status: {soil_data['npk_status']:.1f}/100")
        print(f"- pH Level: {soil_data['ph_level']}")
    
    if irrigation_data:
        print("\nIrrigation Metrics:")
        print(f"- Coverage: {irrigation_data['irrigation_coverage']:.1f}%")
        print(f"- Growth Trend: {irrigation_data['growth_trend']:.1f}%")
        print(f"- Water Availability: {irrigation_data['water_availability_score']:.1f}/100")
    
    print("\nCrop-wise Analysis")
    print("=" * 50)
    
    # Analyze each crop
    for _, row in stats.iterrows():
        crop = row['crop']
        print(f"\n{crop}")
        print("-" * len(crop))
        
        # Price Statistics
        print("\nPrice Statistics:")
        print(f"- Average Price: ₹{row['avg_price']:,.2f} per quintal")
        print(f"- Price Range: ₹{row['min_price']:,.2f} - ₹{row['max_price']:,.2f}")
        print(f"- Price Volatility: {row['avg_volatility']}%")
        print(f"- Seasonal Strength: {row['seasonal_index']:.2f}")
        print(f"- Records Available: {row['total_records']}")
        
        # Get detailed market insights
        insights = get_market_insights(crop, state, district)
        if insights:
            print("\nMarket Intelligence:")
            print(f"- Market Risk Score: {insights['market_summary']['market_risk_score']:.1f}/100")
            print(f"- Soil Suitability Score: {insights['market_summary']['soil_suitability']:.1f}/100")
            
            forecast = insights['price_forecast']['short_term']
            print(f"\nPrice Forecast:")
            print(f"- Expected Range: ₹{forecast['range']['low']:,.2f} - ₹{forecast['range']['high']:,.2f}")
            
            print("\nBest Selling Periods:")
            for period in insights['best_selling_periods']:
                print(f"- {period}")
            
            print("\nRecommendations:")
            for rec in insights['recommendations']:
                print(f"- {rec}")
        
        print("\n" + "="*50)

if __name__ == "__main__":
    state = "ANDHRA PRADESH"
    district = "ANANTAPUR"
    print_location_analysis(state, district)