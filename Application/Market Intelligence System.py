import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from geopy.geocoders import Nominatim
from rich.console import Console

console = Console()

def get_price_trends(crop, state, months=6):
    """Get historical price trends and statistics aggregated for entire state"""
    conn = sqlite3.connect('WareHouse/crop_prices_transformed.db')
    
    query = """
    SELECT 
        arrival_date,
        AVG(price_per_quintal) as price_per_quintal,
        AVG(monthly_avg_price) as monthly_avg_price,
        AVG(price_trend_indicator) as price_trend_indicator,
        AVG(seasonal_index) as seasonal_index,
        AVG(price_volatility) as price_volatility,
        COUNT(DISTINCT district) as district_count
    FROM transformed_crop_prices
    WHERE LOWER(commodity) = LOWER(?) 
    AND LOWER(state) = LOWER(?)
    GROUP BY arrival_date
    ORDER BY arrival_date DESC 
    LIMIT ?
    """
    
    df = pd.read_sql_query(query, conn, params=[crop, state, months * 30])
    conn.close()
    
    if df.empty:
        print(f"Warning: No data found for {crop} in {state}")
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
        },
        'district_coverage': df['district_count'].iloc[0]
    }
    
    return result

def get_soil_suitability(crop, state):
    """Analyze soil suitability for the crop"""
    try:
        conn = sqlite3.connect('WareHouse/soil_health_transformed.db')
        query = """
        SELECT 
            AVG(nitrogen_level) as nitrogen_level,
            AVG(phosphorous_level) as phosphorous_level,
            AVG(potassium_level) as potassium_level,
            AVG(organic_carbon_level) as organic_carbon_level,
            AVG(npk_score) as npk_score,
            AVG(micro_score) as micro_score,
            AVG(overall_soil_health_score) as overall_soil_health_score,
            GROUP_CONCAT(DISTINCT district) as districts,
            GROUP_CONCAT(DISTINCT ph_level) as ph_levels,
            GROUP_CONCAT(DISTINCT ec_level) as ec_levels
        FROM soil_health
        WHERE LOWER(state) = LOWER(?)
        GROUP BY state
        """
        
        df = pd.read_sql_query(query, conn, params=[state])
        conn.close()
        
        if df.empty:
            print(f"Warning: No soil data found for {state}")
            return {
                'soil_health_score': 50.0,
                'npk_status': 50.0,
                'ph_level': 'Neutral',
                'ec_level': 'Non Saline',
                'suitability_score': 50.0,
                'districts': 'No data'
            }
        
        # Calculate NPK status
        npk_status = df['npk_score'].iloc[0]
        
        # Get most common pH and EC levels
        ph_levels = df['ph_levels'].iloc[0].split(',')
        ec_levels = df['ec_levels'].iloc[0].split(',')
        
        return {
            'soil_health_score': df['overall_soil_health_score'].iloc[0],
            'npk_status': npk_status,
            'ph_level': max(set(ph_levels), key=ph_levels.count),
            'ec_level': max(set(ec_levels), key=ec_levels.count),
            'suitability_score': df['overall_soil_health_score'].iloc[0] * 0.8,
            'districts': df['districts'].iloc[0]
        }
        
    except Exception as e:
        print(f"Warning: Error getting soil data - {str(e)}")
        return {
            'soil_health_score': 50.0,
            'npk_status': 50.0,
            'ph_level': 'Neutral',
            'ec_level': 'Non Saline',
            'suitability_score': 50.0,
            'districts': 'Error getting data'
        }

def get_irrigation_status(state):
    """Get irrigation availability and infrastructure details"""
    conn = sqlite3.connect('WareHouse/irrigation_transformed.db')
    
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
    conn.close()
    
    if df.empty:
        print(f"Warning: No irrigation data found for {state}")
        return None
    
    return {
        'irrigation_coverage': df['irrigation_coverage_ratio'].iloc[0],
        'growth_trend': df['irrigation_growth_rate'].iloc[0],
        'water_availability_score': df['water_source_diversity_score'].iloc[0]
    }

def get_market_insights(crop, state):
    """Comprehensive market intelligence analysis"""
    
    # Get price trends
    price_data = get_price_trends(crop, state)
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
    soil_data = get_soil_suitability(crop, state)
    if not soil_data:
        soil_data = {
            'soil_health_score': 50.0,
            'npk_status': 50.0,
            'ph_level': 'Neutral',
            'ec_level': 'Non Saline',
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

def get_location_crop_statistics(state):
    """Get aggregated price statistics for all crops in state"""
    conn = sqlite3.connect('WareHouse/crop_prices_transformed.db')
    
    query = """
    SELECT 
        commodity as crop,
        COUNT(*) as total_records,
        COUNT(DISTINCT district) as district_count,
        ROUND(AVG(price_per_quintal), 2) as avg_price,
        ROUND(MIN(price_per_quintal), 2) as min_price,
        ROUND(MAX(price_per_quintal), 2) as max_price,
        ROUND(AVG(price_volatility) * 100, 1) as avg_volatility,
        ROUND(AVG(seasonal_index), 2) as seasonal_index
    FROM transformed_crop_prices
    WHERE LOWER(state) = LOWER(?)
    GROUP BY commodity
    ORDER BY avg_price DESC
    """
    
    df = pd.read_sql_query(query, conn, params=[state])
    conn.close()
    
    if df.empty:
        print(f"No crop data found for {state}")
        return None
    
    return df

def get_available_crops(state):
    """Get list of crops with available price data"""
    conn = sqlite3.connect('WareHouse/crop_prices_transformed.db')
    
    query = """
    SELECT DISTINCT commodity
    FROM transformed_crop_prices
    WHERE LOWER(state) = LOWER(?)
    """
    params = [state]
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    return df['commodity'].tolist() if not df.empty else []

def print_location_analysis(state):
    """Print comprehensive state-level crop analysis"""
    print("\nState-wide Crop Market Analysis Report")
    print("=" * 50)
    print(f"State: {state}")
    
    # Get available crops first
    available_crops = get_available_crops(state)
    if not available_crops:
        print(f"\nNo crop price data available for {state}")
        return
    
    print(f"\nTotal Crops Analyzed: {len(available_crops)}")
    
    # Get crop statistics and market insights for each crop
    stats = get_location_crop_statistics(state)
    
    if stats is None:
        return
        
    # Print state-wide metrics
    soil_data = get_soil_suitability(None, state)
    irrigation_data = get_irrigation_status(state)
    
    print("\nState-wide Agricultural Metrics")
    print("-" * 40)
    if soil_data:
        print("\nSoil Health Summary:")
        print(f"- Overall Soil Health: {soil_data['soil_health_score']:.1f}/100")
        print(f"- NPK Status: {soil_data['npk_status']:.1f}/100")
        print(f"- Predominant pH Level: {soil_data['ph_level']}")
        print(f"- Predominant EC Level: {soil_data['ec_level']}")
    
    if irrigation_data:
        print("\nIrrigation Infrastructure:")
        print(f"- State Coverage: {irrigation_data['irrigation_coverage']:.1f}%")
        print(f"- Growth Trend: {irrigation_data['growth_trend']:.1f}%")
        print(f"- Water Availability: {irrigation_data['water_availability_score']:.1f}/100")
    
    print("\nCrop-wise Market Analysis")
    print("=" * 50)
    
    # Analyze each crop
    for _, row in stats.iterrows():
        crop = row['crop']
        print(f"\n{crop}")
        print("-" * len(crop))
        
        console.print(f"[bold yellow]Market Coverage: {row['district_count']} districts[/bold yellow]")
        
        # Price Statistics
        print("\nPrice Statistics (State Average):")
        print(f"- Average Price: ₹{row['avg_price']:,.2f} per quintal")
        print(f"- Price Range: ₹{row['min_price']:,.2f} - ₹{row['max_price']:,.2f}")
        print(f"- Price Volatility: {row['avg_volatility']}%")
        print(f"- Seasonal Index: {row['seasonal_index']:.2f}")
        print(f"- Total Market Records: {row['total_records']}")
        
        # Get detailed market insights
        insights = get_market_insights(crop, state)
        if insights:
            console.print("\n[bold cyan]Market Intelligence:[/bold cyan]")
            print(f"- Market Risk Score: {insights['market_summary']['market_risk_score']:.1f}/100")
            print(f"- Soil Suitability Score: {insights['market_summary']['soil_suitability']:.1f}/100")
            
            forecast = insights['price_forecast']['short_term']
            print(f"\nState-wide Price Forecast:")
            print(f"- Expected Range: ₹{forecast['range']['low']:,.2f} - ₹{forecast['range']['high']:,.2f}")
            
            print("\nMarket Timing:")
            for period in insights['best_selling_periods']:
                print(f"- {period}")
            
            console.print("[bold green]\nStrategic Recommendations:[/bold green]")
            for rec in insights['recommendations']:
                print(f"- {rec}")
        
        print("\n" + "="*50)

def get_location_details(lat, lon):
    """Get location details from latitude and longitude"""
    geolocator = Nominatim(user_agent="IIA")
    location = geolocator.reverse(f"{lat},{lon}")
    state = location.raw['address'].get('state')
    district = location.raw['address'].get('state_district')
    return location.address, state, district

def main():
    # Example coordinates (Visakhapatnam)
    lat, lon = 17.6868, 83.2185
    
    # Get location details
    address, state, _ = get_location_details(lat, lon)
    if state is None:
        print("Could not determine state location. Please check your connection.")
        return
    
    print(f"\nLocation Details:")
    print(f"Address: {address}")
    print(f"State: {state}")
    
    # Generate state-wide analysis
    print_location_analysis(state)

if __name__ == "__main__":
    main()