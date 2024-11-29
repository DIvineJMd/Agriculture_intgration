import sqlite3
import pandas as pd
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Add the project root directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from gcp_code.federator import DatabaseFederator

# Load environment variables
load_dotenv()

# Server configurations
SERVERS = [
    {
        "host": os.getenv("CROP_PRICES_HOST"),
        "port": int(os.getenv("CROP_PRICES_PORT")),
        "db_name": "crop_prices"
    },
    {
        "host": os.getenv("IRRIGATED_AREA_HOST"),
        "port": int(os.getenv("IRRIGATED_AREA_PORT")),
        "db_name": "irrigated_area"
    }
]

def validate_server_config():
    """Validate that all required environment variables are set"""
    for server in SERVERS:
        if None in server.values():
            raise EnvironmentError(
                f"Missing environment variables for {server['db_name']}. "
                "Please check your .env file."
            )

def create_transformed_databases():
    """Create separate transformed databases for crop and irrigation data"""
    transform_dir = "WareHouse"
    os.makedirs(transform_dir, exist_ok=True)
    
    # Create separate connections
    crop_conn = sqlite3.connect(f'{transform_dir}/crop_prices_transformed.db')
    irr_conn = sqlite3.connect(f'{transform_dir}/irrigation_transformed.db')
    
    # Create transformed crop prices table
    crop_cursor = crop_conn.cursor()
    crop_cursor.execute('''
    CREATE TABLE IF NOT EXISTS transformed_crop_prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state TEXT,
        district TEXT,
        market TEXT,
        commodity TEXT,
        variety TEXT,
        arrival_date DATE,
        price_per_quintal REAL,
        monthly_avg_price REAL,
        price_trend_indicator REAL,
        seasonal_index REAL,
        price_volatility REAL
    )
    ''')
    
    # Create transformed irrigation table
    irr_cursor = irr_conn.cursor()
    irr_cursor.execute('''
    CREATE TABLE IF NOT EXISTS transformed_irrigation (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state TEXT,
        year INTEGER,
        total_irrigated_area REAL,
        canal_percentage REAL,
        tank_percentage REAL,
        tubewell_percentage REAL,
        other_sources_percentage REAL,
        irrigation_coverage_ratio REAL,
        irrigation_growth_rate REAL,
        water_source_diversity_score REAL
    )
    ''')
    
    crop_conn.commit()
    irr_conn.commit()
    return crop_conn, irr_conn

def transform_crop_prices(federator, crop_server, target_conn):
    """Transform crop prices data with additional analytics"""
    
    # Fetch data using federator
    query = "SELECT * FROM crop_prices ORDER BY Arrival_Date"
    data = federator.query_server(crop_server, query)
    
    if data is None:
        print("Failed to fetch crop prices data from the server")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    transformed_data = []
    
    # Group by state, district, market, commodity
    for name, group in df.groupby(['State', 'District', 'Market', 'Commodity']):
        state, district, market, commodity = name
        
        # Rest of the transformation logic remains the same
        group = group.sort_values('Arrival_Date')
        
        for _, row in group.iterrows():
            # Calculate monthly average
            month_start = pd.to_datetime(row['Arrival_Date']).replace(day=1)
            month_data = group[pd.to_datetime(group['Arrival_Date']).dt.to_period('M') == 
                              pd.to_datetime(month_start).to_period('M')]
            monthly_avg = month_data['Modal_Price'].mean()
            
            # Calculate price trend (over last 30 days)
            date = pd.to_datetime(row['Arrival_Date'])
            last_30_days = group[
                (pd.to_datetime(group['Arrival_Date']) <= date) & 
                (pd.to_datetime(group['Arrival_Date']) > date - timedelta(days=30))
            ]
            if len(last_30_days) > 1:
                trend = (last_30_days['Modal_Price'].iloc[-1] - 
                        last_30_days['Modal_Price'].iloc[0]) / last_30_days['Modal_Price'].iloc[0]
            else:
                trend = 0
            
            # Calculate seasonal index
            yearly_avg = group[
                pd.to_datetime(group['Arrival_Date']).dt.year == 
                pd.to_datetime(row['Arrival_Date']).year
            ]['Modal_Price'].mean()
            seasonal_index = row['Modal_Price'] / yearly_avg if yearly_avg else 1
            
            # Calculate price volatility
            volatility = last_30_days['Modal_Price'].std() if len(last_30_days) > 1 else 0
            
            transformed_data.append({
                'state': state,
                'district': district,
                'market': market,
                'commodity': commodity,
                'variety': row['Variety'],
                'arrival_date': row['Arrival_Date'],
                'price_per_quintal': row['Modal_Price'],
                'monthly_avg_price': monthly_avg,
                'price_trend_indicator': trend,
                'seasonal_index': seasonal_index,
                'price_volatility': volatility
            })
    
    # Save transformed data
    transformed_df = pd.DataFrame(transformed_data)
    transformed_df.to_sql('transformed_crop_prices', target_conn, if_exists='replace', index=False)

def transform_irrigation_data(federator, irr_server, target_conn):
    """Transform irrigation data with additional analytics"""
    
    # Fetch data using federator
    query = "SELECT * FROM irrigated_area ORDER BY Year"
    data = federator.query_server(irr_server, query)
    
    if data is None:
        print("Failed to fetch irrigation data from the server")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    transformed_data = []
    
    # Group by state
    for state, group in df.groupby('State_Name'):
        # Sort by year
        group = group.sort_values('Year')
        
        for _, row in group.iterrows():
            # Calculate total irrigated area by summing all crop-specific areas
            crop_columns = [col for col in df.columns if 'IRRIGATED_AREA' in col]
            
            # Convert each column to numeric before summing
            total_area = sum(pd.to_numeric(row[col], errors='coerce') or 0 for col in crop_columns)
            
            # Since we don't have source-wise breakup, we'll set these to 0 or remove them
            canal_pct = 0
            tank_pct = 0
            tubewell_pct = 0
            other_pct = 100  # Or set to 0 if you prefer
            
            # Calculate growth rate
            prev_year = group[group['Year'] == row['Year'] - 1]
            if not prev_year.empty:
                prev_total = sum(pd.to_numeric(prev_year.iloc[0][col], errors='coerce') or 0 
                               for col in crop_columns)
                growth_rate = ((total_area - prev_total) / prev_total * 100) if prev_total else 0
            else:
                growth_rate = 0
            
            # Since we don't have source diversity, we'll set this to 0
            diversity_score = 0
            
            transformed_data.append({
                'state': state,
                'year': row['Year'],
                'total_irrigated_area': total_area,
                'canal_percentage': canal_pct,
                'tank_percentage': tank_pct,
                'tubewell_percentage': tubewell_pct,
                'other_sources_percentage': other_pct,
                'irrigation_coverage_ratio': 0,  # We don't have this data
                'irrigation_growth_rate': growth_rate,
                'water_source_diversity_score': diversity_score
            })
    
    # Save transformed data
    transformed_df = pd.DataFrame(transformed_data)
    transformed_df.to_sql('transformed_irrigation', target_conn, if_exists='replace', index=False)

def main():
    # Validate environment variables
    validate_server_config()
    
    # Initialize federator
    federator = DatabaseFederator(SERVERS)
    
    # Get server configurations
    crop_server = next((server for server in SERVERS if server["db_name"] == "crop_prices"), None)
    irr_server = next((server for server in SERVERS if server["db_name"] == "irrigated_area"), None)
    
    if not crop_server or not irr_server:
        print("Error: Server configurations not found")
        return
    
    # Create and connect to transformed databases
    crop_target_conn, irr_target_conn = create_transformed_databases()
    
    # Transform the data
    print("Transforming crop prices data...")
    transform_crop_prices(federator, crop_server, crop_target_conn)
    
    print("Transforming irrigation data...")
    transform_irrigation_data(federator, irr_server, irr_target_conn)
    
    # Close connections
    crop_target_conn.close()
    irr_target_conn.close()
    
    print("Crop and irrigation data transformation completed successfully!")

if __name__ == "__main__":
    main()