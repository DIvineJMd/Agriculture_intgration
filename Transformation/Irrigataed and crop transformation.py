import sqlite3
import pandas as pd
import os
from datetime import datetime, timedelta

def create_transformed_databases():
    """Create separate transformed databases for crop and irrigation data"""
    transform_dir = "Transformed_database"
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

def transform_crop_prices(source_conn, target_conn):
    """Transform crop prices data with additional analytics"""
    
    # Read source data
    df = pd.read_sql_query("""
        SELECT * FROM crop_prices
        ORDER BY Arrival_Date
    """, source_conn)
    
    transformed_data = []
    
    # Group by state, district, market, commodity
    for name, group in df.groupby(['State', 'District', 'Market', 'Commodity']):
        state, district, market, commodity = name
        
        # Sort by date
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

def transform_irrigation_data(source_conn, target_conn):
    """Transform irrigation data with additional analytics"""
    
    # Read source data
    df = pd.read_sql_query("""
        SELECT * FROM irrigated_area
        ORDER BY Year
    """, source_conn)
    
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
    # Connect to source databases
    crop_source_conn = sqlite3.connect('database/crop_prices.db')
    irr_source_conn = sqlite3.connect('database/irrigated_area.db')
    
    # Create and connect to transformed databases
    crop_target_conn, irr_target_conn = create_transformed_databases()
    
    # Transform the data
    print("Transforming crop prices data...")
    transform_crop_prices(crop_source_conn, crop_target_conn)
    
    print("Transforming irrigation data...")
    transform_irrigation_data(irr_source_conn, irr_target_conn)
    
    # Close connections
    crop_source_conn.close()
    irr_source_conn.close()
    crop_target_conn.close()
    irr_target_conn.close()
    
    print("Crop and irrigation data transformation completed successfully!")

if __name__ == "__main__":
    main()