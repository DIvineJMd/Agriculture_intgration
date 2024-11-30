import sqlite3
import pandas as pd
import os
from rich.console import Console

console = Console()

def create_transformed_database():
    """Create transformed database for soil health data"""
    transform_dir = "oldcode"
    os.makedirs(transform_dir, exist_ok=True)
    
    conn = sqlite3.connect(f'{transform_dir}/soil_health_transformed.db')
    cursor = conn.cursor()
    
    # Create transformed soil health table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS soil_health (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        block TEXT,
        state TEXT,
        district TEXT,
        
        -- NPK Levels (calculated from ranges)
        nitrogen_level REAL,
        phosphorous_level REAL,
        potassium_level REAL,
        
        -- Soil Characteristics
        organic_carbon_level REAL,
        ec_level TEXT,
        ph_level TEXT,
        
        -- Micro Nutrients (as availability percentage)
        copper_availability REAL,
        boron_availability REAL,
        sulphur_availability REAL,
        iron_availability REAL,
        zinc_availability REAL,
        manganese_availability REAL,
        
        -- Overall Scores
        npk_score REAL,
        micro_score REAL,
        overall_soil_health_score REAL,
        
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    conn.commit()
    return conn

def calculate_weighted_level(high, medium, low):
    """Calculate weighted level from high, medium, low decimal values"""
    # Values are already in decimal form (e.g., 0.60 for 60%)
    # No need to convert from percentage strings
    return (high * 1.0 + medium * 0.5 + low * 0.0) * 100

def calculate_nutrient_availability(sufficient, deficient):
    """Calculate nutrient availability from decimal values"""
    # Value is already in decimal form (e.g., 0.99 for 99%)
    return sufficient * 100

def transform_soil_data(macro_conn, micro_conn, target_conn):
    """Transform and combine macro and micro nutrient data"""
    
    # Read macro and micro data from separate databases
    macro_df = pd.read_sql_query("""
        SELECT * FROM macro_nutrients 
        ORDER BY block, timestamp DESC
    """, macro_conn)
    
    micro_df = pd.read_sql_query("""
        SELECT * FROM micro_nutrients 
        ORDER BY block, timestamp DESC
    """, micro_conn)
    
    # Debug print to verify values
    print("\nSample macro values:")
    print(macro_df[['nitrogen_high', 'nitrogen_medium', 'nitrogen_low']].head(1))
    print("\nSample micro values:")
    print(micro_df[['copper_sufficient', 'copper_deficient']].head(1))
    
    # Process each block's data
    transformed_data = []
    
    for block in macro_df['block'].unique():
        macro_row = macro_df[macro_df['block'] == block].iloc[0]
        micro_row = micro_df[micro_df['block'] == block].iloc[0] if block in micro_df['block'].values else None
        
        # Calculate NPK levels (values are already in decimal form)
        nitrogen_level = calculate_weighted_level(
            macro_row['nitrogen_high'], 
            macro_row['nitrogen_medium'], 
            macro_row['nitrogen_low']
        )
        
        phosphorous_level = calculate_weighted_level(
            macro_row['phosphorous_high'], 
            macro_row['phosphorous_medium'], 
            macro_row['phosphorous_low']
        )
        
        potassium_level = calculate_weighted_level(
            macro_row['potassium_high'], 
            macro_row['potassium_medium'], 
            macro_row['potassium_low']
        )
        
        # Calculate organic carbon level
        oc_level = calculate_weighted_level(
            macro_row['oc_high'],
            macro_row['oc_medium'],
            macro_row['oc_low']
        )
        
        # Determine EC and pH levels (values are already in decimal form)
        ec_level = 'Non Saline' if macro_row['ec_non_saline'] > 0.5 else 'Saline'
        if macro_row['ph_acidic'] > 0.5:
            ph_level = 'Acidic'
        elif macro_row['ph_alkaline'] > 0.5:
            ph_level = 'Alkaline'
        else:
            ph_level = 'Neutral'
        
        # Calculate micro nutrient availability
        micro_nutrients = {
            'copper_availability': 0,
            'boron_availability': 0,
            'sulphur_availability': 0,
            'iron_availability': 0,
            'zinc_availability': 0,
            'manganese_availability': 0
        }
        
        if micro_row is not None:
            micro_nutrients = {
                'copper_availability': calculate_nutrient_availability(micro_row['copper_sufficient'], micro_row['copper_deficient']),
                'boron_availability': calculate_nutrient_availability(micro_row['boron_sufficient'], micro_row['boron_deficient']),
                'sulphur_availability': calculate_nutrient_availability(micro_row['sulphur_sufficient'], micro_row['sulphur_deficient']),
                'iron_availability': calculate_nutrient_availability(micro_row['iron_sufficient'], micro_row['iron_deficient']),
                'zinc_availability': calculate_nutrient_availability(micro_row['zinc_sufficient'], micro_row['zinc_deficient']),
                'manganese_availability': calculate_nutrient_availability(micro_row['manganese_sufficient'], micro_row['manganese_deficient'])
            }
        
        # Calculate scores
        npk_score = (nitrogen_level + phosphorous_level + potassium_level) / 3
        micro_score = sum(micro_nutrients.values()) / len(micro_nutrients)
        overall_score = (npk_score * 0.6 + micro_score * 0.4)  # Weighted average
        
        transformed_data.append({
            'block': block,
            'state': macro_row['state'],
            'district': macro_row['district'],
            'nitrogen_level': nitrogen_level,
            'phosphorous_level': phosphorous_level,
            'potassium_level': potassium_level,
            'organic_carbon_level': oc_level,
            'ec_level': ec_level,
            'ph_level': ph_level,
            **micro_nutrients,
            'npk_score': npk_score,
            'micro_score': micro_score,
            'overall_soil_health_score': overall_score
        })
    
    # Convert to DataFrame and save to database
    transformed_df = pd.DataFrame(transformed_data)
    transformed_df.to_sql('soil_health', target_conn, if_exists='replace', index=False)
    
    # Print sample of transformed data
    print("\nSample of transformed data:")
    print(transformed_df[['block', 'nitrogen_level', 'phosphorous_level', 'potassium_level']].head())

def main():
    # Connect to source databases (both macro and micro)
    macro_conn = sqlite3.connect('database/macro_nutrients.db')
    micro_conn = sqlite3.connect('database/micro_nutrients.db')
    
    # Create and connect to transformed database
    target_conn = create_transformed_database()
    
    # Transform the data
    transform_soil_data(macro_conn, micro_conn, target_conn)
    
    # Close all connections
    macro_conn.close()
    micro_conn.close()
    target_conn.close()
    
    console.print("[bold yellow]Soil health data transformation completed successfully![/bold yellow]")

if __name__ == "__main__":
    main()