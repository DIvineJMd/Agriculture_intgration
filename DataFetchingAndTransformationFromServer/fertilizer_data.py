import pandas as pd
import sqlite3
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from rich.console import Console

console = Console()

# Add the project root directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

# Import after adding to path
from gcp_code.federator import DatabaseFederator

# Load environment variables
load_dotenv()

# Server configurations using only environment variables
SERVERS = [

    {
        "host": os.getenv("FERTILIZER_PREDICTION_HOST"),
        "port": int(os.getenv("FERTILIZER_PREDICTION_PORT")),
        "db_name": "fertilizer_prediction"
    }
  
]

# Add validation to ensure all environment variables are set
def validate_server_config():
    """Validate that all required environment variables are set"""
    for server in SERVERS:
        if None in server.values():
            raise EnvironmentError(
                f"Missing environment variables for {server['db_name']}. "
                "Please check your .env file."
            )

# Validate configuration when module loads
validate_server_config()

def get_server_config(db_name):
    """Get server configuration by database name"""
    return next(
        (server for server in SERVERS if server["db_name"] == db_name),
        None
    )

def create_transformed_database():
    """Create and set up the transformed fertilizer database"""
    # Create WareHouse directory if it doesn't exist
    transform_dir = Path("WareHouse")
    transform_dir.mkdir(exist_ok=True)
    
    conn = sqlite3.connect(transform_dir / 'fertilizer_recommendation.db')
    cursor = conn.cursor()
    
    # Create transformed fertilizer recommendations table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fertilizer_recommendations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        soil_type TEXT,
        crop_type TEXT,
        n_ratio REAL,
        p_ratio REAL,
        k_ratio REAL,
        temperature_category TEXT,
        humidity_category TEXT,
        moisture_category TEXT,
        soil_condition_score REAL,
        recommended_fertilizer TEXT,
        application_rate REAL,
        effectiveness_score REAL
    )
    ''')
    
    # Create view for optimal conditions
    cursor.execute('''
    CREATE VIEW IF NOT EXISTS vw_optimal_conditions AS
    SELECT 
        soil_type,
        crop_type,
        AVG(n_ratio) as optimal_n,
        AVG(p_ratio) as optimal_p,
        AVG(k_ratio) as optimal_k,
        AVG(effectiveness_score) as avg_effectiveness
    FROM fertilizer_recommendations
    WHERE effectiveness_score >= 80
    GROUP BY soil_type, crop_type
    ''')
    
    conn.commit()
    return conn

def categorize_temperature(temp):
    if temp < 20:
        return 'Cool'
    elif temp < 30:
        return 'Moderate'
    else:
        return 'Warm'

def categorize_humidity(humidity):
    if humidity < 40:
        return 'Low'
    elif humidity < 70:
        return 'Medium'
    else:
        return 'High'

def categorize_moisture(moisture):
    if moisture < 30:
        return 'Dry'
    elif moisture < 60:
        return 'Medium'
    else:
        return 'Wet'

def calculate_soil_condition_score(row):
    """Calculate a soil condition score based on NPK values"""
    # Normalize each nutrient value to a 0-100 scale and take weighted average
    n_score = min(100, (row['Nitrogen'] / 140) * 100)
    p_score = min(100, (row['Phosphorous'] / 145) * 100)
    k_score = min(100, (row['Potassium'] / 205) * 100)
    
    return round((n_score * 0.4 + p_score * 0.3 + k_score * 0.3), 2)

def calculate_effectiveness_score(row):
    """Calculate effectiveness score based on multiple factors"""
    base_score = 70  # Base effectiveness score
    
    # Adjust based on soil condition
    if row['soil_condition_score'] >= 75:
        base_score += 15
    elif row['soil_condition_score'] >= 50:
        base_score += 10
    
    # Adjust based on temperature category
    if row['temperature_category'] == 'Moderate':
        base_score += 10
    
    # Adjust based on moisture
    if row['moisture_category'] == 'Medium':
        base_score += 5
    
    return min(100, base_score)  # Cap at 100

def transform_fertilizer_data():
    """Transform fertilizer data with enhanced metrics and recommendations"""
    # Find the fertilizer server from the imported servers list
    fertilizer_server = next(
        (server for server in SERVERS if server["db_name"] == "fertilizer_prediction"),
        None
    )
    
    if not fertilizer_server:
        print("Error: Fertilizer server configuration not found")
        return
    
    # Initialize federator
    federator = DatabaseFederator(SERVERS)
    
    # Fetch data using federator
    query = "SELECT * FROM fertilizer_data"
    data = federator.query_server(fertilizer_server, query)
    
    if data is None:
        console.print("[bold red]Failed to fetch data from the server[/bold red]")
        return
        
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Create transformed database connection
    target_conn = create_transformed_database()
    
    # Transform the data
    transformed_df = pd.DataFrame()
    transformed_df['soil_type'] = df['Soil_Type']
    transformed_df['crop_type'] = df['Crop_Type']
    
    # Updated column names to match the actual data
    transformed_df['n_ratio'] = df['Nitrogen'] / df[['Nitrogen', 'Phosphorous', 'Potassium']].sum(axis=1)
    transformed_df['p_ratio'] = df['Phosphorous'] / df[['Nitrogen', 'Phosphorous', 'Potassium']].sum(axis=1)
    transformed_df['k_ratio'] = df['Potassium'] / df[['Nitrogen', 'Phosphorous', 'Potassium']].sum(axis=1)
    transformed_df['temperature_category'] = df['Temparature'].apply(categorize_temperature)
    transformed_df['humidity_category'] = df['Humidity'].apply(categorize_humidity)
    transformed_df['moisture_category'] = df['Moisture'].apply(categorize_moisture)
    
    # Calculate scores
    transformed_df['soil_condition_score'] = df.apply(calculate_soil_condition_score, axis=1)
    transformed_df['recommended_fertilizer'] = df['Fertilizer_Name']
    
    # Calculate application rate
    transformed_df['application_rate'] = round(
        (df['Nitrogen'] + df['Phosphorous'] + df['Potassium']) / 3 * 0.1, 2
    )
    
    # Calculate effectiveness score
    transformed_df['effectiveness_score'] = transformed_df.apply(
        calculate_effectiveness_score, axis=1
    )
    
    # Save to transformed database
    transformed_df.to_sql(
        'fertilizer_recommendations', 
        target_conn, 
        if_exists='replace', 
        index=False
    )
    
    target_conn.close()
    console.print("[bold yellow]Fertilizer data transformation completed successfully![/bold yellow]")

def get_fertilizer_recommendations(conn, soil_type=None, crop_type=None):
    """Get fertilizer recommendations with optional filtering"""
    query = """
    SELECT 
        soil_type,
        crop_type,
        recommended_fertilizer,
        application_rate,
        soil_condition_score,
        effectiveness_score,
        temperature_category,
        moisture_category
    FROM fertilizer_recommendations
    WHERE effectiveness_score >= 70
    """
    
    if soil_type:
        query += f" AND soil_type = '{soil_type}'"
    if crop_type:
        query += f" AND crop_type = '{crop_type}'"
    
    query += " ORDER BY effectiveness_score DESC"
    
    return pd.read_sql_query(query, conn)

def main():
    transform_fertilizer_data()
    
    # Example: Get recommendations from transformed database
    conn = sqlite3.connect('WareHouse/fertilizer_recommendation.db')
    recommendations = get_fertilizer_recommendations(conn)
    # print("\nTop Fertilizer Recommendations:")
    # print(recommendations.head())
    conn.close()

if __name__ == "__main__":
    main()