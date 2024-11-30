import sqlite3
import pandas as pd
import os
import sys
from dotenv import load_dotenv
    
# Add the project root directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from gcp_code.federator import DatabaseFederator

# Load environment variables
load_dotenv()

# Server configurations using only environment variables
SERVERS = [
    {
        "host": os.getenv("CROP_DATA_HOST"),
        "port": int(os.getenv("CROP_DATA_PORT")),
        "db_name": "crop_data"
    }
]

def fetch_and_transform_data():
    """Fetch and transform crop data"""
    try:
        # Find the crop_data server from the imported servers list
        crop_data_server = next(
            (server for server in SERVERS if server["db_name"] == "crop_data"),
            None
        )
        
        if not crop_data_server:
            print("Error: Crop data server configuration not found")
            return
        
        # Initialize federator with all servers
        federator = DatabaseFederator(SERVERS)
        
        # Create transformed database directory if it doesn't exist
        os.makedirs('WareHouse', exist_ok=True)
        
        # Fetch data using federator
        query = "SELECT * FROM crop_data"
        data = federator.query_server(crop_data_server, query)
        
        if data is not None:
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Connect to the transformed database
            transformed_db = 'WareHouse/transformed_crop_data.db'
            conn = sqlite3.connect(transformed_db)
            
            # Create table and save transformed data
            conn.execute('''
            CREATE TABLE IF NOT EXISTS transformed_crop_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nitrogen FLOAT,
                phosphorus FLOAT,
                potassium FLOAT,
                temperature FLOAT,
                humidity FLOAT,
                ph FLOAT,
                rainfall FLOAT,
                label TEXT
            )
            ''')
            
            # Save to database
            df.to_sql('transformed_crop_data', conn, if_exists='replace', index=False)
            
            # Commit and close
            conn.commit()
            conn.close()
            print("[bold yellow]Data succesfully transformed.[bold yellow]")
        else:
            print("[bold red]Failed to fetch data from the server.[/bold red]")
            
    except Exception as e:
        print(f"Error in crop data transformation: {str(e)}")

if __name__ == "__main__":
    fetch_and_transform_data()
