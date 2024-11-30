import pandas as pd
import sqlite3
import os
import sys
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
        "host": os.getenv("SOIL_TYPES_HOST"),
        "port": int(os.getenv("SOIL_TYPES_PORT")),
        "db_name": "soil_types"
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

def fetch_and_transform_soil_data():
    """Fetch and transform soil types data"""
    try:
        # Get soil types server config
        soil_server = next(
            (server for server in SERVERS if server["db_name"] == "soil_types"),
            None
        )
        
        if not soil_server:
            print("Error: Soil types server configuration not found")
            return
        
        # Initialize federator
        federator = DatabaseFederator(SERVERS)
        
        # Fetch soil types data
        query = "SELECT * FROM soil_types"
        data = federator.query_server(soil_server, query)
        
        if data is None:
            print("[bold red]Failed to fetch soil types data from the server[bold red]")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Fetch state associations
        query_states = "SELECT * FROM soil_type_states"
        states_data = federator.query_server(soil_server, query_states)
        
        if states_data is None:
            print("Failed to fetch soil type states data from the server")
            return
            
        states_df = pd.DataFrame(states_data)
        
        # Create WareHouse directory if it doesn't exist
        os.makedirs('WareHouse', exist_ok=True)
        
        # Connect to database
        conn = sqlite3.connect('WareHouse/soil_types.db')
        cursor = conn.cursor()
        
        # Create soil_types table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS soil_types (
            id INTEGER PRIMARY KEY,
            soil_type TEXT NOT NULL,
            facts TEXT
        )
        ''')
        
        # Create soil_type_states table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS soil_type_states (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soil_type_id INTEGER,
            state TEXT NOT NULL,
            FOREIGN KEY (soil_type_id) REFERENCES soil_types (id),
            UNIQUE (soil_type_id, state)
        )
        ''')
        
        # Insert soil types data
        df.to_sql('soil_types', conn, if_exists='replace', index=False)
        
        # Insert states data if available
        if not states_df.empty:
            states_df.to_sql('soil_type_states', conn, if_exists='replace', index=False)
        
        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_soil_type_states_state ON soil_type_states(state)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_soil_type_states_soil_type_id ON soil_type_states(soil_type_id)')
        
        print("[bold yellow]Soil types database extracted successfully![bold yellow]")
        
        # Verify the data
        cursor.execute("SELECT COUNT(*) FROM soil_types")
        soil_types_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM soil_type_states")
        states_count = cursor.fetchone()[0]
        
        print(f"Inserted {soil_types_count} soil types and {states_count} state associations")
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"Error transforming soil types data: {str(e)}")
        raise  # Re-raise the exception to see the full traceback

def query_soil_types_by_state(state):
    """Helper function to query soil types for a specific state"""
    try:
        conn = sqlite3.connect('WareHouse/soil_types.db')
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT st.soil_type, st.facts 
        FROM soil_types st
        JOIN soil_type_states sts ON st.id = sts.soil_type_id
        WHERE sts.state = ?
        ''', (state,))
        
        results = cursor.fetchall()
        conn.close()
        
        return results
    
    except Exception as e:
        print(f"Error querying soil types: {str(e)}")
        return []

def main():
    # Validate environment variables
    validate_server_config()
    
    # Fetch and transform data
    fetch_and_transform_soil_data()
    
    # # Example query to verify the data
    # print("\nExample Query - Soil Types in Maharashtra:")
    # results = query_soil_types_by_state("Maharashtra")
    # for soil_type, facts in results:
    #     print(f"\nSoil Type: {soil_type}")
    #     print(f"Facts: {facts}")

if __name__ == "__main__":
    main()