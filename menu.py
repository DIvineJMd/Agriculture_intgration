import mysql.connector
from mysql.connector import errors
import time
from colorama import Fore, Style, init
import sys
from rich.console import Console
from rich.table import Table
import pandas as pd
import sqlite3
import os

# Create a console object
console = Console()

def transform_weather_data(current_data, hourly_data, daily_data):
    # ... existing code ...
    console.print("[bold green]Transformation of Weather Data Completed.[/bold green]")

def plot_temperature_trends(daily_data, location_id, save_path="temperature_plot.png"):
    # ... existing code ...
    console.print(f"[bold blue]Temperature plot saved as {save_path}[/bold blue]")

def transform_irrigated_area_data(df):
    # ... existing code ...
    console.print("[bold green]Transformation for irrigated area complete.[/bold green]")

def transform_soil_nutrient_levels(macro_df, micro_df):
    # ... existing code ...
    console.print("[bold green]Transformation for Soil Data complete.[/bold green]")

def transform_crop_price(df):
    # ... existing code ...
    console.print("[bold green]Transformation for Crop Prices complete.[/bold green]")

def disp_table(folder_name, db_file):
    db_path = os.path.join(folder_name, db_file)
    console.print(f"[bold magenta]\\nProcessing database: {db_file}[/bold magenta]")

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]

        for table_name in table_names:
            console.print(f"[bold cyan]\\nTable: {table_name}[/bold cyan]")
            try:
                df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 5", conn)
                table = Table(show_header=True, header_style="bold magenta")
                for column in df.columns:
                    table.add_column(column)
                for row in df.itertuples(index=False):
                    table.add_row(*map(str, row))
                console.print(table)
            except Exception as e:
                console.print(f"[bold red]Error reading table {table_name}: {e}[/bold red]")

# Example usage
# Assuming you have the data loaded as DataFrames
# transform_weather_data(current_weather_data, houry_weather_data, daily_weather_data)
# plot_temperature_trends(daily_weather_data, location_id=1, save_path="temperature_plot.png")
# transform_crop_price(init_price_data)
# transform_soil_nutrient_levels(macro_soil_data, micro_soil_data)
# transform_irrigated_area_data(init_irrigated_area_data)

# Display tables
# dbs = ['Irrigated_Area_and_Crop_Price.db', 'soil_health.db', 'weather_data.db']
# disp_table("Transformed_database", dbs[0])
# disp_table("Transformed_database", dbs[1])
# disp_table("Transformed_database", dbs[2])

