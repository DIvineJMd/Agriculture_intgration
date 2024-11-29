import os
import sys
import subprocess
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from dotenv import load_dotenv
import time
import importlib.util

# Initialize Rich console
console = Console()

# Add this near the top of the file, after the imports
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def run_script(script_path, args=None):
    """Run a script using exec() instead of subprocess"""
    try:
        # Load the script as a module
        spec = importlib.util.spec_from_file_location("module", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return True
    except Exception as e:
        console.print(f"[red]Error running script: {str(e)}[/]")
        return False

def display_welcome():
    """Display welcome message"""
    welcome_text = Text()
    welcome_text.append("Welcome to the ", style="green")
    welcome_text.append("Agricultural Advisor System", style="bold green")
    welcome_text.append("!\n\n", style="green")
    welcome_text.append("Your intelligent companion for agricultural decisions.", style="yellow")
    
    console.print(Panel(welcome_text, border_style="green"))

def load_extract_data():
    """Load data from ExtractAndLoad folder sequentially"""
    extract_scripts = [
        ("weather_data.py", ["--days", "20"]),
        ("soilhealtdata.py", None)
    ]
    
    console.print("\n[bold yellow]Initializing data extraction processes...[/]")
    
    for script, args in extract_scripts:
        script_path = os.path.join(project_root, "ExtractAndLoad", script)
        console.print(f"\n[cyan]Running {script}...[/]")
        
        # Note: args are not used with exec() implementation
        success = run_script(script_path)
        if success:
            console.print(f"[green]Successfully completed {script}[/]")
        else:
            console.print(f"[red]Failed to complete {script}[/]")
            return  # Stop if any script fails

def load_transform_data():
    """Load and transform all required data sequentially"""
    data_scripts = [
        "cropDataTranformation.py",
        "fertilizer_data.py",
        "Irrigated and crop transformation.py",
        "soil_type.py",
        "soilData.py",
        "weatherTransformation.py"
    ]
    
    console.print("\n[bold yellow]Initializing data transformation processes...[/]")
    
    for script in data_scripts:
        script_path = os.path.join(project_root, "DataFetchingAndTransformationFromServer", script)
        console.print(f"\n[cyan]Running {script}...[/]")
        
        success = run_script(script_path)
        if success:
            console.print(f"[green]Successfully completed {script}[/]")
        else:
            console.print(f"[red]Failed to complete {script}[/]")
            return  # Stop if any script fails

def main():
    # Load environment variables
    load_dotenv()
    
    # Clear screen
    os.system('cls' if os.name == 'nt' else 'clear')
    
    # Display welcome message
    display_welcome()
    
    # First run the extraction scripts
    load_extract_data()
    
    # Then run the transformation scripts
    load_transform_data()
    
    console.print("\n[bold green]Data extraction and transformation completed![/]")
    console.print("[yellow]Press Enter to exit...[/]")
    input()

if __name__ == "__main__":
    main()
