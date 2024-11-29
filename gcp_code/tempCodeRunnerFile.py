import os
import sys
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from dotenv import load_dotenv
import time

# Initialize Rich console
console = Console()

# Add this near the top of the file, after the imports
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def run_script(script_name, args=None):
    """Import and run a script by its filename and pass arguments if needed"""
    try:
        script_path = os.path.join(project_root, "ExtractAndLoad", script_name)
        script_name_without_extension = os.path.splitext(script_name)[0]
        
        # Dynamically import the script module
        sys.path.append(os.path.dirname(script_path))  # Add folder to sys.path
        script_module = __import__(script_name_without_extension)

        # Check if the script has a 'main' function
        if hasattr(script_module, 'main'):
            if args:
                script_module.main(*args)
            else:
                script_module.main()
        else:
            console.print(f"[red]{script_name} does not have a main() function.[/]")
            return False

        return True
        
    except Exception as e:
        console.print(f"[red]Error running {script_name}: {str(e)}[/]")
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
    """Load data from ExtractAndLoad folder"""
    extract_scripts = [
        ("weather_data.py", ["--days", "20"]),
        ("soilhealtdata.py", None)
    ]
    
    console.print("\n[bold yellow]Initializing data extraction processes...[/]")

    for script, args in extract_scripts:
        console.print(f"\n[cyan]Running {script}...[/]")

        success = run_script(script, args)
        if success:
            console.print(f"[green]Successfully completed {script}[/]")
        else:
            console.print(f"[red]Failed to complete {script}[/]")

def load_transform_data():
    """Load and transform all required data"""
    data_scripts = [
        "cropDataTranformation.py",
        "fertilizer_data.py",
        "Irrigated_and_crop_transformation.py",
        "soil_type.py",
        "soilData.py",
        "weatherTransformation.py"
    ]
    
    console.print("\n[bold yellow]Initializing data transformation processes...[/]")
    
    for script in data_scripts:
        console.print(f"\n[cyan]Running {script}...[/]")

        success = run_script(script)
        if success:
            console.print(f"[green]Successfully completed {script}[/]")
        else:
            console.print(f"[red]Failed to complete {script}[/]")

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
