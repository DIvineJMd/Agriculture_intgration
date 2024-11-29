import os
import sys
import subprocess
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from dotenv import load_dotenv
import time

# Initialize Rich console
console = Console()

# Add this near the top of the file, after the imports
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def run_script(script_path):
    """Run a script and handle its output in real-time"""
    try:
        # Set environment variables to disable Chrome logging
        env = os.environ.copy()
        env['PYTHONUNBUFFERED'] = '1'
        env['WDM_LOG_LEVEL'] = '0'
        env['WDM_PRINT_FIRST_LINE'] = 'False'

        # Run the script with modified environment
        process = subprocess.Popen(
            [sys.executable, "-u", script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            env=env
        )

        # Read output line by line
        while True:
            output = process.stdout.readline()
            if output:
                sys.stdout.write(output)
                sys.stdout.flush()
            
            error = process.stderr.readline()
            if error and "DeprecationWarning" not in error and "DevTools" not in error:
                sys.stderr.write(error)
                sys.stderr.flush()
            
            # Check if the process is complete
            if process.poll() is not None and not output and not error:
                break

        return process.returncode == 0

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
    """Load data from ExtractAndLoad folder"""
    extract_scripts = [
        ("weather_data.py", ["--days", "20"]),
        ("soilhealtdata.py", None)
    ]
    
    console.print("\n[bold yellow]Initializing data extraction processes...[/]")
    
    for script, args in extract_scripts:
        script_path = os.path.join(project_root, "ExtractAndLoad", script)
        console.print(f"\n[cyan]Running {script}...[/]")
        
        success = run_script(script_path, args)
        if success:
            console.print(f"[green]Successfully completed {script}[/]")
        else:
            console.print(f"[red]Failed to complete {script}[/]")

def load_transform_data():
    """Load and transform all required data"""
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
