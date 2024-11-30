from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, StaleElementReferenceException
import os
import time
import logging
import sys
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
import sqlite3
import pandas as pd
import requests
from geopy.geocoders import Nominatim

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def wait_for_element(driver, by, value, timeout=20, condition="clickable"):
    """Wait for and return an element."""
    wait = WebDriverWait(driver, timeout)
    try:
        if condition == "clickable":
            element = wait.until(EC.element_to_be_clickable((by, value)))
        elif condition == "present":
            element = wait.until(EC.presence_of_element_located((by, value)))
        elif condition == "visible":
            element = wait.until(EC.visibility_of_element_located((by, value)))
        logger.info(f"Found element: {value}")
        return element
    except TimeoutException:
        logger.error(f"Timeout waiting for element: {value}")
        raise
    except StaleElementReferenceException:
        logger.error(f"Element became stale: {value}")
        raise

def calculate_jaccard_similarity(str1, str2):
    """Calculate Jaccard similarity between two strings."""
    set1 = set(str1.lower())
    set2 = set(str2.lower())
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union != 0 else 0

def select_from_mui_dropdown(driver, dropdown_selector, target_text, is_xpath=True):
    """Helper function to select an option from a MUI dropdown with fuzzy matching."""
    try:
        # Wait for and click the dropdown
        dropdown = wait_for_element(
            driver,
            By.XPATH if is_xpath else By.CSS_SELECTOR,
            dropdown_selector,
            condition="clickable"
        )
        
        # Try multiple methods to open the dropdown
        try:
            dropdown.click()
        except:
            try:
                driver.execute_script("arguments[0].click();", dropdown)
            except:
                try:
                    actions = ActionChains(driver)
                    actions.move_to_element(dropdown).click().perform()
                except:
                    dropdown.send_keys(Keys.SPACE)
        
        time.sleep(1)  # Wait for dropdown animation
        
        # Get all options and find best match
        options = driver.find_elements(By.XPATH, "//li[contains(@class, 'MuiMenuItem-root')]")
        best_match = None
        best_similarity = -1
        
        logger.info("Available options in dropdown:")
        for option in options:
            option_text = option.text.strip()
            similarity = calculate_jaccard_similarity(target_text, option_text)
            logger.info(f"- {option_text} (data-value: {option.get_attribute('data-value')}) [similarity: {similarity:.3f}]")
            
            if similarity > best_similarity:
                best_similarity = similarity
                best_match = option
        
        if best_match and best_similarity > 0.5:  # Threshold for minimum similarity
            logger.info(f"[bold yellow]Best match found: '{best_match.text}' with similarity {best_similarity:.3f}[/bold yellow]") 
            driver.execute_script("arguments[0].click();", best_match)
            return True
        else:
            logger.error(f"[bold red]No good match found for '{target_text}'. Best similarity was {best_similarity:.3f}[/bold red]")
            return False
            
    except Exception as e:
        logger.error(f"Failed to select from dropdown: {str(e)}")
        return False

def setup_driver(download_dir):
    """Set up and return the Chrome WebDriver with appropriate options."""
    options = Options()
    options.add_argument('--headless')  # Enable headless mode
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    
    # Configure download settings 
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "plugins.always_open_pdf_externally": True
    }
    options.add_experimental_option("prefs", prefs)
    
    # Specify Brave browser binary location
    brave_path = r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe"
    if not os.path.exists(brave_path):
        logger.error(f"Brave browser not found at: {brave_path}")
        raise FileNotFoundError(f"Brave browser not found at: {brave_path}")
    options.binary_location = brave_path
    
    # Set up ChromeDriver service
    chromedriver_path = r"C:\ChromeDriver\chromedriver.exe"
    if not os.path.exists(chromedriver_path):
        logger.error(f"ChromeDriver not found at: {chromedriver_path}")
        raise FileNotFoundError(f"ChromeDriver not found at: {chromedriver_path}")
    service = Service(executable_path=chromedriver_path)
    
    try:
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except WebDriverException as e:
        logger.error(f"Failed to initialize WebDriver: {str(e)}")
        raise

def create_databases():
    """Create separate SQLite databases for macro and micro nutrients"""
    try:
        # Create database directory if it doesn't exist
        db_path = os.path.join(os.getcwd(), 'database')
        if not os.path.exists(db_path):
            os.makedirs(db_path)
            logger.info("Created database directory")
        
        # Create macro nutrients database
        macro_conn = sqlite3.connect('database/macro_nutrients.db')
        macro_cursor = macro_conn.cursor()
        
        macro_cursor.execute('''
            CREATE TABLE IF NOT EXISTS macro_nutrients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                block TEXT,
                nitrogen_high REAL,
                nitrogen_medium REAL,
                nitrogen_low REAL,
                phosphorous_high REAL,
                phosphorous_medium REAL,
                phosphorous_low REAL,
                potassium_high REAL,
                potassium_medium REAL,
                potassium_low REAL,
                oc_high REAL,
                oc_medium REAL,
                oc_low REAL,
                ec_saline REAL,
                ec_non_saline REAL,
                ph_acidic REAL,
                ph_neutral REAL,
                ph_alkaline REAL,
                state TEXT,
                district TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        macro_conn.commit()
        
        # Create micro nutrients database
        micro_conn = sqlite3.connect('database/micro_nutrients.db')
        micro_cursor = micro_conn.cursor()
        
        micro_cursor.execute('''
            CREATE TABLE IF NOT EXISTS micro_nutrients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                block TEXT,
                copper_sufficient REAL,
                copper_deficient REAL,
                boron_sufficient REAL,
                boron_deficient REAL,
                sulphur_sufficient REAL,
                sulphur_deficient REAL,
                iron_sufficient REAL,
                iron_deficient REAL,
                zinc_sufficient REAL,
                zinc_deficient REAL,
                manganese_sufficient REAL,
                manganese_deficient REAL,
                state TEXT,
                district TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        micro_conn.commit()
        
        logger.info("Databases and tables created/verified successfully")
        return macro_conn, micro_conn
    except Exception as e:
        logger.error(f"Error creating databases: {str(e)}")
        raise

def store_csv_to_database(csv_path, state, district):
    """Store CSV data in appropriate SQLite database"""
    try:
        # Read CSV file
        df = pd.read_csv(csv_path)
        
        # Debug logging to check the data being processed
        logger.info(f"Processing file: {csv_path}")
        logger.info(f"DataFrame columns: {df.columns.tolist()}")
        
        # Determine if this is macro or micro data based on filename
        is_macro = "macro" in csv_path.lower()
        logger.info(f"Is macro data: {is_macro}")
        
        # Connect to appropriate database
        db_name = "macro_nutrients.db" if is_macro else "micro_nutrients.db"
        conn = sqlite3.connect(f'database/{db_name}')
        cursor = conn.cursor()
        
        # Debug logging for database connection
        logger.info(f"Connected to database: {db_name}")
        
        # Process each row in the DataFrame
        for index, row in df.iterrows():
            # Debug first row
            if index == 0:
                logger.info(f"Sample row data: {row.to_dict()}")
            
            # Remove the % symbol and convert to float
            values = [float(str(v).replace('%', '')) / 100 if isinstance(v, str) and '%' in str(v) else v 
                     for v in row.values[1:]]  # Skip the 'Block' column
            
            if is_macro:
                try:
                    cursor.execute('''
                        INSERT INTO macro_nutrients (
                            block, nitrogen_high, nitrogen_medium, nitrogen_low,
                            phosphorous_high, phosphorous_medium, phosphorous_low,
                            potassium_high, potassium_medium, potassium_low,
                            oc_high, oc_medium, oc_low,
                            ec_saline, ec_non_saline,
                            ph_acidic, ph_neutral, ph_alkaline,
                            state, district
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (row['Block'], *values, state, district))
                    if index == 0:
                        logger.info("Successfully inserted first macro row")
                except Exception as e:
                    logger.error(f"Error inserting macro row {index}: {str(e)}")
                    logger.error(f"Values being inserted: {[row['Block'], *values, state, district]}")
            else:
                try:
                    cursor.execute('''
                        INSERT INTO micro_nutrients (
                            block, copper_sufficient, copper_deficient,
                            boron_sufficient, boron_deficient,
                            sulphur_sufficient, sulphur_deficient,
                            iron_sufficient, iron_deficient,
                            zinc_sufficient, zinc_deficient,
                            manganese_sufficient, manganese_deficient,
                            state, district
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (row['Block'], *values, state, district))
                    if index == 0:
                        logger.info("Successfully inserted first micro row")
                except Exception as e:
                    logger.error(f"Error inserting micro row {index}: {str(e)}")
                    logger.error(f"Values being inserted: {[row['Block'], *values, state, district]}")
        
        conn.commit()
        conn.close()
        logger.info(f"Successfully stored data for {len(df)} blocks in {db_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error storing data in database: {str(e)}")
        return False

def download_soil_health_data(state="ANDHRA PRADESH", district="ANANTAPUR", download_dir=None):
    """Main function to download soil health data and store in database"""
    driver = None
    try:
        # Set up download directory
        if download_dir is None:
            download_dir = os.path.join(os.getcwd(), "temp_downloads")
        os.makedirs(download_dir, exist_ok=True)
        logger.info(f"Download directory set to: {download_dir}")
        
        # Clear existing files in download directory
        for file in os.listdir(download_dir):
            file_path = os.path.join(download_dir, file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                logger.warning(f"Error deleting file {file_path}: {str(e)}")
        
        logger.info("Initializing WebDriver...")
        driver = setup_driver(download_dir)
        
        logger.info("Navigating to website...")
        driver.get('https://soilhealth.dac.gov.in/piechart')
        time.sleep(5)  # Wait for page load
        
        # Process both MacroNutrient and MicroNutrient tabs
        tabs = [
            ("MacroNutrient(% View)", "macro"),
            ("MicroNutrient(% View)", "micro")
        ]
        
        for tab_name, data_type in tabs:
            logger.info(f"Processing {tab_name} tab...")
            
            # Click on the appropriate tab
            tab = wait_for_element(
                driver, 
                By.XPATH, 
                f"//button[contains(@class, 'MuiTab-root') and contains(text(), '{tab_name}')]"
            )
            driver.execute_script("arguments[0].click();", tab)
            logger.info(f"Clicked on {tab_name} tab")
            time.sleep(2)  # Wait for tab switch
            
            # Print available states before selection
            logger.info(f"Selecting state: {state}")
            state_xpath = "//div[contains(@class, 'MuiFormControl-root')]//div[contains(@class, 'MuiSelect-select') and contains(@class, 'MuiOutlinedInput-input') and contains(text(), 'Select a state')]"
         
            state_success = select_from_mui_dropdown(
                driver,
                state_xpath,
                state
            )
            if not state_success:
                raise Exception("Failed to select state")
            
            time.sleep(3)  # Wait for district dropdown
            
            # Select district
            logger.info(f"Selecting district: {district}")
            district_xpath = "//div[@class='MuiSelect-select MuiSelect-outlined MuiInputBase-input MuiOutlinedInput-input css-qiwgdb' and @role='combobox' and contains(text(), 'Select a district')]"
            district_success = select_from_mui_dropdown(
                driver,
                district_xpath,
                district
            )
            if not district_success:
                raise Exception("Failed to select district")
            
            time.sleep(3)  # Wait for data load
            
            # Click Export to CSV button
            logger.info("Clicking 'Export to CSV' button...")
            export_button = wait_for_element(
                driver, 
                By.CSS_SELECTOR,
                "a.downloadbtn[download='my-file.csv']"
            )
            
            # Get blob URL and click download
            blob_url = export_button.get_attribute('href')
            if not blob_url or not blob_url.startswith('blob:'):
                raise Exception("Invalid blob URL for download")
            
            driver.execute_script("arguments[0].click();", export_button)
            logger.info("Clicked 'Export to CSV' button")
            
            # Wait for CSV download
            expected_file = os.path.join(download_dir, "my-file.csv")
            timeout = 30
            while timeout > 0 and not os.path.exists(expected_file):
                time.sleep(1)
                timeout -= 1
                sys.stdout.write(f"\rWaiting for CSV download... {timeout} seconds remaining")
                sys.stdout.flush()
            
            if os.path.exists(expected_file):
                logger.info(f"\n{tab_name} CSV file downloaded successfully!")
                # Rename file to avoid overwriting
                new_file_name = os.path.join(download_dir, f"{data_type}_nutrients.csv")
                os.rename(expected_file, new_file_name)
                success = store_csv_to_database(new_file_name, state, district)
                
                if success:
                    os.remove(new_file_name)
                    logger.info(f"{tab_name} CSV file removed after database storage")
            else:
                logger.error("\nDownload timed out!")
                return False
            
            time.sleep(2)  # Wait before switching tabs
        
        return True
            
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return False
    finally:
        if driver:
            driver.quit()
            logger.info("Browser closed")

def get_location_by_ip():
    """Get latitude and longitude from IP address."""
    try:
        response = requests.get("https://ipinfo.io")
        data = response.json()
        location = data.get("loc", "Unknown")
        latitude, longitude = location.split(",") if location != "Unknown" else (None, None)
        logger.info(f"Retrieved location: {latitude}, {longitude}")
        return latitude, longitude
    except Exception as e:
        logger.error(f"Error getting location by IP: {str(e)}")
        return None, None

def get_location_details(lat, lon):
    """Get state and district from latitude and longitude."""
    try:
        geolocator = Nominatim(user_agent="IIA")
        location = geolocator.reverse(f"{lat},{lon}")
        
        # Convert state to uppercase and format district appropriately
        state = location.raw['address'].get('state', '').upper()
        district = location.raw['address'].get('state_district', '')
        
        # Format district: First letter of each word capitalized
        if district:
            district = district.title()
        
        logger.info(f"Retrieved state: {state}, district: {district}")
        return location.address, state, district
    except Exception as e:
        logger.error(f"Error getting location details: {str(e)}")
        return None, None, None

if __name__ == "__main__":
    try:
        # Create databases first
        macro_conn, micro_conn = create_databases()
        macro_conn.close()
        micro_conn.close()
        
        # Get location details
        # lat, lon = get_location_by_ip()
        lat=17.6868
        lon=83.2185
        
        if lat and lon:
            _, state, district = get_location_details(lat, lon)
        else:
            # Default fallback values
            state, district = "ANDHRA PRADESH", "ANANTAPUR"
            logger.warning("Using default location values")
        
        # Create a temporary download directory
        download_dir = os.path.join(os.getcwd(), "temp_downloads")
        success = download_soil_health_data(state=state, district=district, download_dir=download_dir)
        
        # Clean up temporary download directory
        if os.path.exists(download_dir):
            try:
                for file in os.listdir(download_dir):
                    file_path = os.path.join(download_dir, file)
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                os.rmdir(download_dir)
                logger.info("Cleaned up temporary download directory")
            except Exception as e:
                logger.warning(f"Error cleaning up download directory: {str(e)}")
        
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        sys.exit(1)
            