import sqlite3
import pandas as pd
import numpy as np
import os
from rich.console import Console

console = Console()

class CropRecommendationSystem:
    def __init__(self):
        """Initialize the crop recommendation system"""
        self.db_path = "WareHouse"
        self.crop_requirements = self._load_crop_requirements()
    def _load_crop_requirements(self):
        """Load crop requirements from the database"""
        try:
            # Connect to the transformed database
            conn = sqlite3.connect(f'{self.db_path}/transformed_crop_data.db')
            
            # Get data from transformed_crop_data table
            query = "SELECT * FROM transformed_crop_data"
            crop_data = pd.read_sql_query(query, conn)
            conn.close()
            
            if crop_data.empty:
                print("Warning: No crop data found in database")
                return {}
            
            # Group by label to get ranges
            requirements = {}
            for label in crop_data['label'].unique():
                crop_subset = crop_data[crop_data['label'] == label]
                
                # Calculate mean values for each nutrient
                n_mean = crop_subset['N'].mean()
                p_mean = crop_subset['P'].mean()
                k_mean = crop_subset['K'].mean()
                rainfall_mean = crop_subset['rainfall'].mean()
                ph_mean = crop_subset['ph'].mean()
                
                # Calculate ranges (±20% from mean)
                requirements[label] = {
                    'water_need': float(rainfall_mean),
                    'preferred_irrigation': self._get_default_irrigation(float(rainfall_mean)),
                    'N': (float(n_mean * 0.8), float(n_mean * 1.2)),
                    'P': (float(p_mean * 0.8), float(p_mean * 1.2)),
                    'K': (float(k_mean * 0.8), float(k_mean * 1.2)),
                    'pH_category': self._get_ph_categories(float(ph_mean)),
                    'seasons': self._get_default_seasons(label)
                }
            
            return requirements
            
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            return {}
        except Exception as e:
            print(f"Error loading crop requirements: {e}")
            return {}
    def _get_ph_categories(self, ph_value):
        """Determine pH categories based on pH value"""
        try:
            # Convert ph_value to float
            ph_value = float(ph_value)
            
            if ph_value < 5.5:
                return ['Strongly Acidic', 'Moderately Acidic']
            elif ph_value < 6.5:
                return ['Slightly Acidic', 'Neutral']
            elif ph_value < 7.5:
                return ['Neutral']
            elif ph_value < 8.5:
                return ['Neutral', 'Slightly Alkaline']
            else:
                return ['Slightly Alkaline', 'Moderately Alkaline']
        except (ValueError, TypeError):
            print(f"Warning: Invalid pH value: {ph_value}, defaulting to Neutral")
            return ['Neutral']
    def _get_default_irrigation(self, water_need):
        """Determine default irrigation method based on water needs"""
        if water_need > 800:
            return 'Flood'
        elif water_need > 500:
            return 'Sprinkler'
        else:
            return 'Drip'
    def _get_default_seasons(self, crop):
        """Default season mapping based on crop type."""
        # Defining crops for different seasons
        kharif_crops = [
            'rice', 'maize', 'cotton', 'sugarcane', 'sorghum', 
            'bajra', 'soybean', 'groundnut', 'pulses', 'millet'
        ]
        rabi_crops = [
            'wheat', 'chickpea', 'mustard', 'barley', 'peas', 
            'linseed', 'oats', 'lentils', 'gram', 'sunflower'
        ]
        zaid_crops = [
            'watermelon', 'muskmelon', 'cucumber', 'pumpkin', 
            'fodder', 'summer maize', 'cowpea', 'okra', 'tomato'
        ]
        # Convert crop to lowercase for case-insensitive comparison
        crop = crop.lower()
        
        # Check which seasons the crop belongs to
        seasons = []
        if crop in kharif_crops:
            seasons.append('Kharif')
        if crop in rabi_crops:
            seasons.append('Rabi')
        if crop in zaid_crops:
            seasons.append('Zaid')
        # If no specific season, assume it can grow in all major seasons
        return seasons if seasons else ['Kharif', 'Rabi', 'Zaid']
    def get_soil_health_data(self, state):
        """Get aggregated soil health data for the state"""
        try:
            conn = sqlite3.connect(f'{self.db_path}/soil_health_transformed.db')
            query = """
            SELECT 
                AVG(nitrogen_level) as nitrogen_level,
                AVG(phosphorous_level) as phosphorous_level,
                AVG(potassium_level) as potassium_level,
                AVG(ph_level) as ph_level,
                AVG(organic_carbon_level) as organic_carbon_level,
                AVG(micro_score) as micro_score,
                AVG(overall_soil_health_score) as overall_soil_health_score,
                state,
                GROUP_CONCAT(DISTINCT district) as districts
            FROM soil_health
            WHERE state = ?
            GROUP BY state
            """
            soil_data = pd.read_sql_query(query, conn, params=[state])
            conn.close()
            return soil_data.iloc[0] if not soil_data.empty else None
        except Exception as e:
            print(f"Error fetching soil health data: {e}")
            return None
    def get_irrigation_data(self, location, soil_data):
        """Get irrigation availability data"""
        try:
            conn = sqlite3.connect(f'{self.db_path}/irrigation_transformed.db')
            query = """
            SELECT 
                total_irrigated_area,
                irrigation_coverage_ratio,
                water_source_diversity_score,
                canal_percentage,
                tank_percentage,
                tubewell_percentage
            FROM transformed_irrigation
            WHERE state = ?
            """
            irrigation_data = pd.read_sql_query(query, conn, params=[soil_data['state']])
            conn.close()
            return irrigation_data.iloc[0] if not irrigation_data.empty else None
        except Exception as e:
            print(f"Error fetching irrigation data: {e}")
            return None
    def calculate_soil_suitability(self, soil_data, crop):
        """Calculate soil suitability score for a crop"""
        if crop not in self.crop_requirements:
            return 0.5

        requirements = self.crop_requirements[crop]
        
        try:
            # Convert percentage values to appropriate ranges
            # NPK values in soil_data are in percentage (0-100)
            n_value = float(soil_data['nitrogen_level'])
            p_value = float(soil_data['phosphorous_level'])
            k_value = float(soil_data['potassium_level'])
            ph_value = float(soil_data['ph_level'])
            
            # Calculate NPK scores with wider acceptable ranges
            n_score = self._calculate_range_score(n_value, requirements['N'], tolerance=0.5)
            p_score = self._calculate_range_score(p_value, requirements['P'], tolerance=0.5)
            k_score = self._calculate_range_score(k_value, requirements['K'], tolerance=0.5)
            
            # Calculate pH score
            ph_categories = self._get_ph_categories(ph_value)
            ph_score = self._calculate_ph_score(ph_categories[0], requirements['pH_category'])
            
            # Weight the scores (NPK: 90%, pH: 10%)
            final_score = (n_score * 0.3 + p_score * 0.3 + k_score * 0.3 + ph_score * 0.1)
            
            return final_score
            
        except (ValueError, TypeError, KeyError) as e:
            print(f"Warning: Error calculating soil suitability: {e}")
            return 0.5
    def calculate_irrigation_suitability(self, irrigation_data, crop):
        """Calculate irrigation suitability score"""
        if irrigation_data is None:
            return 0.5
        requirements = self.crop_requirements[crop]
        water_need = requirements['water_need']
        
        # Calculate water availability score
        coverage_score = min(1.0, irrigation_data['irrigation_coverage_ratio'] / 100)
        
        # Calculate water source suitability
        preferred = requirements['preferred_irrigation']
        source_scores = {
            'Flood': irrigation_data['canal_percentage'] + irrigation_data['tank_percentage'],
            'Sprinkler': irrigation_data['tubewell_percentage'],
            'Drip': irrigation_data['tubewell_percentage']
        }
        source_score = min(1.0, source_scores[preferred] / 100)
        
        # Combined irrigation score
        irrigation_score = (coverage_score * 0.6 + source_score * 0.4)
        
        return irrigation_score
    def _calculate_range_score(self, value, optimal_range, tolerance=0.5):
        """Calculate score based on optimal range with tolerance"""
        min_val, max_val = optimal_range
        range_width = max_val - min_val
        
        # Extend the acceptable range by the tolerance factor
        extended_min = min_val - (range_width * tolerance)
        extended_max = max_val + (range_width * tolerance)
        
        if extended_min <= value <= extended_max:
            if min_val <= value <= max_val:
                return 1.0
            else:
                # Calculate partial score based on distance from optimal range
                distance = min(abs(value - min_val), abs(value - max_val))
                return max(0.5, 1 - (distance / (range_width * tolerance)))
        return max(0.2, 1 - (min(abs(value - extended_min), abs(value - extended_max)) / (range_width * 2)))
    def _calculate_ph_score(self, ph_category, optimal_categories):
        """Calculate pH score based on categorical values"""
        ph_scores = {
            'Strongly Acidic': 1,
            'Moderately Acidic': 2,
            'Slightly Acidic': 3,
            'Neutral': 4,
            'Slightly Alkaline': 3,
            'Moderately Alkaline': 2,
            'Strongly Alkaline': 1
        }
        
        current_score = ph_scores.get(ph_category, 0)
        optimal_scores = [ph_scores.get(cat, 0) for cat in optimal_categories]
        
        if ph_category in optimal_categories:
            return 1.0
        
        min_distance = min(abs(current_score - opt_score) for opt_score in optimal_scores)
        return max(0, 1 - (min_distance / 3))
    def get_crop_recommendations(self, season=None):
        """Get comprehensive crop recommendations"""
        try:
            # Connect to get soil health data
            conn = sqlite3.connect(f'{self.db_path}/soil_health_transformed.db')
            query = """
            SELECT 
                AVG(nitrogen_level) as nitrogen_level,
                AVG(phosphorous_level) as phosphorous_level,
                AVG(potassium_level) as potassium_level,
                AVG(ph_level) as ph_level,
                AVG(overall_soil_health_score) as overall_soil_health_score,
                state,
                GROUP_CONCAT(DISTINCT district) as districts
            FROM soil_health
            GROUP BY state
            """
            soil_data = pd.read_sql_query(query, conn)
            conn.close()
            
            if soil_data.empty:
                return {"error": "No soil health data found"}
            
            soil_data = soil_data.iloc[0]  # Get first state's data
            
            recommendations = {
                'highly_suitable_crops': [],
                'moderately_suitable_crops': [],
                'location_details': {
                    'state': soil_data['state'],
                    'districts': soil_data['districts'],
                    'soil_health_score': soil_data['overall_soil_health_score'],
                    'npk_levels': f"N:{soil_data['nitrogen_level']:.1f}, "
                                f"P:{soil_data['phosphorous_level']:.1f}, "
                                f"K:{soil_data['potassium_level']:.1f}"
                }
            }
            
            for crop in self.crop_requirements:
                if season and season not in self._get_default_seasons(crop):
                    continue
                    
                soil_score = self.calculate_soil_suitability(soil_data, crop)
                water_req = self.crop_requirements[crop]['water_need']
                
                # Simplified irrigation score based on water requirement
                irrigation_score = 0.7  # Default moderate score
                
                final_score = (soil_score * 0.7 + irrigation_score * 0.3)
                
                crop_info = {
                    'crop': crop,
                    'final_score': final_score,
                    'details': {
                        'soil_score': soil_score,
                        'irrigation_score': irrigation_score,
                        'water_requirement': water_req,
                        'suitable_seasons': self._get_default_seasons(crop),
                        'preferred_irrigation': self._get_default_irrigation(water_req)
                    }
                }
                
                # Adjusted thresholds for recommendations
                if final_score >= 0.4:  # Further lowered threshold
                    recommendations['highly_suitable_crops'].append(crop_info)
                elif final_score >= 0.2:  # Further lowered threshold
                    recommendations['moderately_suitable_crops'].append(crop_info)
            
            # Sort recommendations by final score
            for category in ['highly_suitable_crops', 'moderately_suitable_crops']:
                recommendations[category].sort(key=lambda x: x['final_score'], reverse=True)
            
            return recommendations
            
        except Exception as e:
            print(f"Error getting recommendations: {e}")
            return {"error": str(e)}
    def _load_crop_data(self):
        """Load crop data from the database"""
        try:
            conn = sqlite3.connect(f'{self.db_path}/transformed_crop_data.db')
            query = "SELECT * FROM crop_data"
            crop_data = pd.read_sql_query(query, conn)
            conn.close()
            return crop_data
        except Exception as e:
            print(f"Error loading crop data: {e}")
            return pd.DataFrame()
    def get_location(self):
        """Get and validate location input from user"""
        try:
            # Connect to the soil health database to get available states
            conn = sqlite3.connect(f'{self.db_path}/soil_health_transformed.db')
            cursor = conn.cursor()
            
            # Get all unique states
            cursor.execute("SELECT DISTINCT state FROM soil_health ORDER BY state")
            states = cursor.fetchall()
            conn.close()
            
            if not states:
                print("No states found in database")
                return None
            
            # Print available states
            print("\nAvailable States:")
            for i, (state,) in enumerate(states, 1):
                print(f"{i}. {state}")
            
            # Get user input
            while True:
                try:
                    choice = int(input("\nEnter the number of your state (or 0 to exit): "))
                    if choice == 0:
                        return None
                    if 1 <= choice <= len(states):
                        return states[choice-1][0]  # Return the state name
                    print("Invalid choice. Please try again.")
                except ValueError:
                    print("Please enter a valid number.")
                
        except Exception as e:
            print(f"Error getting states: {e}")
            return None
def main():
    # Initialize the system
    recommender = CropRecommendationSystem()
    
    # Get season input
    seasons = ['Kharif', 'Rabi', 'Zaid']
    print("\nSeasons:")
    for i, season in enumerate(seasons, 1):
        print(f"{i}. {season}")
    print("4. All seasons")
    
    while True:
        try:
            season_choice = int(input("\nEnter the number for season (or 0 to exit): "))
            if season_choice == 0:
                return
            if season_choice == 4:
                season = None
                break
            if 1 <= season_choice <= len(seasons):
                season = seasons[season_choice-1]
                break
            print("Invalid choice. Please try again.")
        except ValueError:
            print("Please enter a valid number.")
    
    # Get recommendations
    recommendations = recommender.get_crop_recommendations(season)
    if "error" in recommendations:
        print(f"Error: {recommendations['error']}")
        return
    
    # Print recommendations
    state = recommendations['location_details']['state']
    print(f"\nCrop Recommendations for {state}")
    print(f"\nLocation Details:")
    for key, value in recommendations['location_details'].items():
        print(f"{key.replace('_', ' ').title()}: {value}")
    
    console.print("\n[bold green]Highly Suitable Crops:[/bold green]")
    if recommendations['highly_suitable_crops']:
        for crop in recommendations['highly_suitable_crops']:
            print(f"\n{crop['crop']}:")
            print(f"Overall Suitability: {crop['final_score']:.2f}")
            print(f"Soil Score: {crop['details']['soil_score']:.2f}")
            print(f"Irrigation Score: {crop['details']['irrigation_score']:.2f}")
            print(f"Water Requirement: {crop['details']['water_requirement']} mm")
            print(f"Suitable Seasons: {', '.join(crop['details']['suitable_seasons'])}")
            print(f"Preferred Irrigation: {crop['details']['preferred_irrigation']}")
    else:
        print("No highly suitable crops found for this season")
    
    console.print("\n[bold yellow]Moderately Suitable Crops:[/bold yellow]")
    if recommendations['moderately_suitable_crops']:
        for crop in recommendations['moderately_suitable_crops']:
            print(f"\n{crop['crop']}:")
            print(f"Overall Suitability: {crop['final_score']:.2f}")
            print(f"Soil Score: {crop['details']['soil_score']:.2f}")
            print(f"Irrigation Score: {crop['details']['irrigation_score']:.2f}")
            print(f"Water Requirement: {crop['details']['water_requirement']} mm")
            print(f"Suitable Seasons: {', '.join(crop['details']['suitable_seasons'])}")
            print(f"Preferred Irrigation: {crop['details']['preferred_irrigation']}")
    else:
        console.print("[bold red]No moderately suitable crops found for this location and season[/bold red]")

        print("No moderately suitable crops found for this season")
if __name__ == "__main__":
    main()