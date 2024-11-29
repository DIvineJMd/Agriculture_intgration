import sqlite3
import pandas as pd
import numpy as np

class CropRecommendationSystem:
    def __init__(self):
        """Initialize the crop recommendation system"""
        self.db_path = "WareHouse"
        self.crop_requirements = self._load_crop_requirements()

    def _load_crop_requirements(self):
        """Load crop requirements from the database"""
        try:
            conn = sqlite3.connect(f'{self.db_path}/transformed_crop_data.db')
            # Print the table schema to debug
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(crop_data)")
            columns = [col[1] for col in cursor.fetchall()]
            print("Available columns:", columns)

            crop_data = pd.read_sql_query("SELECT * FROM crop_data", conn)
            conn.close()

            # Group by label to get ranges
            requirements = {}
            for label in crop_data['label'].unique():
                crop_subset = crop_data[crop_data['label'] == label]
                
                # Calculate ranges using mean ± standard deviation
                n_mean, n_std = crop_subset['N'].mean(), crop_subset['N'].std()
                p_mean, p_std = crop_subset['P'].mean(), crop_subset['P'].std()
                k_mean, k_std = crop_subset['K'].mean(), crop_subset['K'].std()
                
                requirements[label] = {
                    'water_need': crop_subset['rainfall'].mean(),
                    'preferred_irrigation': self._get_default_irrigation(crop_subset['rainfall'].mean()),
                    'N': (max(0, n_mean - n_std), n_mean + n_std),
                    'P': (max(0, p_mean - p_std), p_mean + p_std),
                    'K': (max(0, k_mean - k_std), k_mean + k_std),
                    'pH_category': self._get_ph_categories(crop_subset['ph'].mean()),
                    'seasons': self._get_default_seasons(label)
                }
            return requirements
        except Exception as e:
            print(f"Error loading crop requirements: {e}")
            print("DataFrame columns:", crop_data.columns.tolist())  # Debug info
            return {}

    def _get_ph_categories(self, ph_value):
        """Determine pH categories based on pH value"""
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


    def get_soil_health_data(self, location):
        """Get soil health data for the location"""
        try:
            conn = sqlite3.connect(f'{self.db_path}/soil_health_transformed.db')
            query = """
            SELECT 
                nitrogen_level,
                phosphorous_level,
                potassium_level,
                ph_level,
                organic_carbon_level,
                micro_score,
                overall_soil_health_score,
                state,
                district
            FROM soil_health
            WHERE block = ?
            """
            soil_data = pd.read_sql_query(query, conn, params=[location])
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
        
        # Normalize the soil values to match the scale of crop requirements
        n_value = soil_data['nitrogen_level'] * 20  # Convert from percentage to ppm
        p_value = soil_data['phosphorous_level']
        k_value = soil_data['potassium_level']
        
        # Calculate NPK scores with wider acceptable ranges
        n_score = self._calculate_range_score(n_value, requirements['N'], tolerance=0.5)
        p_score = self._calculate_range_score(p_value, requirements['P'], tolerance=0.5)
        k_score = self._calculate_range_score(k_value, requirements['K'], tolerance=0.5)
        
        # Calculate pH score
        ph_score = self._calculate_ph_score(soil_data['ph_level'], requirements['pH_category'])
        
        # Weight the scores (NPK: 90%, pH: 10%)
        final_score = (n_score * 0.3 + p_score * 0.3 + k_score * 0.3 + ph_score * 0.1)
        
        return final_score

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

    def _calculate_range_score(self, value, optimal_range, tolerance=0.3):
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

    def get_crop_recommendations(self, location, season=None):
        """Get comprehensive crop recommendations"""
        soil_data = self.get_soil_health_data(location)
        if soil_data is None:
            return {"error": f"No soil health data found for location: {location}"}
        
        irrigation_data = self.get_irrigation_data(location, soil_data)
        
        # Load crop data from the database
        crop_data = self._load_crop_data()
        
        # Use sets to track unique crops
        seen_crops = set()
        
        recommendations = {
            'highly_suitable_crops': [],
            'moderately_suitable_crops': [],
            'location_details': {
                'block': location,
                'district': soil_data['district'],
                'state': soil_data['state'],
                'soil_health_score': soil_data['overall_soil_health_score'],
                'npk_levels': f"N:{soil_data['nitrogen_level']:.1f}, "
                            f"P:{soil_data['phosphorous_level']:.1f}, "
                            f"K:{soil_data['potassium_level']:.1f}"
            }
        }
        
        for _, crop in crop_data.iterrows():
            # Skip if we've already processed this crop
            if crop['label'] in seen_crops:
                continue
            seen_crops.add(crop['label'])
            
            if season and season not in self._get_default_seasons(crop['label']):
                continue
                
            soil_score = self.calculate_soil_suitability(soil_data, crop['label'])
            irrigation_score = self.calculate_irrigation_suitability(irrigation_data, crop['label'])
            
            final_score = (soil_score * 0.6 + irrigation_score * 0.4)
            
            crop_info = {
                'crop': crop['label'],
                'final_score': final_score,
                'details': {
                    'soil_score': soil_score,
                    'irrigation_score': irrigation_score,
                    'water_requirement': crop['rainfall'],
                    'suitable_seasons': self._get_default_seasons(crop['label']),
                    'preferred_irrigation': self._get_default_irrigation(crop['rainfall'])
                }
            }
            
            if final_score >= 0.6:
                recommendations['highly_suitable_crops'].append(crop_info)
            elif final_score >= 0.4:
                recommendations['moderately_suitable_crops'].append(crop_info)
        
        # Sort recommendations by final score
        for category in ['highly_suitable_crops', 'moderately_suitable_crops']:
            recommendations[category].sort(key=lambda x: x['final_score'], reverse=True)
        
        return recommendations

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

def main():
    # Example usage
    recommender = CropRecommendationSystem()
    location = "ATMAKUR"
    season = "Kharif"  # Optional: Can be None, "Kharif", or "Rabi"
    
    recommendations = recommender.get_crop_recommendations(location, season)
    if "error" in recommendations:
        print(f"Error: {recommendations['error']}")
        return
    
    # Print recommendations
    print(f"\nCrop Recommendations for {location}")
    print(f"\nLocation Details:")
    for key, value in recommendations['location_details'].items():
        print(f"{key.replace('_', ' ').title()}: {value}")
    
    print("\nHighly Suitable Crops:")
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
        print("No highly suitable crops found for this location and season")
    
    print("\nModerately Suitable Crops:")
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
        print("No moderately suitable crops found for this location and season")

if __name__ == "__main__":
    main()