import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from geopy.geocoders import Nominatim
class FertilizerRecommendationSystem:
    def __init__(self):
        """Initialize fertilizer recommendation system"""
        self.db_path = "WareHouse"
        self.crop_nutrient_requirements = self._load_crop_nutrient_requirements()

    def _load_crop_nutrient_requirements(self):
        """Load crop nutrient requirements from the database"""
        try:
            conn = sqlite3.connect(f'{self.db_path}/transformed_crop_data.db')
            query = """
            SELECT label as crop_name, N, P, K, rainfall, ph
            FROM transformed_crop_data
            GROUP BY label
            """
            crop_data = pd.read_sql_query(query, conn)
            conn.close()

            requirements = {}
            for _, row in crop_data.iterrows():
                requirements[row['crop_name'].lower()] = {  
                    'N': {
                        'low': max(0, row['N'] * 0.8),
                        'medium': row['N'],
                        'high': row['N'] * 1.2
                    },
                    'P': {
                        'low': max(0, row['P'] * 0.8),
                        'medium': row['P'],
                        'high': row['P'] * 1.2
                    },
                    'K': {
                        'low': max(0, row['K'] * 0.8),
                        'medium': row['K'],
                        'high': row['K'] * 1.2
                    },
                    'growth_stages': ['Initial', 'Development', 'Mid-season', 'Late-season'],
                    'stage_requirements': {
                        'N': [0.2, 0.3, 0.3, 0.2],
                        'P': [0.3, 0.3, 0.2, 0.2],
                        'K': [0.2, 0.3, 0.3, 0.2]
                    }
                }
            return requirements
        except Exception as e:
            print(f"Error loading crop nutrient requirements: {e}")
            return {}

    def get_soil_health_data(self, location):
        """Get soil health data from database"""
        try:
            conn = sqlite3.connect(f'{self.db_path}/soil_health_transformed.db')
            query = """
            SELECT 
                nitrogen_level,
                phosphorous_level,
                potassium_level,
                organic_carbon_level,
                ph_level,
                ec_level,
                npk_score,
                micro_score,
                overall_soil_health_score
            FROM soil_health
            WHERE LOWER(district) = LOWER(?)
            GROUP BY district
            """
            
            soil_data = pd.read_sql_query(query, conn, params=(location,))
            conn.close()

            if soil_data.empty:
                return None

            soil_dict = {
                'nitrogen_level': soil_data['nitrogen_level'].iloc[0],
                'phosphorous_level': soil_data['phosphorous_level'].iloc[0],
                'potassium_level': soil_data['potassium_level'].iloc[0],
                'ph_level': soil_data['ph_level'].iloc[0],
                'overall_soil_health_score': soil_data['overall_soil_health_score'].iloc[0]
            }
            return soil_dict
            
        except Exception as e:
            print(f"Error getting soil health data: {e}")
            return None

    def get_fertilizer_data(self):
        """Get fertilizer recommendations from database"""
        try:
            conn = sqlite3.connect(f'{self.db_path}/fertilizer_recommendation.db')
            query = """
            SELECT *
            FROM fertilizer_recommendations
            WHERE effectiveness_score >= 70
            ORDER BY effectiveness_score DESC
            """
            fertilizer_data = pd.read_sql_query(query, conn)
            conn.close()
            return fertilizer_data
        except Exception as e:
            print(f"Error getting fertilizer data: {e}")
            return pd.DataFrame()

    def calculate_nutrient_requirements(self, crop, soil_data):
        """Calculate required nutrients based on soil health and crop needs"""
        if crop not in self.crop_nutrient_requirements:
            return None
            
        crop_needs = self.crop_nutrient_requirements[crop]
        current_levels = {
            'N': soil_data['nitrogen_level'],
            'P': soil_data['phosphorous_level'],
            'K': soil_data['potassium_level']
        }
        
        requirements = {}
        for nutrient in ['N', 'P', 'K']:
            if current_levels[nutrient] < 30:
                target = crop_needs[nutrient]['high']
            elif current_levels[nutrient] < 60:
                target = crop_needs[nutrient]['medium']
            else:
                target = crop_needs[nutrient]['low']
                
            required = max(0, target - current_levels[nutrient])
            requirements[nutrient] = required
            
        return requirements

    def get_application_schedule(self, crop, nutrient_requirements):
        """Generate fertilizer application schedule"""
        if crop not in self.crop_nutrient_requirements:
            return None
            
        schedule = []
        crop_info = self.crop_nutrient_requirements[crop]
        
        for i, stage in enumerate(crop_info['growth_stages']):
            stage_requirements = {
                'stage': stage,
                'nutrients': {
                    'N': nutrient_requirements['N'] * crop_info['stage_requirements']['N'][i],
                    'P': nutrient_requirements['P'] * crop_info['stage_requirements']['P'][i],
                    'K': nutrient_requirements['K'] * crop_info['stage_requirements']['K'][i]
                }
            }
            schedule.append(stage_requirements)
            
        return schedule

    def recommend_fertilizers(self, nutrient_requirements, soil_data):
        """Recommend specific fertilizers based on requirements"""
        fertilizer_data = self.get_fertilizer_data()
        
        recommendations = []
        for _, fertilizer in fertilizer_data.iterrows():
            n_match = min(1, fertilizer['n_ratio'] / nutrient_requirements['N']) if nutrient_requirements['N'] > 0 else 1
            p_match = min(1, fertilizer['p_ratio'] / nutrient_requirements['P']) if nutrient_requirements['P'] > 0 else 1
            k_match = min(1, fertilizer['k_ratio'] / nutrient_requirements['K']) if nutrient_requirements['K'] > 0 else 1
            
            match_score = (n_match + p_match + k_match) / 3
            
            if match_score >= 0.6:
                recommendations.append({
                    'fertilizer': fertilizer['recommended_fertilizer'],
                    'match_score': match_score,
                    'effectiveness_score': fertilizer['effectiveness_score'],
                    'application_rate': fertilizer['application_rate']
                })
                
        return sorted(recommendations, key=lambda x: x['match_score'], reverse=True)

    def get_fertilizer_plan(self, location, crop):
        """Generate comprehensive fertilizer plan"""
        soil_data = self.get_soil_health_data(location)
        if soil_data is None:
            return {"error": f"No soil health data found for location: {location}"}
            
        nutrient_requirements = self.calculate_nutrient_requirements(crop, soil_data)
        if nutrient_requirements is None:
            return {"error": f"No nutrient requirements data available for crop: {crop}"}
            
        schedule = self.get_application_schedule(crop, nutrient_requirements)
        if schedule is None:
            return {"error": f"Could not generate schedule for crop: {crop}"}
            
        fertilizer_recommendations = self.recommend_fertilizers(nutrient_requirements, soil_data)
        
        return {
            'crop': crop,
            'location': location,
            'soil_health': {
                'nitrogen': soil_data['nitrogen_level'],
                'phosphorous': soil_data['phosphorous_level'],
                'potassium': soil_data['potassium_level'],
                'ph': soil_data['ph_level']
            },
            'nutrient_requirements': nutrient_requirements,
            'application_schedule': schedule,
            'recommended_fertilizers': fertilizer_recommendations[:3],
            'estimated_cost': self._calculate_cost(
                fertilizer_recommendations[0] if fertilizer_recommendations else None,
                nutrient_requirements
            )
        }

    def _calculate_cost(self, recommended_fertilizer, nutrient_requirements):
        """Calculate estimated cost of fertilizer plan"""
        if recommended_fertilizer is None:
            return None
            
        try:
            conn = sqlite3.connect(f'{self.db_path}/fertilizer_recommendation.db')
            query = """
            SELECT 
                n_ratio,
                p_ratio,
                k_ratio,
                application_rate,
                effectiveness_score
            FROM fertilizer_recommendations
            WHERE recommended_fertilizer = ?
            ORDER BY effectiveness_score DESC
            LIMIT 1
            """
            
            fertilizer_data = pd.read_sql_query(query, conn, params=(recommended_fertilizer['fertilizer'],))
            conn.close()
            
            if fertilizer_data.empty:
                # Fallback to default costs if fertilizer not found
                base_cost = {'N': 50, 'P': 70, 'K': 40}
                total_cost = sum(nutrient_requirements[nutrient] * base_cost[nutrient] 
                               for nutrient in ['N', 'P', 'K'])
                return round(total_cost, 2)
            
            # Calculate cost based on application rate and nutrient ratios
            row = fertilizer_data.iloc[0]
            application_rate = row['application_rate']
            
            # Base cost calculation using nutrient ratios
            n_cost = nutrient_requirements['N'] * row['n_ratio'] * 50  # ₹50 per kg N
            p_cost = nutrient_requirements['P'] * row['p_ratio'] * 70  # ₹70 per kg P
            k_cost = nutrient_requirements['K'] * row['k_ratio'] * 40  # ₹40 per kg K
            
            total_cost = (n_cost + p_cost + k_cost) * (application_rate / 100)
            
            return round(total_cost, 2)
                        
        except Exception as e:
            print(f"Error calculating cost: {e}")
            # Fallback to default costs if database fails
            base_cost = {'N': 50, 'P': 70, 'K': 40}
            total_cost = sum(nutrient_requirements[nutrient] * base_cost[nutrient] 
                            for nutrient in ['N', 'P', 'K'])
            return round(total_cost, 2)
    @staticmethod
    def get_location_details(lat, lon):
        """Get location details from latitude and longitude"""
        geolocator = Nominatim(user_agent="IIA")
        location = geolocator.reverse(f"{lat},{lon}")
        state = location.raw['address'].get('state')
        district = location.raw['address'].get('state_district')
        return location.address, state, district

def main():
    recommender = FertilizerRecommendationSystem()
    lat = 17.6868
    lon = 83.2185
    
    # Using the static method correctly
    address, state, district = FertilizerRecommendationSystem.get_location_details(lat, lon)
    location = district
    crop = input("Enter the crop name : ")
    
    plan = recommender.get_fertilizer_plan(location, crop)
    
    if 'error' in plan:
        print(f"Error: {plan['error']}")
        return
        
    print(f"\nFertilizer Plan for {crop} in {location}")
    print("\nSoil Health:")
    for nutrient, value in plan['soil_health'].items():
        print(f"{nutrient.capitalize()}: {value}")
        
    print("\nNutrient Requirements (kg/ha):")
    for nutrient, value in plan['nutrient_requirements'].items():
        print(f"{nutrient}: {value:.1f}")
        
    print("\nApplication Schedule:")
    for stage in plan['application_schedule']:
        print(f"\n{stage['stage']}:")
        for nutrient, amount in stage['nutrients'].items():
            print(f"  {nutrient}: {amount:.1f} kg/ha")
            
    print("\nRecommended Fertilizers:")
    for rec in plan['recommended_fertilizers']:
        print(f"\n{rec['fertilizer']}:")
        print(f"Match Score: {rec['match_score']:.2f}")
        print(f"Effectiveness: {rec['effectiveness_score']:.2f}")
        print(f"Application Rate: {rec['application_rate']} kg/ha")
        
    if plan['estimated_cost']:
        print(f"\nEstimated Cost: ₹{plan['estimated_cost']}/ha")

if __name__ == "__main__":
    main()