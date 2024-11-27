import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

class FertilizerRecommendationSystem:
    def __init__(self):
        """Initialize fertilizer recommendation system"""
        self.db_path = "Transformed_database"
        self.crop_nutrient_requirements = self._load_crop_nutrient_requirements()

    def _load_crop_nutrient_requirements(self):
        """Load crop nutrient requirements from the database"""
        try:
            conn = sqlite3.connect(f'{self.db_path}/transformed_crop_data.db')
            query = """
            SELECT label as crop_name, N, P, K, rainfall, ph
            FROM crop_data
            GROUP BY label
            """
            crop_data = pd.read_sql_query(query, conn)
            conn.close()

            requirements = {}
            for _, row in crop_data.iterrows():
                requirements[row['crop_name'].lower()] = {  # Convert to lowercase
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
            SELECT nitrogen_level,
                phosphorous_level,
                potassium_level,
                ph_level,
                district,
                state,
                overall_soil_health_score
            FROM soil_health
            WHERE block = ?
            """
            soil_data = pd.read_sql_query(query, conn, params=(location,))
            conn.close()

            if soil_data.empty:
                return None

            soil_dict = soil_data.iloc[0].to_dict()
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
            conn = sqlite3.connect(f'{self.db_path}/fertilizer_costs.db')
            query = "SELECT nutrient, cost_per_kg FROM fertilizer_costs"
            costs = pd.read_sql_query(query, conn)
            conn.close()
            
            base_cost = dict(zip(costs['nutrient'], costs['cost_per_kg']))
            total_cost = sum(nutrient_requirements[nutrient] * base_cost[nutrient] 
                           for nutrient in ['N', 'P', 'K'])
                        
            return round(total_cost, 2)
        except Exception as e:
            print(f"Error calculating cost: {e}")
            # Fallback to default costs if database fails
            base_cost = {'N': 50, 'P': 70, 'K': 40}
            total_cost = sum(nutrient_requirements[nutrient] * base_cost[nutrient] 
                           for nutrient in ['N', 'P', 'K'])
            return round(total_cost, 2)

def main():
    recommender = FertilizerRecommendationSystem()
    location = "GOOTY"
    crop = "mango"  # Make sure this matches the label in your crop_data table
    
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