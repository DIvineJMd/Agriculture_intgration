import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from geopy.geocoders import Nominatim
from rich.console import Console
from rich.table import Table

console = Console()

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
        
    def _get_high_effectiveness_fertilizers(self):
        """Retrieve fertilizers with high effectiveness score."""
        try:
            conn = sqlite3.connect(f'{self.db_path}/fertilizer_recommendation.db')
            query = """
            SELECT 
                recommended_fertilizer, 
                n_ratio, 
                p_ratio, 
                k_ratio, 
                application_rate, 
                effectiveness_score
            FROM fertilizer_recommendations
            WHERE effectiveness_score >= 80
            ORDER BY effectiveness_score DESC
            """
            fertilizer_data = pd.read_sql_query(query, conn)
            conn.close()
            return fertilizer_data
        except Exception as e:
            print(f"Error retrieving high-effectiveness fertilizers: {e}")
            return pd.DataFrame()
    
    def _get_cost_efficient_fertilizers(self):
        """Retrieve fertilizers optimized for cost-efficiency."""
        try:
            conn = sqlite3.connect(f'{self.db_path}/fertilizer_recommendation.db')
            query = """
            SELECT 
                recommended_fertilizer, 
                n_ratio, 
                p_ratio, 
                k_ratio, 
                application_rate, 
                effectiveness_score,
                (effectiveness_score / application_rate) AS cost_efficiency
            FROM fertilizer_recommendations
            WHERE effectiveness_score >= 70
            ORDER BY cost_efficiency DESC
            """
            fertilizer_data = pd.read_sql_query(query, conn)
            conn.close()
            return fertilizer_data
        except Exception as e:
            print(f"Error retrieving cost-efficient fertilizers: {e}")
            return pd.DataFrame()

    def _get_nutrient_specific_fertilizers(self):
        """Retrieve fertilizers categorized by nutrient type."""
        try:
            conn = sqlite3.connect(f'{self.db_path}/fertilizer_recommendation.db')
            query = """
            SELECT 
                recommended_fertilizer, 
                n_ratio, 
                p_ratio, 
                k_ratio, 
                application_rate, 
                effectiveness_score,
                CASE
                    WHEN n_ratio > p_ratio AND n_ratio > k_ratio THEN 'Nitrogen-rich'
                    WHEN p_ratio > n_ratio AND p_ratio > k_ratio THEN 'Phosphorous-rich'
                    WHEN k_ratio > n_ratio AND k_ratio > p_ratio THEN 'Potassium-rich'
                    ELSE 'Balanced'
                END AS nutrient_type
            FROM fertilizer_recommendations
            ORDER BY effectiveness_score DESC
            """
            fertilizer_data = pd.read_sql_query(query, conn)
            conn.close()
            return fertilizer_data
        except Exception as e:
            print(f"Error retrieving nutrient-specific fertilizers: {e}")
            return pd.DataFrame()


    @staticmethod
    def get_location_details(lat, lon):
        """Get location details from latitude and longitude"""
        geolocator = Nominatim(user_agent="IIA")
        location = geolocator.reverse(f"{lat},{lon}")
        state = location.raw['address'].get('state')
        district = location.raw['address'].get('state_district')
        return location.address, state, district

    def create_fertilizer_analysis_view(self, location, crop, plan):
        """Create a comprehensive view combining all fertilizer analysis data"""
        try:
            # Create SQL-like view combining all tables
            query = """
            CREATE VIEW fertilizer_analysis AS
            WITH soil_health AS (
                SELECT 
                    'Nitrogen' as parameter,
                    {nitrogen} as current_level,
                    CASE 
                        WHEN {nitrogen} < 30 THEN 'Low'
                        WHEN {nitrogen} < 60 THEN 'Medium'
                        ELSE 'High'
                    END as status
                UNION ALL
                SELECT 
                    'Phosphorous',
                    {phosphorous},
                    CASE 
                        WHEN {phosphorous} < 30 THEN 'Low'
                        WHEN {phosphorous} < 60 THEN 'Medium'
                        ELSE 'High'
                    END
                UNION ALL
                SELECT 
                    'Potassium',
                    {potassium},
                    CASE 
                        WHEN {potassium} < 30 THEN 'Low'
                        WHEN {potassium} < 60 THEN 'Medium'
                        ELSE 'High'
                    END
                UNION ALL
                SELECT 'pH Level', {ph}, {ph}
            ),
            nutrient_requirements AS (
                SELECT 
                    nutrient,
                    amount,
                    CASE 
                        WHEN amount > 50 THEN 'High'
                        WHEN amount > 25 THEN 'Medium'
                        ELSE 'Low'
                    END as priority
                FROM (
                    SELECT 'N' as nutrient, {n_req} as amount
                    UNION ALL SELECT 'P', {p_req}
                    UNION ALL SELECT 'K', {k_req}
                )
            ),
            application_schedule AS (
                SELECT 
                    stage,
                    n_amount,
                    p_amount,
                    k_amount
                FROM (
                    {schedule_data}
                )
            ),
            fertilizer_recommendations AS (
                SELECT 
                    fertilizer,
                    match_score,
                    effectiveness_score,
                    application_rate
                FROM (
                    {fertilizer_data}
                )
            ),
            cost_analysis AS (
                SELECT 
                    {total_cost} as total_cost,
                    {total_cost}/{stages} as cost_per_stage
            )
            """
            
            # Format query with actual values
            formatted_query = query.format(
                nitrogen=plan['soil_health']['nitrogen'],
                phosphorous=plan['soil_health']['phosphorous'],
                potassium=plan['soil_health']['potassium'],
                ph=plan['soil_health']['ph'],
                n_req=plan['nutrient_requirements']['N'],
                p_req=plan['nutrient_requirements']['P'],
                k_req=plan['nutrient_requirements']['K'],
                schedule_data='\nUNION ALL\n'.join([
                    f"SELECT '{stage['stage']}', {stage['nutrients']['N']}, {stage['nutrients']['P']}, {stage['nutrients']['K']}"
                    for stage in plan['application_schedule']
                ]),
                fertilizer_data='\nUNION ALL\n'.join([
                    f"SELECT '{rec['fertilizer']}', {rec['match_score']}, {rec['effectiveness_score']}, {rec['application_rate']}"
                    for rec in plan['recommended_fertilizers']
                ]),
                total_cost=plan['estimated_cost'],
                stages=len(plan['application_schedule'])
            )
            
            # Create comprehensive view table
            view_table = Table(title=f"[bold cyan]Fertilizer Analysis View for {crop} in {location}[/bold cyan]")
            
            # Add columns for all metrics
            view_table.add_column("Category", style="cyan", width=20)
            view_table.add_column("Parameter", style="green", width=20)
            view_table.add_column("Value", justify="right", width=15)
            view_table.add_column("Status/Notes", justify="center", width=15)
            
            # Add soil health data
            for param, value in plan['soil_health'].items():
                status = "N/A"
                if param != 'ph':
                    num_value = float(value)
                    status = "Low" if num_value < 30 else "Medium" if num_value < 60 else "High"
                view_table.add_row(
                    "Soil Health",
                    param.capitalize(),
                    f"{value}",
                    status
                )
            
            # Add nutrient requirements
            for nutrient, amount in plan['nutrient_requirements'].items():
                priority = "High" if amount > 50 else "Medium" if amount > 25 else "Low"
                view_table.add_row(
                    "Nutrient Requirements",
                    f"Required {nutrient}",
                    f"{amount:.1f} kg/ha",
                    priority
                )
            
            # Add application schedule
            for stage in plan['application_schedule']:
                for nutrient, amount in stage['nutrients'].items():
                    view_table.add_row(
                        "Application Schedule",
                        f"{stage['stage']} - {nutrient}",
                        f"{amount:.1f} kg/ha",
                        "Scheduled"
                    )
            
            # Add fertilizer recommendations
            for rec in plan['recommended_fertilizers']:
                view_table.add_row(
                    "Recommendations",
                    rec['fertilizer'],
                    f"{rec['application_rate']} kg/ha",
                    f"Score: {rec['match_score']:.2f}"
                )
            
            # Add cost analysis
            view_table.add_row(
                "Cost Analysis",
                "Total Cost",
                f"₹{plan['estimated_cost']:,.2f}",
                "Per Hectare"
            )
            view_table.add_row(
                "Cost Analysis",
                "Cost per Stage",
                f"₹{plan['estimated_cost']/len(plan['application_schedule']):,.2f}",
                "Per Stage"
            )
            
            # Display the comprehensive view
            console.print("\n")
            console.print(view_table)
            
        except Exception as e:
            console.print(f"[bold red]Error creating fertilizer analysis view: {e}[/bold red]")
            console.print(f"[yellow]Query attempted:[/yellow]\n{formatted_query}")

def main():
    recommender = FertilizerRecommendationSystem()
    lat = 17.6868
    lon = 83.2185
    
    address, state, district = FertilizerRecommendationSystem.get_location_details(lat, lon)
    location = district
    crop = input("Enter the crop name : ")
    
    plan = recommender.get_fertilizer_plan(location, crop)
    
    if 'error' in plan:
        print(f"Error: {plan['error']}")
        return
    
    # Display comprehensive analysis view
    recommender.create_fertilizer_analysis_view(location, crop, plan)
    
    # Original output
    console.print(f"\n[bold blue]Fertilizer Plan Summary for {crop} in {location}[/bold blue]")
    console.print("[bold green]\nSoil Health:[/bold green]")
    for nutrient, value in plan['soil_health'].items():
        print(f"{nutrient.capitalize()}: {value}")
        
    console.print("[bold yellow]\nNutrient Requirements (kg/ha):[/bold yellow]")
    for nutrient, value in plan['nutrient_requirements'].items():
        print(f"{nutrient}: {value:.1f}")
        
    console.print("[bold cyan]\nApplication Schedule:[/bold cyan]")
    for stage in plan['application_schedule']:
        print(f"\n{stage['stage']}:")
        for nutrient, amount in stage['nutrients'].items():
            print(f"  {nutrient}: {amount:.1f} kg/ha")
            
    console.print("[bold green]\nRecommended Fertilizers:[/bold green]")
    for rec in plan['recommended_fertilizers']:
        print(f"\n{rec['fertilizer']}:")
        print(f"Match Score: {rec['match_score']:.2f}")
        print(f"Effectiveness: {rec['effectiveness_score']:.2f}")
        print(f"Application Rate: {rec['application_rate']} kg/ha")
        
    if plan['estimated_cost']:
        print(f"\nEstimated Cost: ₹{plan['estimated_cost']}/ha")

if __name__ == "__main__":
    main()