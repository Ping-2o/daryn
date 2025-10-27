"""
EcoImpact AI - Central Asia Data Collection
============================================
Loads 365 mining sites from Kazakhstan, Uzbekistan, Kyrgyzstan, Tajikistan
Generates environmental estimates for all sites

Author: [Your Name]
Competition: Daryn 2025-2026 (Regional Expansion)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

print("="*80)
print("🌍 ECOIMPACT AI - CENTRAL ASIA DATASET")
print("="*80)

# ============================================================================
# STEP 1: LOAD MENDELEY CENTRAL ASIA DATA
# ============================================================================

print("\n[Step 1/4] Loading Mendeley Central Asia mining dataset...")

# Load the downloaded file (adjust filename as needed)
df_mendeley = pd.read_csv('mendeley_central_asia_mines.csv')  # or .xlsx

print(f"  Total mines: {len(df_mendeley)}")
print(f"  Countries: {df_mendeley['Country'].value_counts().to_dict()}")

# Filter for complete records
df_mendeley = df_mendeley[df_mendeley['Country'].isin([
    'Kazakhstan', 'Uzbekistan', 'Kyrgyzstan', 'Tajikistan'
])]

print(f"  After filtering: {len(df_mendeley)} mines")

# ============================================================================
# STEP 2: PREPARE GEOGRAPHIC/CLIMATE DATA FOR EACH COUNTRY
# ============================================================================

print("\n[Step 2/4] Setting country-specific parameters...")

# Country-specific environmental baselines
country_params = {
    'Kazakhstan': {
        'temp_range': (-5, 15),
        'precip_range': (200, 600),
        'elevation_range': (200, 1500),
        'water_quality_baseline': (5, 8),
        'air_quality_baseline': (5, 8),
        'soil_types': ['Sandy', 'Clay', 'Loam', 'Rocky'],
        'oblasts': ['Pavlodar', 'Karaganda', 'East Kazakhstan', 'West Kazakhstan', 'Kostanay']
    },
    'Uzbekistan': {
        'temp_range': (5, 20),
        'precip_range': (100, 400),
        'elevation_range': (200, 2000),
        'water_quality_baseline': (4, 7),
        'air_quality_baseline': (4, 7),
        'soil_types': ['Sandy', 'Clay', 'Desert'],
        'oblasts': ['Tashkent', 'Navoi', 'Samarkand', 'Fergana']
    },
    'Kyrgyzstan': {
        'temp_range': (-2, 12),
        'precip_range': (300, 800),
        'elevation_range': (500, 3000),
        'water_quality_baseline': (6, 9),
        'air_quality_baseline': (6, 9),
        'soil_types': ['Mountain', 'Loam', 'Rocky'],
        'oblasts': ['Chuy', 'Jalal-Abad', 'Osh', 'Issyk-Kul']
    },
    'Tajikistan': {
        'temp_range': (0, 15),
        'precip_range': (400, 1000),
        'elevation_range': (800, 4000),
        'water_quality_baseline': (6, 8),
        'air_quality_baseline': (6, 8),
        'soil_types': ['Mountain', 'Rocky', 'Loam'],
        'oblasts': ['Dushanbe', 'Sughd', 'Khatlon']
    }
}

# ============================================================================
# STEP 3: GENERATE ENVIRONMENTAL FEATURES
# ============================================================================

print("\n[Step 3/4] Generating environmental features for all 365 mines...")

augmented_data = []

for idx, mine in df_mendeley.iterrows():
    site = {}
    
    # Copy Mendeley data
    site['site_id'] = f"{mine.get('Country', 'UNK')}_{idx}"
    site['name'] = mine.get('Mine_Name', f'Mine_{idx}')
    site['country'] = mine.get('Country', 'Unknown')
    site['mineral_type'] = mine.get('Primary_Commodity', 'Unknown')
    
    # Get country-specific parameters
    country = site['country']
    params = country_params.get(country, country_params['Kazakhstan'])
    
    # Estimate mine type from Mendeley data (or assign randomly)
    if 'Asset_Type' in mine:
        site['mine_type'] = mine['Asset_Type']
    else:
        site['mine_type'] = np.random.choice(['Open-pit', 'Underground', 'Mixed'])
    
    # Generate production based on mineral type and country
    mineral = site['mineral_type']
    if mineral in ['Copper', 'Gold', 'Uranium']:
        production_base = 200000
    elif mineral in ['Coal', 'Iron']:
        production_base = 400000
    else:
        production_base = 150000
    
    site['production_volume_tons'] = max(50000, np.random.normal(production_base, production_base * 0.5))
    
    # Generate other mining parameters
    site['operation_years'] = np.random.randint(5, 35)
    
    if site['mine_type'] == 'Open-pit':
        site['depth_meters'] = np.random.randint(100, 500)
    elif site['mine_type'] == 'Underground':
        site['depth_meters'] = np.random.randint(200, 1000)
    else:
        site['depth_meters'] = np.random.randint(150, 700)
    
    site['status'] = np.random.choice(['Active', 'Active', 'Active', 'Closed'])
    
    # Geographic features (country-specific)
    site['distance_to_water_km'] = np.random.uniform(0.5, 20)
    site['elevation_m'] = np.random.randint(*params['elevation_range'])
    site['annual_precipitation_mm'] = np.random.randint(*params['precip_range'])
    site['avg_temperature_c'] = np.random.uniform(*params['temp_range'])
    site['soil_type'] = np.random.choice(params['soil_types'])
    site['oblast'] = np.random.choice(params['oblasts'])
    
    # Baseline environmental conditions (country-specific)
    site['baseline_water_quality'] = np.random.uniform(*params['water_quality_baseline'])
    site['baseline_air_quality'] = np.random.uniform(*params['air_quality_baseline'])
    site['baseline_ndvi'] = np.random.uniform(0.3, 0.7)
    
    # Location (use Mendeley coordinates if available, otherwise estimate)
    if 'Latitude' in mine and not pd.isna(mine['Latitude']):
        site['latitude'] = mine['Latitude']
        site['longitude'] = mine['Longitude']
    else:
        # Central Asia coordinate ranges
        coord_ranges = {
            'Kazakhstan': {'lat': (42, 55), 'lon': (46, 87)},
            'Uzbekistan': {'lat': (37, 46), 'lon': (56, 73)},
            'Kyrgyzstan': {'lat': (39, 43), 'lon': (69, 80)},
            'Tajikistan': {'lat': (37, 41), 'lon': (67, 75)}
        }
        ranges = coord_ranges.get(country, coord_ranges['Kazakhstan'])
        site['latitude'] = np.random.uniform(*ranges['lat'])
        site['longitude'] = np.random.uniform(*ranges['lon'])
    
    # ========================================================================
    # ENVIRONMENTAL IMPACT ESTIMATES
    # ========================================================================
    
    # Water contamination (adjusted for country water quality)
    base_water = site['production_volume_tons'] / 100000
    distance_factor = 1 / (site['distance_to_water_km'] + 1)
    depth_factor = 1 + (site['depth_meters'] / 1000)
    baseline_factor = (10 - site['baseline_water_quality']) / 10
    
    site['water_contamination_mg_L'] = max(0.5, 
        base_water * distance_factor * depth_factor * baseline_factor * np.random.uniform(0.7, 1.3)
    )
    
    # Air pollution (higher in drier climates)
    base_air = site['production_volume_tons'] / 50000 + 10
    precip_factor = 1 - (site['annual_precipitation_mm'] / 1000)  # Less rain = more dust
    altitude_factor = 1 + (site['elevation_m'] / 5000)  # Higher elevation = thinner air
    
    site['air_pollution_pm25'] = max(5, 
        base_air * precip_factor * altitude_factor * np.random.uniform(0.7, 1.3)
    )
    
    # Vegetation loss (lower in wetter, mountainous regions)
    base_veg = (site['production_volume_tons'] / 100000) * 10 + (site['operation_years'] / 40) * 20
    climate_factor = 1 - (site['annual_precipitation_mm'] / 1500)  # More rain = less vegetation loss
    
    site['vegetation_loss_percent'] = min(95, max(10, 
        base_veg * climate_factor * np.random.uniform(0.7, 1.3)
    ))
    
    # Other fields
    site['water_ph'] = np.random.uniform(6.5, 8.5)
    site['dust_emissions_tons'] = site['production_volume_tons'] * 0.001
    site['current_ndvi'] = max(0.05, site['baseline_ndvi'] * (1 - site['vegetation_loss_percent']/100))
    site['soil_contamination_index'] = site['water_contamination_mg_L'] * np.random.uniform(0.5, 1.5)
    site['measurement_date'] = datetime.now() - timedelta(days=np.random.randint(30, 730))
    
    # Satellite indices
    site['ndvi_mean'] = max(0.1, 0.7 - (site['vegetation_loss_percent'] / 150))
    site['ndvi_std'] = np.random.uniform(0.05, 0.15)
    site['ndwi_mean'] = np.random.uniform(-0.3, 0.3)
    site['evi_mean'] = max(0.1, 0.6 - (site['vegetation_loss_percent'] / 180))
    
    site['data_source'] = 'Mendeley_Central_Asia'
    
    augmented_data.append(site)
    
    if (idx + 1) % 50 == 0:
        print(f"  Processed {idx + 1}/{len(df_mendeley)} mines...")

df_final = pd.DataFrame(augmented_data)

print(f"\n✓ Generated environmental data for {len(df_final)} Central Asian mines")

# ============================================================================
# STEP 4: SAVE AND SUMMARIZE
# ============================================================================

print("\n[Step 4/4] Saving dataset...")

df_final.to_csv('central_asia_mining_environmental_data.csv', index=False)

print(f"✓ Saved: central_asia_mining_environmental_data.csv")

print("\n" + "="*80)
print("📊 DATASET SUMMARY")
print("="*80)

print(f"\nTotal mining sites: {len(df_final)}")
print(f"\nBy country:")
for country in df_final['country'].unique():
    count = len(df_final[df_final['country'] == country])
    percent = count / len(df_final) * 100
    print(f"  {country}: {count} ({percent:.1f}%)")

print(f"\nBy mineral type:")
for mineral in df_final['mineral_type'].value_counts().head(10).items():
    print(f"  {mineral[0]}: {mineral[1]}")

print(f"\nBy mine type:")
for mine_type in df_final['mine_type'].value_counts().items():
    print(f"  {mine_type[0]}: {mine_type[1]}")

print("\n✅ Central Asia dataset ready for training!")
print("Run your model training script with this new dataset.")
print("="*80)
