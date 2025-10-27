"""
EcoImpact AI - Data Collection Script (FIXED VERSION)
======================================================
This script downloads and saves all necessary data for model training.

FIXES:
- Fixed 'oblast' KeyError in merge step
- Improved error handling
- Better sample data generation

Author: [Your Name]
Date: October 2025
Competition: Daryn 2025-2026
"""

import os
import requests
import pandas as pd
import numpy as np
from pathlib import Path
import time
from datetime import datetime, timedelta
import json
import warnings
warnings.filterwarnings('ignore')

# Try to import Google Earth Engine (optional)
try:
    import ee
    GEE_AVAILABLE = True
except ImportError:
    print("⚠️  Google Earth Engine not available. Install with: pip install earthengine-api")
    GEE_AVAILABLE = False

# ============================================================================
# CONFIGURATION
# ============================================================================

# Project directories
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
RAW_DIR = DATA_DIR / 'raw'
PROCESSED_DIR = DATA_DIR / 'processed'

# Create directories if they don't exist
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Kazakhstan major mining regions (for satellite data)
MINING_REGIONS = {
    'Ekibastuz': {'lat': 52.0, 'lon': 75.3, 'type': 'Coal', 'oblast': 'Pavlodar'},
    'Zhezkazgan': {'lat': 47.8, 'lon': 67.7, 'type': 'Copper', 'oblast': 'Karaganda'},
    'Karaganda': {'lat': 49.8, 'lon': 73.1, 'type': 'Coal', 'oblast': 'Karaganda'},
    'Aktogay': {'lat': 47.3, 'lon': 80.5, 'type': 'Copper', 'oblast': 'East Kazakhstan'},
    'Shalkiya': {'lat': 47.8, 'lon': 59.6, 'type': 'Zinc-Lead', 'oblast': 'West Kazakhstan'},
    'Kostanay': {'lat': 53.2, 'lon': 63.6, 'type': 'Iron', 'oblast': 'Kostanay'},
    'Balkhash': {'lat': 46.8, 'lon': 75.0, 'type': 'Copper', 'oblast': 'Karaganda'},
    'Ridder': {'lat': 50.3, 'lon': 83.5, 'type': 'Polymetallic', 'oblast': 'East Kazakhstan'},
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def download_file(url, filename, max_retries=3):
    """
    Download a file from URL with retry logic.
    
    Args:
        url (str): URL to download from
        filename (str): Local filename to save
        max_retries (int): Number of retry attempts
    
    Returns:
        bool: True if successful, False otherwise
    """
    for attempt in range(max_retries):
        try:
            print(f"  Downloading {filename}... (attempt {attempt + 1}/{max_retries})")
            
            response = requests.get(url, timeout=30, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(filename, 'wb') as f:
                if total_size == 0:
                    f.write(response.content)
                else:
                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        downloaded += len(chunk)
                        progress = (downloaded / total_size) * 100
                        print(f"    Progress: {progress:.1f}%", end='\r')
            
            print(f"\n  ✓ Downloaded {filename} successfully!")
            return True
            
        except Exception as e:
            print(f"  ✗ Error downloading {filename}: {e}")
            if attempt < max_retries - 1:
                print(f"  Retrying in 5 seconds...")
                time.sleep(5)
            else:
                print(f"  ✗ Failed to download {filename} after {max_retries} attempts")
                return False
    
    return False


def create_sample_mining_data():
    """
    Create sample mining data if Mendeley download fails.
    This provides a starting dataset for immediate model development.
    
    Returns:
        pd.DataFrame: Sample mining sites data
    """
    print("\n📊 Creating sample mining dataset...")
    
    # Expanded sample data based on known Kazakhstan mining sites
    sample_data = []
    
    for region_name, region_data in MINING_REGIONS.items():
        # Create multiple sample sites per region
        for i in range(3):
            site = {
                'site_id': f"{region_name}_{i+1}",
                'name': f"{region_name} Mine {i+1}",
                'latitude': region_data['lat'] + np.random.uniform(-0.5, 0.5),
                'longitude': region_data['lon'] + np.random.uniform(-0.5, 0.5),
                'country': 'Kazakhstan',
                'oblast': region_data['oblast'],
                'mineral_type': region_data['type'],
                'mine_type': np.random.choice(['Open-pit', 'Underground', 'Mixed']),
                'production_volume_tons': np.random.randint(50000, 500000),
                'operation_years': np.random.randint(5, 40),
                'depth_meters': np.random.randint(50, 800),
                'status': np.random.choice(['Active', 'Active', 'Active', 'Closed']),
            }
            sample_data.append(site)
    
    df = pd.DataFrame(sample_data)
    print(f"  ✓ Created {len(df)} sample mining sites")
    
    return df


def create_sample_environmental_data(mining_df):
    """
    Create sample environmental data corresponding to mining sites.
    
    Args:
        mining_df (pd.DataFrame): Mining sites dataframe
    
    Returns:
        pd.DataFrame: Sample environmental measurements
    """
    print("\n🌍 Creating sample environmental data...")
    
    env_data = []
    
    for idx, site in mining_df.iterrows():
        # Simulate environmental impacts based on production volume and distance
        base_contamination = site['production_volume_tons'] / 100000
        
        env = {
            'site_id': site['site_id'],
            'name': site['name'],
            'oblast': site['oblast'],  # IMPORTANT: Include oblast here
            
            # Water quality (higher production = more contamination)
            'water_contamination_mg_L': base_contamination * np.random.uniform(0.5, 1.5),
            'baseline_water_quality': np.random.uniform(3, 9),  # 1-10 scale
            'water_ph': np.random.uniform(6.5, 8.5),
            
            # Air quality
            'air_pollution_pm25': base_contamination * 3 + np.random.uniform(5, 25),
            'baseline_air_quality': np.random.uniform(4, 9),
            'dust_emissions_tons': site['production_volume_tons'] * 0.001,
            
            # Vegetation
            'vegetation_loss_percent': min(100, base_contamination * 10 + np.random.uniform(10, 40)),
            'baseline_ndvi': np.random.uniform(0.3, 0.7),
            'current_ndvi': np.random.uniform(0.1, 0.4),
            
            # Geographic/Climate
            'distance_to_water_km': np.random.uniform(0.5, 20),
            'elevation_m': np.random.randint(200, 1500),
            'annual_precipitation_mm': np.random.randint(200, 600),
            'avg_temperature_c': np.random.uniform(-5, 15),
            
            # Soil
            'soil_type': np.random.choice(['Sandy', 'Clay', 'Loam', 'Rocky']),
            'soil_contamination_index': base_contamination * np.random.uniform(0.3, 1.2),
            
            # Dates
            'measurement_date': datetime.now() - timedelta(days=np.random.randint(30, 365)),
        }
        
        env_data.append(env)
    
    df = pd.DataFrame(env_data)
    print(f"  ✓ Created environmental data for {len(df)} sites")
    
    return df


# ============================================================================
# DATA DOWNLOAD FUNCTIONS
# ============================================================================

def download_mendeley_mining_data():
    """
    Download Critical Materials Mining in Kazakhstan dataset from Mendeley.
    
    Returns:
        pd.DataFrame or None: Mining data if successful
    """
    print("\n" + "="*70)
    print("📥 DOWNLOADING MENDELEY MINING DATASET")
    print("="*70)
    
    mendeley_file = RAW_DIR / 'mendeley_mining_kazakhstan.csv'
    
    if mendeley_file.exists():
        print(f"  ✓ Found existing file: {mendeley_file}")
        try:
            df = pd.read_csv(mendeley_file)
            print(f"  ✓ Loaded {len(df)} mining sites")
            return df
        except Exception as e:
            print(f"  ✗ Error reading file: {e}")
    
    print("\n  ⚠️  Mendeley data requires manual download:")
    print("  1. Visit: https://data.mendeley.com/datasets/mp328hh34n")
    print("  2. Download the CSV file")
    print(f"  3. Save as: {mendeley_file}")
    print("\n  Creating sample data for now...")
    
    df = create_sample_mining_data()
    df.to_csv(RAW_DIR / 'sample_mining_kazakhstan.csv', index=False)
    
    return df


def download_satellite_data_gee(region_name, lat, lon, start_date='2020-01-01', end_date='2024-12-31'):
    """
    Download satellite imagery data from Google Earth Engine.
    
    Args:
        region_name (str): Name of mining region
        lat (float): Latitude
        lon (float): Longitude
        start_date (str): Start date for imagery
        end_date (str): End date for imagery
    
    Returns:
        dict: Satellite-derived indices
    """
    if not GEE_AVAILABLE:
        # Return simulated data
        return {
            'region': region_name,
            'oblast': MINING_REGIONS[region_name]['oblast'],  # IMPORTANT: Include oblast
            'ndvi_mean': np.random.uniform(0.2, 0.6),
            'ndvi_std': np.random.uniform(0.05, 0.15),
            'ndwi_mean': np.random.uniform(-0.2, 0.3),
            'evi_mean': np.random.uniform(0.2, 0.5),
        }
    
    try:
        # Initialize Earth Engine
        ee.Initialize()
        
        # Define point of interest
        point = ee.Geometry.Point([lon, lat])
        
        # Get Sentinel-2 imagery
        s2 = ee.ImageCollection('COPERNICUS/S2_SR') \
            .filterBounds(point) \
            .filterDate(start_date, end_date) \
            .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20))
        
        # Calculate vegetation indices
        def add_indices(image):
            ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI')
            ndwi = image.normalizedDifference(['B3', 'B8']).rename('NDWI')
            evi = image.expression(
                '2.5 * ((NIR - RED) / (NIR + 6 * RED - 7.5 * BLUE + 1))',
                {
                    'NIR': image.select('B8'),
                    'RED': image.select('B4'),
                    'BLUE': image.select('B2')
                }
            ).rename('EVI')
            return image.addBands([ndvi, ndwi, evi])
        
        s2_with_indices = s2.map(add_indices)
        
        # Calculate statistics
        stats = s2_with_indices.select(['NDVI', 'NDWI', 'EVI']).reduce(
            ee.Reducer.mean().combine(ee.Reducer.stdDev(), '', True)
        )
        
        # Extract values
        values = stats.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=point,
            scale=10,
            maxPixels=1e9
        ).getInfo()
        
        result = {
            'region': region_name,
            'oblast': MINING_REGIONS[region_name]['oblast'],
            'ndvi_mean': values.get('NDVI_mean', np.nan),
            'ndvi_std': values.get('NDVI_stdDev', np.nan),
            'ndwi_mean': values.get('NDWI_mean', np.nan),
            'evi_mean': values.get('EVI_mean', np.nan),
        }
        
        print(f"  ✓ Retrieved satellite data for {region_name}")
        return result
        
    except Exception as e:
        print(f"  ✗ Error retrieving satellite data for {region_name}: {e}")
        # Return simulated data as fallback
        return {
            'region': region_name,
            'oblast': MINING_REGIONS[region_name]['oblast'],
            'ndvi_mean': np.random.uniform(0.2, 0.6),
            'ndvi_std': np.random.uniform(0.05, 0.15),
            'ndwi_mean': np.random.uniform(-0.2, 0.3),
            'evi_mean': np.random.uniform(0.2, 0.5),
        }


def download_all_satellite_data():
    """
    Download satellite data for all mining regions.
    
    Returns:
        pd.DataFrame: Satellite indices for all regions
    """
    print("\n" + "="*70)
    print("🛰️  DOWNLOADING SATELLITE IMAGERY DATA")
    print("="*70)
    
    satellite_data = []
    
    for region_name, region_info in MINING_REGIONS.items():
        print(f"\n  Processing {region_name}...")
        data = download_satellite_data_gee(
            region_name,
            region_info['lat'],
            region_info['lon']
        )
        satellite_data.append(data)
    
    df = pd.DataFrame(satellite_data)
    
    # Save to file
    output_file = RAW_DIR / 'satellite_indices.csv'
    df.to_csv(output_file, index=False)
    print(f"\n  ✓ Saved satellite data to {output_file}")
    
    return df


# ============================================================================
# MAIN DATA COLLECTION WORKFLOW
# ============================================================================

def main():
    """
    Main data collection workflow.
    Downloads all necessary data for model training.
    """
    print("\n" + "="*70)
    print("🚀 ECOIMPACT AI - DATA COLLECTION SCRIPT")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Data directory: {DATA_DIR}")
    print("="*70)
    
    # Track what data we successfully collected
    collected_data = {}
    
    # 1. Download mining sites data
    print("\n[1/4] Mining Sites Data")
    mining_df = download_mendeley_mining_data()
    if mining_df is not None:
        collected_data['mining'] = mining_df
        print(f"  ✓ Collected data for {len(mining_df)} mining sites")
    
    # 2. Create environmental data
    print("\n[2/4] Environmental Indicators")
    if mining_df is not None:
        env_df = create_sample_environmental_data(mining_df)
        collected_data['environment'] = env_df
        env_file = RAW_DIR / 'environmental_measurements.csv'
        env_df.to_csv(env_file, index=False)
        print(f"  ✓ Saved environmental data to {env_file}")
    
    # 3. Download satellite data
    print("\n[3/4] Satellite Imagery Data")
    try:
        satellite_df = download_all_satellite_data()
        collected_data['satellite'] = satellite_df
    except Exception as e:
        print(f"  ✗ Error collecting satellite data: {e}")
    
    # 4. Create merged dataset (FIXED VERSION)
    print("\n[4/4] Merging Datasets")
    if 'mining' in collected_data and 'environment' in collected_data:
        try:
            mining_df = collected_data['mining']
            env_df = collected_data['environment']
            
            # Merge mining and environmental data
            # Both already have 'site_id', 'name', and 'oblast' columns
            merged_df = mining_df.merge(
                env_df,
                on=['site_id', 'name', 'oblast'],  # Merge on common keys
                how='inner'
            )
            
            print(f"  ✓ Merged mining and environmental data: {len(merged_df)} sites")
            
            # Add satellite data if available
            if 'satellite' in collected_data and len(collected_data['satellite']) > 0:
                satellite_df = collected_data['satellite']
                
                # Merge satellite data by oblast
                merged_df = merged_df.merge(
                    satellite_df[['oblast', 'ndvi_mean', 'ndvi_std', 'ndwi_mean', 'evi_mean']],
                    on='oblast',
                    how='left'
                )
                
                print(f"  ✓ Added satellite data")
            
            # Save merged dataset
            output_file = PROCESSED_DIR / 'master_dataset.csv'
            merged_df.to_csv(output_file, index=False)
            
            print(f"\n  ✓ Created master dataset with {len(merged_df)} sites")
            print(f"  ✓ Saved to {output_file}")
            print(f"\n  Columns ({len(merged_df.columns)}): {list(merged_df.columns)}")
            print(f"\n  Dataset shape: {merged_df.shape}")
            
            # Display sample
            print("\n  Sample data (first 3 rows):")
            print(merged_df.head(3).to_string())
            
            # Save summary statistics
            summary_file = PROCESSED_DIR / 'dataset_summary.txt'
            with open(summary_file, 'w') as f:
                f.write("EcoImpact AI - Dataset Summary\n")
                f.write("="*70 + "\n\n")
                f.write(f"Total sites: {len(merged_df)}\n")
                f.write(f"Total features: {len(merged_df.columns)}\n\n")
                f.write("Dataset Info:\n")
                f.write(str(merged_df.info()) + "\n\n")
                f.write("Descriptive Statistics:\n")
                f.write(str(merged_df.describe()) + "\n\n")
                f.write("Missing Values:\n")
                f.write(str(merged_df.isnull().sum()) + "\n")
            
            print(f"  ✓ Saved dataset summary to {summary_file}")
            
        except Exception as e:
            print(f"  ✗ Error merging datasets: {e}")
            import traceback
            traceback.print_exc()
    
    # 5. Create data sources documentation
    print("\n[5/5] Creating Documentation")
    sources_file = DATA_DIR / 'sources.txt'
    with open(sources_file, 'w') as f:
        f.write("EcoImpact AI - Data Sources\n")
        f.write("="*70 + "\n\n")
        f.write(f"Data collected on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("1. MINING SITES DATA\n")
        f.write("   Source: Critical Materials Mining in Kazakhstan (Mendeley)\n")
        f.write("   URL: https://data.mendeley.com/datasets/mp328hh34n\n")
        f.write(f"   Records: {len(collected_data.get('mining', []))}\n")
        f.write("   License: CC BY 4.0\n\n")
        
        f.write("2. ENVIRONMENTAL INDICATORS\n")
        f.write("   Source: Sample data generated based on mining characteristics\n")
        f.write(f"   Records: {len(collected_data.get('environment', []))}\n")
        f.write("   Note: Replace with real data when available\n\n")
        
        f.write("3. SATELLITE IMAGERY\n")
        f.write("   Source: Google Earth Engine (Sentinel-2) or simulated\n")
        f.write("   URL: https://earthengine.google.com\n")
        f.write(f"   Regions: {len(collected_data.get('satellite', []))}\n")
        f.write("   License: Open Access\n\n")
        
        f.write("4. ADDITIONAL SOURCES (for future enhancement)\n")
        f.write("   - ICMM Global Mining Dataset: https://www.icmm.com/en-gb/research/data/2025/global-mining-dataset\n")
        f.write("   - Kazakhstan Open Data: https://odin.opendatawatch.com/Report/countryProfileUpdated/KAZ\n")
        f.write("   - EITI Kazakhstan: https://eiti.org/countries/kazakhstan\n")
    
    print(f"  ✓ Created data sources file: {sources_file}")
    
    # Final summary
    print("\n" + "="*70)
    print("✅ DATA COLLECTION COMPLETE")
    print("="*70)
    print(f"\nCollected datasets:")
    for name, data in collected_data.items():
        print(f"  • {name.capitalize()}: {len(data)} records")
    
    print(f"\nFiles saved in:")
    print(f"  • Raw data: {RAW_DIR}")
    print(f"  • Processed data: {PROCESSED_DIR}")
    
    if (PROCESSED_DIR / 'master_dataset.csv').exists():
        print(f"\n✨ SUCCESS! Master dataset created:")
        print(f"   {PROCESSED_DIR / 'master_dataset.csv'}")
    
    print(f"\n⏭️  Next steps:")
    print(f"  1. Review the data: {PROCESSED_DIR / 'master_dataset.csv'}")
    print(f"  2. Check summary: {PROCESSED_DIR / 'dataset_summary.txt'}")
    print(f"  3. Run preprocessing: python code/02_preprocessing.py")
    print(f"  4. Train models: python code/03_model_training.py")
    
    print(f"\nFinished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70 + "\n")


if __name__ == '__main__':
    main()
