#!/usr/bin/env python
# coding: utf-8

# In[ ]:

### WHAT THIS CODE DOES ###
# Contact: Mandy M. Lopez amanda.m.lopez@jpl.nasa.gov

# Queries and downloads Sentinel-3 OLCI Level 2 Ocean Colour data collections from EUMETSAT Data Store
# Here southern California ROI is used
# As of August 2025 the full data record is split between two collections:
#   EO:EUM:DAT:0556 04-25-16 to 04-28-21
#   EO:EUM:DAT:0407 04-29-21 to 07-30-25
# I am batching downloads by year to avoid hitting data download limit errors
# This results in 11 download batches:
# EO:EUM:DAT:0556
# (1)     Jan 2016 - Dec 2016
# (2)     Jan 2017 - Dec 2017
# (3)     Jan 2018 - Dec 2018
# (4)     Jan 2019 - Dec 2019
# (5)     Jan 2020 - Dec 2020
# (6)     Jan 2021 - Dec 2021
# EO:EUM:DAT:0407
# (7)     Jan 2021 - Dec 2021
# (8)     Jan 2022 - Dec 2022
# (9)     Jan 2023 - Dec 2023
# (10)    Jan 2024 - Dec 2024
# (11)    Jan 2025 - Jul 2025

import os
import csv
import json
import datetime
import shutil
import eumdac
import requests

# --------------------------------------------------------------
# Step 1: Setup directories and credentials
# --------------------------------------------------------------
download_dir = os.path.join(os.getcwd(), "/Users/lopezama/Documents/Blackwood/S3/scripts/eumdac/products")
logs_dir = os.path.join(os.getcwd(), "/Users/lopezama/Documents/Blackwood/S3/scripts/eumdac/logs")
os.makedirs(download_dir, exist_ok=True)
os.makedirs(logs_dir, exist_ok=True)

cred_path = os.path.join(os.path.expanduser("~"), ".eumdac", "credentials")

with open(cred_path) as json_file:
    credentials = json.load(json_file)
    token = eumdac.AccessToken((credentials["consumer_key"], credentials["consumer_secret"]))

try:
    print(f"This token '{token}' expires {token.expiration}")
except requests.exceptions.HTTPError as exc:
    print(f"Error when trying the request to the server: '{exc}'")

# Create data store object
datastore = eumdac.DataStore(token)
"""
# Option to show all Sentinel-3 collections in Data Store so you can get the collection IDs
for collectionID in datastore.collections:
    if 'Sentinel-3' in collectionID.title:
        try:
            print(f"{collectionID}: {collectionID.title}")
        except eumdac.datastore.DataStoreError as error:
            print(f"Error related to the data store: '{error.msg}'")
        except eumdac.collection.CollectionError as error:
            print(f"Error related to a collection: '{error.msg}'")
        except requests.exceptions.RequestException as error:
            print(f"Unexpected error: {error}")
"""
# Set collection IDs
collection0407_ID = 'EO:EUM:DAT:0407' # OLCI Level 2 Ocean Colour Full Resolution - Sentinel-3
collection0556_ID = 'EO:EUM:DAT:0556' # OLCI Level 2 Ocean Colour Full Resolution (version BC003) - Sentinel-3 - Reprocessed

# Define ROI (same for all batches)
WKT = "POLYGON((-121.343994 32.369016, -116.883545 32.369016, -116.883545 35.270920, -121.343994 35.270920, -121.343994 32.369016))"


"""
# List all data collections in the Data Store

# create data store object
datastore = eumdac.DataStore(token)

# show all collections:
for collectionID in datastore.collections:
    if 'Sentinel-3' in collectionID.title:
        try:
            print(f"{collectionID}: {collectionID.title}")
        except eumdac.datastore.DataStoreError as error:
            print(f"Error related to the data store: '{error.msg}'")
        except eumdac.collection.CollectionError as error:
            print(f"Error related to a collection: '{error.msg}'")
        except requests.exceptions.RequestException as error:
            print(f"Unexpected error: {error}")
"""

# --------------------------------------------------------------
# Step 2: Define batch configurations
# --------------------------------------------------------------
batch_configs = [
    {
        'name': 'Batch_01_0556_2016',
        'collection_id': collection0556_ID,
        'start_date': datetime.datetime(2016, 1, 1),
        'end_date': datetime.datetime(2016, 12, 31)
    },
    {
        'name': 'Batch_02_0556_2017',
        'collection_id': collection0556_ID,
        'start_date': datetime.datetime(2017, 1, 1),
        'end_date': datetime.datetime(2017, 12, 31)
    },
    {
        'name': 'Batch_03_0556_2018',
        'collection_id': collection0556_ID,
        'start_date': datetime.datetime(2018, 1, 1),
        'end_date': datetime.datetime(2018, 12, 31)
    },
    {
        'name': 'Batch_04_0556_2019',
        'collection_id': collection0556_ID,
        'start_date': datetime.datetime(2019, 1, 1),
        'end_date': datetime.datetime(2019, 12, 31)
    },
    {
        'name': 'Batch_05_0556_2020',
        'collection_id': collection0556_ID,
        'start_date': datetime.datetime(2020, 1, 1),
        'end_date': datetime.datetime(2020, 12, 31)
    },
    {
        'name': 'Batch_06_0556_2021',
        'collection_id': collection0556_ID,
        'start_date': datetime.datetime(2021, 1, 1),
        'end_date': datetime.datetime(2021, 12, 31)
    },
    {
        'name': 'Batch_07_0407_2021',
        'collection_id': collection0407_ID,
        'start_date': datetime.datetime(2021, 1, 1),
        'end_date': datetime.datetime(2021, 12, 31)
    },
    {
        'name': 'Batch_08_0407_2022',
        'collection_id': collection0407_ID,
        'start_date': datetime.datetime(2022, 1, 1),
        'end_date': datetime.datetime(2022, 12, 31)
    },
    {
        'name': 'Batch_09_0407_2023',
        'collection_id': collection0407_ID,
        'start_date': datetime.datetime(2023, 1, 1),
        'end_date': datetime.datetime(2023, 12, 31)
    },
    {
        'name': 'Batch_10_0407_2024',
        'collection_id': collection0407_ID,
        'start_date': datetime.datetime(2024, 1, 1),
        'end_date': datetime.datetime(2024, 12, 31)
    },
    {
        'name': 'Batch_11_0407_2025',
        'collection_id': collection0407_ID,
        'start_date': datetime.datetime(2025, 1, 1),
        'end_date': datetime.datetime(2025, 7, 31)
    }
]

# --------------------------------------------------------------
# Step 3: Helper functions
# --------------------------------------------------------------
def create_log_file(batch_name):
    """Create CSV log file with headers for a batch"""
    log_file = os.path.join(logs_dir, f"{batch_name}_download_log.csv")
    
    # Check if log file already exists
    if not os.path.exists(log_file):
        with open(log_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'timestamp', 'batch_name', 'product_name', 'zip_filename', 
                'status', 'file_size_mb', 'error_message', 'download_time_seconds'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
    
    return log_file

def log_download_result(log_file, batch_name, product_name, zip_filename, status, 
                       file_size_mb=None, error_message=None, download_time=None):
    """Log download result to CSV file"""
    with open(log_file, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'timestamp', 'batch_name', 'product_name', 'zip_filename', 
            'status', 'file_size_mb', 'error_message', 'download_time_seconds'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writerow({
            'timestamp': datetime.datetime.now().isoformat(),
            'batch_name': batch_name,
            'product_name': product_name,
            'zip_filename': zip_filename,
            'status': status,
            'file_size_mb': file_size_mb,
            'error_message': error_message,
            'download_time_seconds': download_time
        })

def get_file_size_mb(file_path):
    """Get file size in MB"""
    try:
        size_bytes = os.path.getsize(file_path)
        return round(size_bytes / (1024 * 1024), 2)
    except:
        return None

def process_filename(product_name):
    """Ensure filename has proper .zip extension"""
    if product_name.endswith('.SEN3'):
        return product_name + '.zip'
    elif not product_name.endswith('.zip'):
        return product_name + '.SEN3.zip'
    else:
        return product_name

# --------------------------------------------------------------
# Step 4: Process each batch
# --------------------------------------------------------------
def process_batch(batch_config):
    """Process a single batch - search and download"""
    batch_name = batch_config['name']
    collection_id = batch_config['collection_id']
    start_date = batch_config['start_date']
    end_date = batch_config['end_date']
    
    print(f"\n{'='*60}")
    print(f"Processing {batch_name}")
    print(f"Collection: {collection_id}")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"{'='*60}")
    
    # Create log file for this batch
    log_file = create_log_file(batch_name)
    
    try:
        # Get collection and search for products
        collection = datastore.get_collection(collection_id)
        products = collection.search(
            geo=WKT,
            dtstart=start_date,
            dtend=end_date
        )
        
        # Convert to list
        product_list = list(products)
        total_products = len(product_list)
        
        print(f"Found {total_products} products for {batch_name}")
        
        if total_products == 0:
            log_download_result(log_file, batch_name, "N/A", "N/A", "NO_PRODUCTS_FOUND")
            return
        
        # Download each product
        successful_downloads = 0
        skipped_files = 0
        failed_downloads = 0
        
        for i, product in enumerate(product_list, 1):
            product_name = str(product)
            zip_filename = process_filename(product_name)
            file_path = os.path.join(download_dir, zip_filename)
            
            print(f"\n[{i}/{total_products}] Processing: {zip_filename}")
            
            # Check if file already exists
            if os.path.exists(file_path):
                file_size = get_file_size_mb(file_path)
                print(f"  File already exists ({file_size} MB), skipping...")
                log_download_result(log_file, batch_name, product_name, zip_filename, 
                                  "SKIPPED_EXISTS", file_size)
                skipped_files += 1
                continue
            
            # Download the product
            download_start = datetime.datetime.now()
            try:
                with product.open() as fsrc, open(file_path, mode='wb') as fdst:
                    shutil.copyfileobj(fsrc, fdst)
                
                download_end = datetime.datetime.now()
                download_time = (download_end - download_start).total_seconds()
                file_size = get_file_size_mb(file_path)
                
                print(f"  Downloaded successfully ({file_size} MB in {download_time:.1f}s)")
                log_download_result(log_file, batch_name, product_name, zip_filename, 
                                  "SUCCESS", file_size, download_time=download_time)
                successful_downloads += 1
                
            except Exception as error:
                download_end = datetime.datetime.now()
                download_time = (download_end - download_start).total_seconds()
                error_message = str(error)
                
                print(f"  Download failed: {error_message}")
                log_download_result(log_file, batch_name, product_name, zip_filename, 
                                  "FAILED", error_message=error_message, download_time=download_time)
                failed_downloads += 1
                
                # Remove partial file if it exists
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except:
                        pass
        
        # Print batch summary
        print(f"\n{batch_name} Summary:")
        print(f"  Total products found: {total_products}")
        print(f"  Successful downloads: {successful_downloads}")
        print(f"  Skipped (already exist): {skipped_files}")
        print(f"  Failed downloads: {failed_downloads}")
        
        # Log batch summary
        log_download_result(log_file, batch_name, "BATCH_SUMMARY", "N/A", "SUMMARY", 
                          error_message=f"Found:{total_products}, Success:{successful_downloads}, Skipped:{skipped_files}, Failed:{failed_downloads}")
        
    except Exception as error:
        print(f"Error processing batch {batch_name}: {error}")
        log_download_result(log_file, batch_name, "BATCH_ERROR", "N/A", "BATCH_FAILED", 
                          error_message=str(error))

# --------------------------------------------------------------
# Step 5: Main execution
# --------------------------------------------------------------
def main():
    """Main execution function"""
    print("Starting batch download process...")
    print(f"Download directory: {download_dir}")
    print(f"Logs directory: {logs_dir}")
    
    start_time = datetime.datetime.now()
    
    # Process each batch
    for batch_config in batch_configs:
        try:
            process_batch(batch_config)
        except KeyboardInterrupt:
            print("\n\nDownload interrupted by user. Exiting...")
            break
        except Exception as error:
            print(f"\nUnexpected error processing batch {batch_config['name']}: {error}")
            continue
    
    end_time = datetime.datetime.now()
    total_time = (end_time - start_time).total_seconds()
    
    print(f"\n{'='*60}")
    print("ALL BATCHES COMPLETED!")
    print(f"Total processing time: {total_time/3600:.2f} hours")
    print(f"Check individual log files in: {logs_dir}")
    print(f"{'='*60}")

# Run the main function
if __name__ == "__main__":
    main()

"""
# If you see an error like this, it is likely that your account needs more time for the data licenses to be active. Wait and re-try after 2-3 days.

Downloading product 1/2: S3A_OL_2_WFR____20160503T182657_20160503T182857_20210705T072114_0119_003_355______MAR_R_NT_003.SEN3
Error related to the product: 'Could not download Product S3A_OL_2_WFR____20160503T182657_20160503T182857_20210705T072114_0119_003_355______MAR_R_NT_003.SEN3 of Collection EO:EUM:DAT:0556 - Unauthorised (403)'

Downloading product 2/2: S3A_OL_2_WFR____20160501T173830_20160501T174030_20210704T233845_0119_003_326______MAR_R_NT_003.SEN3
Error related to the product: 'Could not download Product S3A_OL_2_WFR____20160501T173830_20160501T174030_20210704T233845_0119_003_326______MAR_R_NT_003.SEN3 of Collection EO:EUM:DAT:0556 - Unauthorised (403)'
"""
