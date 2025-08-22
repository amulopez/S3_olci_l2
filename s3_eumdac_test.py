#!/usr/bin/env python
# coding: utf-8

# In[ ]:


### WHAT THIS CODE DOES ###
# Download Sentinel-3 OLCI Full Resolution Level 2 Ocean Colour data from EUMDAC
# Southern California, USA ROI
# Contact: Mandy M. Lopez amanda.m.lopez@jpl.nasa.gov

import os
import csv
import json
import datetime
import shutil
import eumdac
import requests
from IPython.display import YouTubeVideo, HTML

# --------------------------------------------------------------
# Step 1 Create a download directory for downloaded products
# --------------------------------------------------------------
download_dir = os.path.join(os.getcwd(), "path/to/products")
os.makedirs(download_dir, exist_ok=True)

# --------------------------------------------------------------
# Step 2: Path to the credentials file
# --------------------------------------------------------------
cred_path = os.path.join(os.path.expanduser("~"), ".eumdac", "credentials")

with open(cred_path) as json_file:
    credentials = json.load(json_file)
    token = eumdac.AccessToken((credentials["consumer_key"], credentials["consumer_secret"]))

try:
    print(f"This token '{token}' expires {token.expiration}")
except requests.exceptions.HTTPError as exc:
    print(f"Error when trying the request to the server: '{exc}'")


# --------------------------------------------------------------
# Step 3 (optional): List all data collections in the Data Store
# --------------------------------------------------------------
# Create data store object
datastore = eumdac.DataStore(token)

# Show all collections:
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

# --------------------------------------------------------------
# Step 4: Define which data collections you are interested in
# --------------------------------------------------------------
# Create data store object
datastore = eumdac.DataStore(token) # RUN THIS LINE ONLY IF YOU SKIPPED STEP 3

# Set collection IDs for S3 OLCI L2 
collection0407_ID = 'EO:EUM:DAT:0407' # OLCI Level 2 Ocean Colour Full Resolution - Sentinel-3
collection0556_ID = 'EO:EUM:DAT:0556' # OLCI Level 2 Ocean Colour Full Resolution (version BC003) - Sentinel-3 - Reprocessed

# Use collection ID 0407 OLCI Level 2 Ocean Colour Full Resolution - Sentinel-3
collection0407 = datastore.get_collection(collection0407_ID)
try:
    print(collection0407.title)
except eumdac.collection.CollectionError as error:
    print(f"Error related to a collection: '{error.msg}'")
except requests.exceptions.RequestException as error:
    print(f"Unexpected error: {error}")

# Use collection ID 0556 OLCI Level 2 Ocean Colour Full Resolution (version BC003) - Sentinel-3 - Reprocessed
collection0556 = datastore.get_collection(collection0556_ID)
try:
    print(collection0556.title)
except eumdac.collection.CollectionError as error:
    print(f"Error related to a collection: '{error.msg}'")
except requests.exceptions.RequestException as error:
    print(f"Unexpected error: {error}")

# --------------------------------------------------------------
# Step 5: Test Downloads EO:EUM:DAT:0556 May 1, 2016 - May 4, 2016
# --------------------------------------------
collection0556_May2016 = datastore.get_collection(collection0556_ID)
start = datetime.datetime(2016, 5, 1)
end = datetime.datetime(2016, 5, 4)
WKT = "POLYGON((-121.343994 32.369016, -116.883545 32.369016, -116.883545 35.270920, -121.343994 35.270920, -121.343994 32.369016))"

products_0556_May2016 = collection0556_May2016.search(
    geo=WKT,
    dtstart=start, 
    dtend=end)

# Convert search results to a list to access individual products
product_list = list(products_0556_May2016)
print(f"Found {len(product_list)} products")

for product in product_list:
    try:
        # The product object itself contains the filename as a string
        product_name = str(product)
        print(f"Product: {product_name}")
        
        # Print all available attributes for debugging (just once to see what's available)
        print(f"Available attributes: {[attr for attr in dir(product) if not attr.startswith('_')]}")
        print("-" * 50)
        
    except eumdac.collection.CollectionError as error:
        print(f"Error related to the collection: '{error.message}'")  # Fixed: .msg -> .message
    except eumdac.product.ProductError as error:
        print(f"Error related to the product: '{error}'")
    except requests.exceptions.RequestException as error:
        print(f"Unexpected error: {error}")
    except Exception as error:
        print(f"Unexpected error: {error}")

print(f"\nDownloading all {len(product_list)} products...")

for i, product in enumerate(product_list, 1):
    try:
        product_name = str(product)  # Get the product name as string
        print(f"\nDownloading product {i}/{len(product_list)}: {product_name}")
        
        # Check if file already exists
        file_path = os.path.join(download_dir, product_name)
        if os.path.exists(file_path):
            print(f"File {product_name} already exists, skipping...")
            continue
            
        with product.open() as fsrc, open(file_path, mode='wb') as fdst:
            shutil.copyfileobj(fsrc, fdst)
            print(f'Download of product {product_name} finished.')
            
    except eumdac.product.ProductError as error:
        print(f"Error related to the product: '{error}'")
    except requests.exceptions.RequestException as error:
        print(f"Unexpected error: {error}")
    except Exception as error:
        print(f"Unexpected error during download: {error}")


# Step 5 error
"""
Found 2 products
Product: S3A_OL_2_WFR____20160503T182657_20160503T182857_20210705T072114_0119_003_355______MAR_R_NT_003.SEN3
Available attributes: ['acronym', 'collection', 'cycle_number', 'datastore', 'entries', 'format', 'ingested', 'instrument', 'is_mtg', 'md5', 'metadata', 'open', 'orbit_direction', 'orbit_is_LEO', 'orbit_number', 'orbit_type', 'processingTime', 'processorVersion', 'product_type', 'qualityStatus', 'region_coverage', 'relative_orbit', 'repeat_cycle', 'satellite', 'sensing_end', 'sensing_start', 'size', 'subregion_identifier', 'timeliness', 'url']
--------------------------------------------------
Product: S3A_OL_2_WFR____20160501T173830_20160501T174030_20210704T233845_0119_003_326______MAR_R_NT_003.SEN3
Available attributes: ['acronym', 'collection', 'cycle_number', 'datastore', 'entries', 'format', 'ingested', 'instrument', 'is_mtg', 'md5', 'metadata', 'open', 'orbit_direction', 'orbit_is_LEO', 'orbit_number', 'orbit_type', 'processingTime', 'processorVersion', 'product_type', 'qualityStatus', 'region_coverage', 'relative_orbit', 'repeat_cycle', 'satellite', 'sensing_end', 'sensing_start', 'size', 'subregion_identifier', 'timeliness', 'url']
--------------------------------------------------

Downloading all 2 products...

Downloading product 1/2: S3A_OL_2_WFR____20160503T182657_20160503T182857_20210705T072114_0119_003_355______MAR_R_NT_003.SEN3
Error related to the product: 'Could not download Product S3A_OL_2_WFR____20160503T182657_20160503T182857_20210705T072114_0119_003_355______MAR_R_NT_003.SEN3 of Collection EO:EUM:DAT:0556 - Unauthorised (403)'

Downloading product 2/2: S3A_OL_2_WFR____20160501T173830_20160501T174030_20210704T233845_0119_003_326______MAR_R_NT_003.SEN3
Error related to the product: 'Could not download Product S3A_OL_2_WFR____20160501T173830_20160501T174030_20210704T233845_0119_003_326______MAR_R_NT_003.SEN3 of Collection EO:EUM:DAT:0556 - Unauthorised (403)'
"""
