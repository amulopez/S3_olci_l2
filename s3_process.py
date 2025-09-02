#!/usr/bin/env python
# coding: utf-8

# In[ ]:

### WHAT THIS CODE DOES ###
# Contact: Mandy M. Lopez amanda.m.lopez@jpl.nasa.gov

# Step 1 Unzips Sentinel-3 Level 2 data .zip file, extracts .SEN3 folder with netCDFs, and deletes the .zip folder
# Step 2 Deletes unwanted .nc files and keeps specified .nc files. Prints out a list of what was deleted/kept. 
# Step 3 Creates a subfolder in the folder containing data downloads called "geotiff" and creates TSM .tif from the tsn_nn.nc and geo_coordinates.nc files 
#       for all data and stores georeferenced .tif in the "geotiff" folder
# Step 4A-D data formatting and stacking GeoTIFFs 
# Step 4E stack data and save as netCDF
# Step 4F option to clip stacked data using ROI shapefile and save as a separate netCDF
# Step 4G temporal aggregation defined by user with options to use unclipped and/or ROI-clipped data

###  EDIT BEFORE RUNNING  ###
# Step 1 base_directory = path to folder containing the .zip files downloaded from EUMETSAT Data Store
# Step 2 files_to_keep - confirm that the netCDFs you want are listed
# Step 3 verify the base_dir and output_dir paths
# Step 4B paths 
# Step 4G interval_choice = daily, weekly, monthly, seasonal, annual


import os
import zipfile
import shutil
from pathlib import Path
import numpy as np
import xarray as xr
from pyresample import geometry as geom
from pyresample import kd_tree as kdt
from osgeo import gdal, gdal_array, osr
from datetime import datetime
import warnings
import geopandas as gpd
import rioxarray

# -------------------------
# Step 1: unzips all .zip files in a directory then deletes the original .zip file
# -------------------------

def unzip_and_delete(directory):
    """Unzips all zip files in a directory and then deletes the original zip files.

    Args:
        directory: The path to the directory containing the zip files.
    """
    for filename in os.listdir(directory):
        if filename.endswith(".zip"):
            file_path = os.path.join(directory, filename)
            try:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(directory)
                os.remove(file_path)
                print(f"Unzipped and deleted: {filename}")
            except zipfile.BadZipFile:
                print(f"Skipping invalid zip file: {filename}")
            except Exception as e:
                 print(f"An error occurred processing {filename}: {e}")
                

# Example usage:
base_directory = "/nobackup/amulcan/data/s3/downloads"  # Path to directory with .ZIP files
unzip_and_delete(base_directory)


# -------------------------
# Step 2: Deletes netCDFs that are not needed, prints list of deleted and kept files
# -------------------------

# List of filenames to keep (modify as needed)
files_to_keep = [
    "cloud.nc", "common_flags.nc", "cqsf.nc", "geo_coordinates.nc", 
    "iop_nn.nc", "par.nc", "tie_geo_coordinates.nc", "tie_geometries.nc", 
    "time_coordinates.nc", "tsm_nn.nc", "trsp.nc", "wqsf.nc"
]

# Loop through all folders in the base directory
for folder_name in os.listdir(base_directory):
    folder_path = os.path.join(base_directory, folder_name)

    # Process only directories that end with ".SEN3"
    if os.path.isdir(folder_path) and folder_name.endswith(".SEN3"):
        print(f"Processing folder: {folder_name}")

        # Set this folder as the new base directory
        newbase_directory = folder_path

        # Loop through all files in the folder
        for file in os.listdir(newbase_directory):
            file_path = os.path.join(newbase_directory, file)

            # Delete the file if it's not in the keep list
            if os.path.isfile(file_path) and file not in files_to_keep:
                os.remove(file_path)
                print(f"Deleted: {file_path}")
            else:
                print(f"Kept: {file_path}")

        # Change the working directory to the newly set base directory
        os.chdir(newbase_directory)
        print(f"New working directory: {os.getcwd()}")

print("Cleanup process complete.")

# -------------------------
# Step 3: Make GeoTIFF from swath
# -------------------------

# Define function to create GeoTIFF from swath data
def create_geotiff_from_swath(tsm_nc_path, geo_nc_path, output_path, res_deg=0.0027):
    tsm_ds = xr.open_dataset(tsm_nc_path)
    geo_ds = xr.open_dataset(geo_nc_path)

    tsm = tsm_ds["TSM_NN"].values.squeeze()
    lat = geo_ds["latitude"].values
    lon = geo_ds["longitude"].values

    mask = np.isfinite(tsm) & np.isfinite(lat) & np.isfinite(lon)
    if np.sum(mask) == 0:
        print(f"❌ No valid TSM pixels found in {tsm_nc_path}")
        return

    swath_def = geom.SwathDefinition(lons=lon, lats=lat)
    lat_min, lat_max = np.nanmin(lat), np.nanmax(lat)
    lon_min, lon_max = np.nanmin(lon), np.nanmax(lon)

    ref_lats = np.arange(lat_min, lat_max, res_deg)
    ref_lons = np.arange(lon_min, lon_max, res_deg)
    cols, rows = len(ref_lons), len(ref_lats)

    area_extent = (lon_min, lat_min, lon_max, lat_max)
    area_def = geom.AreaDefinition(
        "area_id", "MERIS Grid", "latlon",
        {'proj': 'longlat', 'datum': 'WGS84'},
        cols, rows, area_extent
    )

    index, outdex, index_array, dist_array = kdt.get_neighbour_info(
        swath_def, area_def, radius_of_influence=5000, neighbours=1
    )

    grid = kdt.get_sample_from_neighbour_info(
        'nn', area_def.shape, tsm, index, outdex, index_array, fill_value=np.nan
    )

    driver = gdal.GetDriverByName("GTiff")
    dataset = driver.Create(
        str(output_path), cols, rows, 1,
        gdal_array.NumericTypeCodeToGDALTypeCode(grid.dtype)
    )

    pixel_size_x = (lon_max - lon_min) / cols
    pixel_size_y = (lat_max - lat_min) / rows
    transform = [lon_min, pixel_size_x, 0, lat_max, 0, -pixel_size_y]
    dataset.SetGeoTransform(transform)

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    dataset.SetProjection(srs.ExportToWkt())

    band = dataset.GetRasterBand(1)
    band.WriteArray(grid)
    band.SetNoDataValue(np.nan)
    band.FlushCache()

    dataset = None
    print(f"Saved properly geolocated GeoTIFF: {output_path}")


# Set base directory and output directory
base_dir = Path("/nobackup/amulcan/data/s3/processed")
output_dir = base_dir / "geotiff"
output_dir.mkdir(exist_ok=True)

# Loop through each subfolder
for subfolder in base_dir.iterdir():
    if subfolder.is_dir():
        tsm_path = subfolder / "tsm_nn.nc"
        geo_path = subfolder / "geo_coordinates.nc"

        if tsm_path.exists() and geo_path.exists():
            output_filename = f"TSM_{subfolder.name}.tif"
            output_path = output_dir / output_filename
            print(f"\n📂 Processing folder: {subfolder.name}")
            create_geotiff_from_swath(tsm_path, geo_path, output_path)
        else:
            print(f"\n⏩ Skipping folder: {subfolder.name} (required files not found)")


warnings.filterwarnings("ignore")

# -------------------------
# Step 4A: Extract datetime from filename
# -------------------------
def extract_datetime_from_filename(filepath):
    """Extract datetime from filename of format TSM_..._YYYYMMDDTHHMMSS_..."""
    stem = filepath.stem
    try:
        for part in stem.split("_"):
            if len(part) == 15 and part.startswith("20"):
                return datetime.strptime(part, "%Y%m%dT%H%M%S")
    except ValueError:
        pass
    print(f"❌ Could not extract timestamp from {filepath.name}")
    return None


# -------------------------
# Step 4B: Define paths
# -------------------------
# Directory containing geotiffs
geotiff_dir = Path("/nobackup/amulcan/data/s3/processed/geotiff")

# Directory where you want the stacked netCDFs to be saved
stacked_dir = Path("/nobackup/amulcan/data/s3/processed/stacked_nc")
stacked_dir.mkdir(parents=True, exist_ok=True)

# Directory where you want to save the stacked unclipped netCDF & include name for the stacked unclipped netCDF
unclipped_nc_path = stacked_dir / "stacked_tsm_unclipped.nc"

# Directory where you want to save the stacked ROI-clipped netCDF & include name for the stacked ROI-clipped netCDF
clipped_nc_path = stacked_dir / "clipped" / "stacked_tsm_clipped.nc"
clipped_nc_path.parent.mkdir(parents=True, exist_ok=True)

# Directory where ROI shapefile is located
roi_shape = '/nobackup/amulcan/roi/south_ca/south_ca_coast_10km.shp'

# -------------------------
# Step 4C: Find all GeoTIFF files
# -------------------------
files = sorted(geotiff_dir.glob("TSM_*.tif"))
if not files:
    raise FileNotFoundError("No matching TSM_*.tif files found.")

# -------------------------
# Step 4D: Load GeoTIFFs as DataArrays
# -------------------------
dataarrays = []
for f in files:
    print(f"Processing: {f.name}")
    timestamp = extract_datetime_from_filename(f)
    if timestamp is None:
        continue
    try:
        da = rioxarray.open_rasterio(f).squeeze()
        da = da.expand_dims(time=[timestamp])
        dataarrays.append(da)
    except Exception as e:
        print(f"⚠️ Skipping {f.name} due to error: {e}")

# -------------------------
# Step 4E: Stack into single dataset
# -------------------------
stacked = xr.concat(dataarrays, dim="time")
stacked.name = "TSM"
stacked.to_netcdf(unclipped_nc_path)
print(f"Saved UNCLIPPED stack: {unclipped_nc_path}")

# -------------------------
# Step 4F: Clip to ROI and save
# -------------------------
def clip_stack_to_shapefile(stacked, shapefile_path):
    shp = gpd.read_file(shapefile_path)
    shp = shp.to_crs(stacked.rio.crs)
    return stacked.rio.clip(shp.geometry.values, shp.crs, drop=True)

stack_clip = clip_stack_to_shapefile(stacked, roi_shape)
stack_clip.to_netcdf(clipped_nc_path)
print(f"Saved CLIPPED stack: {clipped_nc_path}")

# -------------------------
# Step 4G: Aggregation Function
# -------------------------
def aggregate_stack(ds, interval="daily", output_prefix="TSM"):
    times = ds["time"].to_index()  # Convert to Pandas DatetimeIndex

    if interval == "daily":
        ds.coords["period"] = ("time", times.floor("D"))
    elif interval == "weekly":
        ds.coords["period"] = ("time", times.to_period("W").start_time)
    elif interval == "monthly":
        ds.coords["period"] = ("time", times.to_period("M").start_time)
    elif interval == "annual":
        ds.coords["period"] = ("time", times.to_period("Y").start_time)
    elif interval == "seasonal":
        def get_season(month):
            if month in [12, 1, 2]:
                return "Winter"
            elif month in [3, 4, 5]:
                return "Spring"
            elif month in [6, 7, 8]:
                return "Summer"
            else:
                return "Fall"
        ds.coords["period"] = ("time", [get_season(m) for m in times.month])
    else:
        raise ValueError("Invalid interval. Choose daily, weekly, monthly, seasonal, annual.")

    agg = ds.groupby("period").mean(dim="time", skipna=True)
    agg.name = f"{output_prefix}_{interval}"
    return agg


# -------------------------
# Specify aggregation interval here
# -------------------------
interval_choice = "daily"  # Options: daily, weekly, monthly, seasonal, annual

# -------------------------
# Aggregate UNCLIPPED
# -------------------------
agg_unclipped = aggregate_stack(stacked, interval=interval_choice, output_prefix="TSM")
agg_unclipped_path = stacked_dir / f"{interval_choice}_unclipped.nc"
agg_unclipped.to_netcdf(agg_unclipped_path)
print(f"Saved {interval_choice.upper()} temporal composite UNCLIPPED file: {agg_unclipped_path}")

# -------------------------
# Aggregate CLIPPED
# -------------------------
agg_clipped = aggregate_stack(stack_clip, interval=interval_choice, output_prefix="TSM")
agg_clipped_path = stacked_dir / "clipped" / f"{interval_choice}_clipped.nc"
agg_clipped.to_netcdf(agg_clipped_path)
print(f"Saved {interval_choice.upper()} temporal composite CLIPPED file: {agg_clipped_path}")
