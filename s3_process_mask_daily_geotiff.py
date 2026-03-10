#!/usr/bin/env python
# coding: utf-8

# ==============================================================================
# TSM PROCESSING WORKFLOW: FROM RAW DATA TO DAILY COMPOSITES (WITH MASKING)
# ==============================================================================
# 
# This workflow processes Sentinel-3 OLCI ocean color products to create quality flag masked, daily
# Total Suspended Matter (TSM) composite rasters clipped to a region of interest
#
# ------------------
# WORKFLOW OVERVIEW
# ------------------
# Step 1: Unzip and DELETE original zip file downloads (USERS ADVISED TO SAVE BACKUP ZIP DATA)
# Step 2: Keeps specified netCDF files and deletes the rest
# Step 3: Applies data quality flags to mask poor quality/unusable pixels
# Step 4: Convert netCDF swath data to georeferenced GeoTIFF rasters
# Step 5: Reclassify negative TSM values to zero
# Step 6: Clip rasters to Region of Interest (ROI) using shapefile
# Step 7: Create daily composite rasters (if multiple passes per day, otherwise single date acquistion retain unchanged)
# 
# -----------------------------
# REQUIRED USER CONFIGURATION
# -----------------------------
# Before running, users MUST edit the following paths/parameters:
# 
# In the USER CONFIGURATION section
#   Step 1 base_directory - path to folder containing zip files
#   Step 2 files_to_keep - list of netCDF variables to retain >> USER SPECIFIES
#   Step 3 custom_flags - list of data qulaity flags to be used to mask TSM data >> USER SPECIFIES
#   Step 6 roi_shape - path to ROI shapefile for clipping
#
# ------------------
# OUTPUT STRUCTURE
# ------------------
# BASE_DIRECTORY
# ├── *.SEN3
# ├── nc_mask
# ├── geotiff
# ├── geotiff_reclassified
# └── geotiff_reclass_clipped
#      └── daily_composites
#
# --------------------------------------------------------------------------------

import os
import re
import glob
import zipfile
from pathlib import Path
import numpy as np
import xarray as xr
from pyresample import geometry as geom
from pyresample import kd_tree as kdt
from osgeo import gdal, gdal_array, osr
import warnings
import geopandas as gpd
import rioxarray
import rasterio
from rasterio.merge import merge
from rasterio.warp import reproject
import eumartools

warnings.filterwarnings("ignore")

# ==============================================================================
# USER CONFIGURATION
# ==============================================================================

BASE_DIRECTORY = "/Users/lopezama/Documents/Blackwood/S3/s3_workflow_tests/github_test/data"

ROI_SHAPEFILE = ("/Users/lopezama/Documents/Blackwood/shapefiles/huc10_coastline_buffer/huc10_coastline_buffer.shp")

FILES_TO_KEEP = [
    "cloud.nc",
    "common_flags.nc",
    "cqsf.nc",
    "geo_coordinates.nc",
    "iop_nn.nc",
    "par.nc",
    "tie_geo_coordinates.nc",
    "tie_geometries.nc",
    "time_coordinates.nc",
    "tsm_nn.nc",
    "trsp.nc",
    "wqsf.nc",
]

OUTPUT_RES_DEG = 0.0027

# Quality flags to be used from the wqsf.nc file
CUSTOM_FLAGS = [
    'LAND', 'CLOUD', 'CLOUD_AMBIGUOUS', 'CLOUD_MARGIN',
    'INVALID', 'COSMETIC', 'SATURATED', 'SUSPECT',
    'HISOLZEN', 'HIGHGLINT', 'SNOW_ICE'
]


# ==============================================================================
# STEP 1: UNZIP FILES
# ==============================================================================

def unzip_and_delete(directory):
    zip_count = 0

    for filename in os.listdir(directory):

        if filename.endswith(".zip"):

            file_path = os.path.join(directory, filename)

            try:
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    zip_ref.extractall(directory)

                os.remove(file_path)

                zip_count += 1

                print(f"✓ Unzipped: {filename}")

            except zipfile.BadZipFile:
                print(f"⚠ Bad zip file: {filename}")

    print(f"STEP 1 COMPLETE: {zip_count} files unzipped\n")


# ==============================================================================
# STEP 2: CLEAN NETCDF FILES
# ==============================================================================

def cleanup_netcdf_files(base_directory):

    deleted = 0
    kept = 0

    for folder in os.listdir(base_directory):

        folder_path = os.path.join(base_directory, folder)

        if folder.endswith(".SEN3") and os.path.isdir(folder_path):

            print(f"📂 {folder}")

            for f in os.listdir(folder_path):

                fp = os.path.join(folder_path, f)

                if os.path.isfile(fp) and f not in FILES_TO_KEEP:

                    os.remove(fp)

                    deleted += 1

                elif os.path.isfile(fp):

                    kept += 1

    print(f"STEP 2 COMPLETE: Deleted {deleted}, kept {kept}\n")


# ==============================================================================
# STEP 3: APPLY DATA QUALITY FLAG MASK
# ==============================================================================

def apply_tsm_mask(tsm_path, flag_path, output_path, flag_list):

    try:

        tsm_ds = xr.open_dataset(tsm_path)

        tsm_data = tsm_ds["TSM_NN"].values

        quality_mask = eumartools.flag_mask(str(flag_path), 'WQSF', flag_list)

        masked = tsm_data.copy()

        masked[quality_mask] = np.nan

        masked_ds = tsm_ds.copy(deep=True)

        masked_ds["TSM_NN"].values = masked

        masked_ds["TSM_NN"].attrs['quality_flags_applied'] = ", ".join(flag_list)

        output_path.parent.mkdir(exist_ok=True)

        masked_ds.to_netcdf(output_path)

        masked_ds.close()

        tsm_ds.close()

        print(f"✓ Masked saved: {output_path.name}")

        return True

    except Exception as e:

        print(f"Masking failed for {tsm_path.name}: {e}")

        return False


# ==============================================================================
# STEP 4: SWATH TO GEOTIFF
# ==============================================================================

def create_geotiff_from_swath(tsm_nc, geo_nc, out_tif, res_deg):

    tsm_ds = xr.open_dataset(tsm_nc)

    geo_ds = xr.open_dataset(geo_nc)

    tsm = tsm_ds["TSM_NN"].values.squeeze()

    lat = geo_ds["latitude"].values

    lon = geo_ds["longitude"].values

    mask = np.isfinite(tsm) & np.isfinite(lat) & np.isfinite(lon)

    if np.sum(mask) == 0:

        print("   No valid pixels")

        return

    swath = geom.SwathDefinition(lons=lon, lats=lat)

    lat_min, lat_max = np.nanmin(lat), np.nanmax(lat)

    lon_min, lon_max = np.nanmin(lon), np.nanmax(lon)

    ref_lats = np.arange(lat_min, lat_max, res_deg)

    ref_lons = np.arange(lon_min, lon_max, res_deg)

    area_def = geom.AreaDefinition(
        "grid",
        "OLCI Grid",
        "latlon",
        {"proj": "longlat", "datum": "WGS84"},
        len(ref_lons),
        len(ref_lats),
        (lon_min, lat_min, lon_max, lat_max),
    )

    idx, oidx, idx_arr, _ = kdt.get_neighbour_info(
        swath, area_def, radius_of_influence=5000, neighbours=1
    )

    grid = kdt.get_sample_from_neighbour_info(
        "nn",
        area_def.shape,
        tsm,
        idx,
        oidx,
        idx_arr,
        fill_value=np.nan,
    )

    driver = gdal.GetDriverByName("GTiff")

    ds = driver.Create(
        str(out_tif),
        grid.shape[1],
        grid.shape[0],
        1,
        gdal_array.NumericTypeCodeToGDALTypeCode(grid.dtype),
    )

    ds.SetGeoTransform([
        lon_min,
        (lon_max - lon_min) / grid.shape[1],
        0,
        lat_max,
        0,
        -(lat_max - lat_min) / grid.shape[0],
    ])

    srs = osr.SpatialReference()

    srs.ImportFromEPSG(4326)

    ds.SetProjection(srs.ExportToWkt())

    band = ds.GetRasterBand(1)

    band.WriteArray(grid)

    band.SetNoDataValue(np.nan)

    ds = None

    print(f"✓ Saved {out_tif.name}")


# ==============================================================================
# STEP 5: RECLASSIFY NEGATIVE VALUES
# ==============================================================================

def reclassify_negative_to_zero(in_tif, out_tif):

    with rasterio.open(in_tif) as src:

        data = src.read(1)

        meta = src.meta.copy()

    data = np.where((data < 0) & ~np.isnan(data), 0, data)

    with rasterio.open(out_tif, "w", **meta) as dst:

        dst.write(data, 1)


# ==============================================================================
# STEP 6: CLIP TO ROI
# ==============================================================================

def clip_geotiff(geotiff, shapefile, output):

    roi = gpd.read_file(shapefile)

    raster = rioxarray.open_rasterio(geotiff, masked=True)

    if raster.rio.crs != roi.crs:

        roi = roi.to_crs(raster.rio.crs)

    clipped = raster.rio.clip(roi.geometry, roi.crs)

    clipped.rio.to_raster(output, compress="lzw")


# ==============================================================================
# STEP 7: DAILY COMPOSITES
# ==============================================================================

def merge_and_average(files):

    srcs = [rasterio.open(f) for f in files]

    merged, transform = merge(srcs, method="first")

    stack = []

    for src in srcs:

        arr = src.read(1)

        reproj = np.empty_like(merged[0], dtype=np.float32)

        reproject(
            arr,
            reproj,
            src_transform=src.transform,
            src_crs=src.crs,
            dst_transform=transform,
            dst_crs=src.crs,
            dst_nodata=np.nan,
        )

        stack.append(reproj)

    for s in srcs:

        s.close()

    return np.nanmean(np.stack(stack), axis=0), transform, srcs[0].meta.copy()


# ==============================================================================
# MAIN PIPELINE
# ==============================================================================

def main():

    base_dir = Path(BASE_DIRECTORY)

    print("\nSTEP 1: UNZIP FILES")

    unzip_and_delete(BASE_DIRECTORY)

    print("STEP 2: CLEAN UP NETCDF FILES")

    cleanup_netcdf_files(BASE_DIRECTORY)


    # STEP 3 MASK
    print("STEP 3: APPLY QUALITY MASK")

    mask_dir = base_dir / "nc_mask"

    mask_dir.mkdir(exist_ok=True)

    for sen3 in base_dir.glob("*.SEN3"):

        tsm = sen3 / "tsm_nn.nc"

        flags = sen3 / "wqsf.nc"

        if tsm.exists() and flags.exists():

            out = mask_dir / f"{sen3.name}_tsm_masked.nc"

            apply_tsm_mask(tsm, flags, out, CUSTOM_FLAGS)


    # STEP 4 GEOTIFF
    print("STEP 4: CREATE GEOTIFFS")

    geotiff_dir = base_dir / "geotiff"

    geotiff_dir.mkdir(exist_ok=True)

    for sen3 in base_dir.glob("*.SEN3"):

        masked = mask_dir / f"{sen3.name}_tsm_masked.nc"

        geo = sen3 / "geo_coordinates.nc"

        if masked.exists() and geo.exists():

            out = geotiff_dir / f"TSM_{sen3.name}.tif"

            create_geotiff_from_swath(masked, geo, out, OUTPUT_RES_DEG)


    # STEP 5 RECLASS
    print("STEP 5: RECLASSIFY NEGATIVE TSM VALUES")

    recl_dir = base_dir / "geotiff_reclassified"

    recl_dir.mkdir(exist_ok=True)

    for tif in geotiff_dir.glob("*.tif"):

        reclassify_negative_to_zero(tif, recl_dir / tif.name)


    # STEP 6 CLIP
    print("STEP 6: CLIP GEOTIFFS")

    clip_dir = base_dir / "geotiff_reclass_clipped"

    clip_dir.mkdir(exist_ok=True)

    for tif in recl_dir.glob("*.tif"):

        clip_geotiff(tif, ROI_SHAPEFILE, clip_dir / tif.name)


    # STEP 7 DAILY COMPOSITES
    print("STEP 7: MAKE DAILY COMPOSITES")

    composite_dir = clip_dir / "daily_composites"

    composite_dir.mkdir(exist_ok=True)

    files = glob.glob(str(clip_dir / "*.tif"))

    pattern = re.compile(r"(\d{8})")

    grouped = {}

    for f in files:

        m = pattern.search(os.path.basename(f))

        if m:

            grouped.setdefault(m.group(1), []).append(f)

    for date, flist in grouped.items():

        arr, trans, meta = merge_and_average(flist)

        meta.update(
            height=arr.shape[0],
            width=arr.shape[1],
            transform=trans,
            nodata=np.nan,
            count=1,
        )

        out = composite_dir / f"TSM_daily_{date}.tif"

        with rasterio.open(out, "w", **meta) as dst:

            dst.write(arr, 1)

    print("\nPIPELINE COMPLETE ✔")


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    main()