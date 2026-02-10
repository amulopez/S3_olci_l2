#!/usr/bin/env python
# coding: utf-8

"""
# Sentinel-3 OLCI Data Downloader - Local Computer Version
# Contact: Mandy M. Lopez amanda.m.lopez@jpl.nasa.gov

# Queries and downloads Sentinel-3 OLCI Level 2 Ocean Colour data collections from EUMETSAT Data Store
# Recommend batching downloads by year to avoid hitting data download limit errors if working with multi-year datasets
# As of August 2025 the full data record is split between two collections:
#   EO:EUM:DAT:0556 04-25-16 to 04-28-21
#   EO:EUM:DAT:0407 04-29-21 to 07-30-25
#
# BEFORE USING
# Users must have EUMDAC credentials set up before using this script, see resources below for assistance with this--
#   Tutorial https://gitlab.eumetsat.int/eumetlab/data-services/eumdac_data_store/-/blob/master/1_4_Sentinel3_data_access.ipynb
#   EUMDAC package installed https://user.eumetsat.int/resources/user-guides/eumetsat-data-access-client-eumdac-guide
#   Credentials file formatted as a JSON stored in your home directory (Option 1: creating .eumdac_credentials in home directory section of the tutorial)
#
# CLI USAGE (multi-batch; local computers)
#   python3.9 s3_download_local.py --download_dir "/path/to/s3_products" \
#       --bbox MIN_LON MIN_LAT MAX_LON MAX_LAT \
#       --batch "Name,YYYY-MM-DD,YYYY-MM-DD,0407" \
#       --batch "Name2,YYYY-MM-DD,YYYY-MM-DD,0556"
#
# ROI OPTIONS
#   --bbox MIN_LON MIN_LAT MAX_LON MAX_LAT   (auto-converted to WKT polygon)
#   --roi_wkt "POLYGON((lon lat, lon lat, ...))"
#
# NOTES
# - If ROI is not provided, the script uses the default ROI in S3Downloader (Southern California example).
# - Each --batch entry is parsed as: "name,start,end,collection" where collection is optional (defaults to 0407).
"""

import os
import csv
import json
import datetime
import shutil
import argparse
import eumdac


# ====================================================================
# S3Downloader CLASS
# ====================================================================
class S3Downloader:
    def __init__(self, download_dir=None, logs_dir=None):
        """Initialize the downloader with directories"""

        # =======================
        # Setup directories
        # =======================
        if download_dir is None:
            download_dir = os.path.join(os.getcwd(), "s3_products")
        if logs_dir is None:
            logs_dir = os.path.join(download_dir, "logs")

        self.download_dir = download_dir
        self.logs_dir = logs_dir

        os.makedirs(self.download_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)

        # =======================
        # Load credentials
        # =======================
        cred_path = os.path.join(os.path.expanduser("~"), ".eumdac", "credentials")
        with open(cred_path) as json_file:
            credentials = json.load(json_file)
            self.token = eumdac.AccessToken(
                (credentials["consumer_key"], credentials["consumer_secret"])
            )

        print(f"✓ Token expires: {self.token.expiration}")

        # Create DataStore object
        self.datastore = eumdac.DataStore(self.token)

        # =======================
        # Collection IDs
        # =======================
        self.collection_ids = {
            "0407": "EO:EUM:DAT:0407",  # Current (2021-present)
            "0556": "EO:EUM:DAT:0556",  # Reprocessed (2016-2021)
        }

        # =======================
        # Default ROI (Southern California)
        # =======================
        self.roi = (
            "POLYGON((-119.457859 32.369016, -116.883545 32.369016, "
            "-116.883545 35.270920, -119.457859 35.270920, "
            "-119.457859 32.369016))"
        )

    # ====================================================================
    # MAIN SINGLE-BATCH DOWNLOAD METHOD
    # ====================================================================
    def download_batch(self, start_date, end_date, collection="0407",
                       batch_name=None, roi=None):
        """Download a batch of Sentinel-3 OLCI products"""

        # Parse dates
        if isinstance(start_date, str):
            start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

        # Assign batch name
        if batch_name is None:
            batch_name = (
                f"batch_{start_date.strftime('%Y%m%d')}_"
                f"{end_date.strftime('%Y%m%d')}"
            )

        # Use default ROI if not supplied
        if roi is None:
            roi = self.roi

        # Collection
        collection_id = self.collection_ids.get(collection, collection)

        print("\n" + "=" * 60)
        print(f"Downloading batch: {batch_name}")
        print(f"Collection: {collection_id}")
        print(f"Date range: {start_date.date()} to {end_date.date()}")
        print("=" * 60 + "\n")

        # Create log file
        log_file = self._create_log_file(batch_name)

        try:
            collection_obj = self.datastore.get_collection(collection_id)
            products = collection_obj.search(
                geo=roi,
                dtstart=start_date,
                dtend=end_date,
            )

            product_list = list(products)
            total_products = len(product_list)

            print(f"Found {total_products} products\n")

            if total_products == 0:
                print("⚠ No products found.")
                return {
                    "total": 0, "downloaded": 0,
                    "skipped": 0, "failed": 0
                }

            stats = {
                "total": total_products,
                "downloaded": 0,
                "skipped": 0,
                "failed": 0,
            }

            # ---------------------------------------------------
            # DOWNLOAD LOOP
            # ---------------------------------------------------
            for i, product in enumerate(product_list, 1):
                product_name = str(product)
                zip_filename = self._process_filename(product_name)
                file_path = os.path.join(self.download_dir, zip_filename)

                print(f"[{i}/{total_products}] {zip_filename[:60]} ...", end=" ")

                # Already downloaded?
                if os.path.exists(file_path):
                    file_size = self._get_file_size_mb(file_path)
                    print(f"✓ Already exists ({file_size} MB)")
                    self._log_result(
                        log_file, batch_name, product_name,
                        zip_filename, "SKIPPED", file_size
                    )
                    stats["skipped"] += 1
                    continue

                # Download file
                download_start = datetime.datetime.now()
                try:
                    with product.open() as fsrc, open(file_path, "wb") as fdst:
                        shutil.copyfileobj(fsrc, fdst)

                    # Compute stats
                    dl_time = (datetime.datetime.now() - download_start).total_seconds()
                    file_size = self._get_file_size_mb(file_path)

                    print(f"✓ Downloaded ({file_size} MB, {dl_time:.1f}s)")
                    self._log_result(
                        log_file, batch_name, product_name,
                        zip_filename, "SUCCESS",
                        file_size, download_time=dl_time
                    )
                    stats["downloaded"] += 1

                except Exception as error:
                    dl_time = (datetime.datetime.now() - download_start).total_seconds()
                    #print(f"✗ Failed: {str(error)[:50]}")
                    print(f"✗ Failed: {error}")
                    self._log_result(
                        log_file, batch_name, product_name,
                        zip_filename, "FAILED",
                        error_message=str(error),
                        download_time=dl_time
                    )
                    stats["failed"] += 1

                    # Cleanup partial
                    if os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except Exception:
                            pass

            # ---------------------------------------------------
            # Batch summary
            # ---------------------------------------------------
            print("\n" + "=" * 60)
            print("DOWNLOAD SUMMARY")
            print("=" * 60)
            print(f"Total:       {stats['total']}")
            print(f"Downloaded:  {stats['downloaded']}")
            print(f"Skipped:     {stats['skipped']}")
            print(f"Failed:      {stats['failed']}")
            print("=" * 60)
            print(f"Files saved to: {self.download_dir}")
            print(f"Log file: {log_file}\n")

            return stats

        except Exception as error:
            print(f"✗ Error: {error}")
            return None

    # ====================================================================
    # MULTI-BATCH DOWNLOADER
    # ====================================================================
    def download_multiple_batches(self, batch_list):
        """
        Run multiple batch downloads sequentially. Example format:

        batch_list = [
            {'start': '2024-12-01', 'end': '2024-12-31', 'collection': '0407', 'name': 'Batch_A'},
            {'start': '2016-04-25', 'end': '2016-12-31', 'collection': '0556', 'name': 'Batch_B'},
        ]
        """

        all_stats = []

        print("\n============================================================")
        print(" Running MULTI-BATCH Sentinel-3 Download ")
        print("============================================================\n")

        for batch in batch_list:
            start = batch["start"]
            end = batch["end"]
            collection = batch.get("collection", "0407")
            batch_name = batch.get("name", None)
            roi = batch.get("roi", None)

            print(f"\n🚀 Starting batch: {batch_name}\n")

            stats = self.download_batch(
                start_date=start,
                end_date=end,
                collection=collection,
                batch_name=batch_name,
                roi=roi
            )
            all_stats.append((batch_name, stats))

        # ---------------------------------------------------
        # PRINT FINAL SUMMARY TABLE
        # ---------------------------------------------------
        print("\n\n============================================================")
        print(" FINAL MULTI-BATCH SUMMARY ")
        print("============================================================")
        print(f"{'Batch Name':28} | Total | Downloaded | Skipped | Failed")
        print("-" * 75)

        for batch_name, s in all_stats:
            if s is None:
                print(f"{batch_name:28} |  ERR  |   ERR     |   ERR   |  ERR")
                continue

            print(
                f"{batch_name:28} |"
                f" {s['total']:5} |"
                f" {s['downloaded']:10} |"
                f" {s['skipped']:7} |"
                f" {s['failed']:6}"
            )

        print("\nAll downloads complete.\n")

        return all_stats

    # ====================================================================
    # HELPERS
    # ====================================================================
    def _create_log_file(self, batch_name):
        log_file = os.path.join(self.logs_dir, f"{batch_name}_download_log.csv")
        if not os.path.exists(log_file):
            with open(log_file, "w", newline="", encoding="utf-8") as csvfile:
                fieldnames = [
                    "timestamp", "batch_name", "product_name",
                    "zip_filename", "status", "file_size_mb",
                    "error_message", "download_time_seconds"
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
        return log_file

    def _log_result(self, log_file, batch_name, product_name,
                    zip_filename, status, file_size_mb=None,
                    error_message=None, download_time=None):
        with open(log_file, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=[
                "timestamp", "batch_name", "product_name",
                "zip_filename", "status", "file_size_mb",
                "error_message", "download_time_seconds"
            ])
            writer.writerow({
                "timestamp": datetime.datetime.now().isoformat(),
                "batch_name": batch_name,
                "product_name": product_name,
                "zip_filename": zip_filename,
                "status": status,
                "file_size_mb": file_size_mb,
                "error_message": error_message,
                "download_time_seconds": download_time
            })

    def _get_file_size_mb(self, file_path):
        try:
            size_bytes = os.path.getsize(file_path)
            return round(size_bytes / (1024 * 1024), 2)
        except Exception:
            return None

    def _process_filename(self, product_name):
        if product_name.endswith(".SEN3"):
            return product_name + ".zip"
        elif not product_name.endswith(".zip"):
            return product_name + ".SEN3.zip"
        return product_name


# ====================================================================
# CLI ENTRYPOINT (LOCAL MULTI-BATCH)
# ====================================================================
def make_batch_name(start, end):
    """
    Generate batch name as %b%d-%d_%Y
    Example: 2018-01-04 to 2018-01-16 -> Jan04-16_2018
    """
    start_dt = datetime.datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.datetime.strptime(end, "%Y-%m-%d")

    return (
        f"{start_dt.strftime('%b')}"
        f"{start_dt.strftime('%d')}-"
        f"{end_dt.strftime('%d')}_"
        f"{end_dt.strftime('%Y')}"
    )

    
def bbox_to_wkt(min_lon, min_lat, max_lon, max_lat):
    return (
        f"POLYGON(({min_lon} {min_lat}, {max_lon} {min_lat}, "
        f"{max_lon} {max_lat}, {min_lon} {max_lat}, "
        f"{min_lon} {min_lat}))"
    )


def parse_batch_arg(batch_str):
    """
    Parse a --batch string.

    Accepted formats:
      "Name,YYYY-MM-DD,YYYY-MM-DD"
      "Name,YYYY-MM-DD,YYYY-MM-DD,0407"
      "YYYY-MM-DD,YYYY-MM-DD"
      "YYYY-MM-DD,YYYY-MM-DD,0407"

    If name is omitted, it is auto-generated as %b%d-%d_%Y
    """
    parts = [p.strip() for p in batch_str.split(",")]

    if len(parts) not in (2, 3, 4):
        raise ValueError(
            f'Invalid --batch "{batch_str}". Use either:\n'
            f'  "Name,YYYY-MM-DD,YYYY-MM-DD[,collection]" OR\n'
            f'  "YYYY-MM-DD,YYYY-MM-DD[,collection]"'
        )

    # Detect whether a name was supplied
    if parts[0][0].isdigit():
        # No name provided
        start = parts[0]
        end = parts[1]
        collection = parts[2] if len(parts) == 3 else "0407"
        name = make_batch_name(start, end)
    else:
        # Name provided
        name = parts[0]
        start = parts[1]
        end = parts[2]
        collection = parts[3] if len(parts) == 4 else "0407"

    # Validate dates
    datetime.datetime.strptime(start, "%Y-%m-%d")
    datetime.datetime.strptime(end, "%Y-%m-%d")

    return {
        "name": name,
        "start": start,
        "end": end,
        "collection": collection
    }
    


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Download Sentinel-3 OLCI Level-2 products from EUMETSAT (multi-batch local downloader)"
    )

    parser.add_argument(
        "--download_dir",
        type=str,
        required=True,
        help="Directory where Sentinel-3 products will be downloaded"
    )

    # ROI options: either bbox or raw WKT
    roi_group = parser.add_mutually_exclusive_group(required=False)

    roi_group.add_argument(
        "--roi_wkt",
        type=str,
        default=None,
        help='ROI as WKT polygon, e.g. "POLYGON((lon lat, lon lat, ...))"'
    )

    roi_group.add_argument(
        "--bbox",
        nargs=4,
        type=float,
        metavar=("MIN_LON", "MIN_LAT", "MAX_LON", "MAX_LAT"),
        default=None,
        help="ROI bounding box as min_lon min_lat max_lon max_lat"
    )

    parser.add_argument(
        "--batch",
        action="append",
        required=True,
        help='Repeatable. Format: "Name,YYYY-MM-DD,YYYY-MM-DD,0407" (collection optional; defaults to 0407).'
    )

    args = parser.parse_args()

    # Determine ROI override (if any). If None, S3Downloader default ROI is used.
    roi = None
    if args.roi_wkt:
        roi = args.roi_wkt
    elif args.bbox:
        min_lon, min_lat, max_lon, max_lat = args.bbox
        roi = bbox_to_wkt(min_lon, min_lat, max_lon, max_lat)

    # Build batch_list
    batch_list = []
    for b in args.batch:
        batch = parse_batch_arg(b)
        if roi:
            batch["roi"] = roi
        batch_list.append(batch)

    downloader = S3Downloader(download_dir=args.download_dir)
    downloader.download_multiple_batches(batch_list)
