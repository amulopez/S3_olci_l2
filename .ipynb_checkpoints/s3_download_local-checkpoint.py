#!/usr/bin/env python
# coding: utf-8

# In[ ]:

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
#   Credentials file formatted as a JSON stored in your home directory (Option 1: creating  .eumdac_credentials in home directory section of the tutorial)
# 
# USERS MUST EDIT 
# ROI polygon coordinates (southern California ROI used in this example script) ~lines 81-87
# download_dir  ~line 350
# batch_list    ~lines 353-366
"""

import os
import csv
import json
import datetime
import shutil
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
            "POLYGON((-121.343994 32.369016, -116.883545 32.369016, "
            "-116.883545 35.270920, -121.343994 35.270920, "
            "-121.343994 32.369016))"
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
                    print(f"✗ Failed: {str(error)[:50]}")
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
                        except:
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
    # MULTI-BATCH DOWNLOADER (NEW)
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
        except:
            return None

    def _process_filename(self, product_name):
        if product_name.endswith(".SEN3"):
            return product_name + ".zip"
        elif not product_name.endswith(".zip"):
            return product_name + ".SEN3.zip"
        return product_name


# ====================================================================
# EXAMPLE USAGE: DOWNLOAD USER DEFINED BATCHES
# ====================================================================

if __name__ == "__main__":

    downloader = S3Downloader(
        download_dir="/Users/lopezama/Documents/Blackwood/Palisades/s3_products"
    )

    batch_list = [
        {
            "name": "Jan5-15_2018",
            "start": "2018-01-04",  # Buffer by -1 day to ensure all data from date of interest are downloaded
            "end": "2018-01-16",    # Buffer by +1 day to ensure all data from date of interest are downloaded
            "collection": "0556"
        },
        {
            "name": "Jan24-Feb19_2025",
            "start": "2025-01-23",  # Buffer by -1 day to ensure all data from date of interest are downloaded
            "end": "2025-02-20",    # Buffer by +1 day to ensure all data from date of interest are downloaded
            "collection": "0407"
        }
    ]

    downloader.download_multiple_batches(batch_list)

