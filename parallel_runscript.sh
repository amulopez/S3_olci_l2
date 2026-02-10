#!/bin/bash
#PBS -N s3_olci_parallel_test
#PBS -q s3_parallel_test
#PBS -l select=1:ncpus=2
#PBS -l walltime=01:00:00
#PBS -j oe

echo "Start time: $(date)"

# If any command returns a non-zero exit code, the script stops immediately.
set -euo pipefail

# Lowest General directory. Users should update this to their own environment
DIR="/users/bdmor/Documents"

# Paths (absolute, derived from DIR)
BATCH_LIST="$DIR/S3_olci_l2/batch_list.txt"
PY_SCRIPT="$DIR/S3_olci_l2/s3_download_local_CL.py"
DOWN_DIR="$DIR/s3_products"
ROI="-116.883545,32.369016,-119.457859,35.270920"

# Use the number of CPUs PBS actually allocated
NCPUS=$(wc -l < "$PBS_NODEFILE")
echo "Detected NCPUS: $NCPUS"

# If GNU Parallel is provided via modules on your system, uncomment:
#module load parallel

# NOTE1: -j "$NCPUS tells parallel how many processors to use with Parallel
# NOTE2: the "{}" is a Parallel iterator that loops through each line of $BATCH_LIST
parallel -j "$NCPUS" --eta --joblog parallel_joblog.tsv \
  'py -3.9 '"$PY_SCRIPT"' \
     --roi '"$ROI"' \
     --download_dir '"$DOWN_DIR"' \
     --batch '"{}"' ' \
  :::: "$BATCH_LIST"


echo "End time: $(date)"