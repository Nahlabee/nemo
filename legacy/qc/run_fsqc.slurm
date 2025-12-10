#!/bin/bash
#SBATCH -J fsqc
#SBATCH -p skylake
#SBATCH --mem=16gb
#SBATCH -n 1
#SBATCH -t 9:00:00
#SBATCH -o %x_%j.out
#SBATCH -e %x_%j.err

module purge
module load userspace/all
module load singularity

DATA_BIDS_DIR="/scratch/lhashimoto/nemo_derivatives/qsiprep"
DERIVATIVES_BIDS_DIR="/scratch/lhashimoto/nemo_derivatives"
FSQC_CONTAINER="/scratch/lhashimoto/fsqc-2.1.4.sif"

# Execute Singularity container
apptainer run \
  --writable-tmpfs --cleanenv \
  -B $DERIVATIVES_BIDS_DIR/freesurfer:/data \
  -B $DERIVATIVES_BIDS_DIR/qc/fsqc:/out \
  $FSQC_CONTAINER --subjects_dir /data --output_dir /out \
    --subjects sub-1054001_ses-01 \
    --screenshots \
    --surfaces \
    --skullstrip \
    --fornix \
    --no-group \
    --skip-existing \