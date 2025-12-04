#!/bin/bash
#SBATCH -J qsirecon
#SBATCH -p kepler
#SBATCH --gpus-per-node=2
#SBATCH --mem=70gb
#SBATCH -n 1
#SBATCH -t 24:00:00
#SBATCH -o %x_%j.out
#SBATCH -e %x_%j.err

module purge
module load userspace/all
module load singularity
#module load python3/3.12.0

# Set the path to config.py
#CONFIG_FILE="tests/config.py"

# Read paths from config.py and export them as environment variables
#eval $(python3 -c 'import config; config.print_paths()')

DERIVATIVES_BIDS_DIR="/scratch/lhashimoto/nemo_database_derivatives"
FREESURFER_LICENSE="/scratch/lhashimoto/freesurfer/license"
QSIRECON_CONTAINER="/scratch/lhashimoto/qsirecon-1.1.1.sif"

# Execute Singularity container
apptainer run \
  --nv --containall --writable-tmpfs --cleanenv \
  -B $DERIVATIVES_BIDS_DIR \
  -B $FREESURFER_LICENSE/license.txt:/opt/freesurfer/license.txt \
  $QSIRECON_CONTAINER $DERIVATIVES_BIDS_DIR/qsiprep $DERIVATIVES_BIDS_DIR/qsirecon participant \
    --participant-label sub-1054001 --session-id ses-01 \
    -v -w $DERIVATIVES_BIDS_DIR/work \
    --fs-license-file /opt/freesurfer/license.txt \
    --fs-subjects-dir $DERIVATIVES_BIDS_DIR/freesurfer \
    --recon-spec mrtrix_multishell_msmt_ACT-hsvs \
    --atlases AAL116