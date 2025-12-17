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

DATA_BIDS_DIR="/scratch/lhashimoto/nemo_derivatives/qsiprep"
DERIVATIVES_BIDS_DIR="/scratch/lhashimoto/nemo_derivatives"
FREESURFER_CONTAINER="/scratch/lhashimoto/freesurfer-7.4.1.sif"
FREESURFER_LICENSE="/scratch/lhashimoto/freesurfer/license"
FREESURFER_DIR="/scratch/lhashimoto/nemo_derivatives/freesurfer"
QSIRECON_CONTAINER="/scratch/lhashimoto/qsirecon-1.1.1.sif"
CONFIG="/scratch/lhashimoto/code/nemo/config/qsirecon_config.toml"

# Execute Singularity container
apptainer run \
  --nv --writable-tmpfs --cleanenv \
  -B $DERIVATIVES_BIDS_DIR/qsiprep:/data \
  -B $DERIVATIVES_BIDS_DIR/qsirecon:/out \
  -B $FREESURFER_DIR:/freesurfer \
  -B $FREESURFER_LICENSE/license.txt:/opt/freesurfer/license.txt \
  -B $CONFIG:/config/qsirecon_config.toml \
  $QSIRECON_CONTAINER /data /out participant \
    --participant-label 1054001 --session-id 01 \
    -v -w /out/temp_qsirecon \
    --fs-license-file /opt/freesurfer/license.txt \
    --fs-subjects-dir /freesurfer \
    --config-file /config/qsirecon_config.toml \
    --atlases AAL116 \
    --bids-database-dir /out/temp_qsirecon/bids_db_dir