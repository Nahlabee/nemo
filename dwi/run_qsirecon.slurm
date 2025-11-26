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

# Set the path to config.py
CONFIG_FILE="../config.py"

# Read paths from config.py and export them as environment variables
eval $(python3 -c 'import config; config.print_paths()')

# Execute Singularity container
apptainer run \
  --nv --cleanenv \
  -B $DERIVATIVES_BIDS_DIR:/data \
  -B $FREESURFER_DIR:/freesurfer \
  -B $FREESURFER_LICENSE/license.txt:/opt/freesurfer/license.txt \
  $QSIRECON_CONTAINER /data /data participant \
  --participant-label sub-1054001 --session-id ses-01 \
  --skip-bids-validation -v -w /out/temp_wf_qsirecon \
  --fs-license-file /opt/freesurfer/license.txt \
  --fs-subjects-dir /freesurfer \
  --output-resolution 1.8 \
  --recon-spec mrtrix_multishell_msmt_ACT-hsvs