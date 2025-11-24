#!/bin/bash
#SBATCH -J qsiprep
#SBATCH -p kepler
#SBATCH -n 1
#SBATCH -t 9:00:00
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
apptainer run --nv -B $DATA_BIDS_DIR:/data,$DERIVATIVES_BIDS_DIR:/out,$FREESURFER_LICENSE/license.txt:/opt/freesurfer/license.txt $QSIPREP_CONTAINER /data /out participant --participant-label sub-1054001 --session-id ses-01 --skip-bids-validation -v -w /out/temp_wf_qsiprep --fs-license-file /opt/freesurfer/license.txt --output-resolution 1.2