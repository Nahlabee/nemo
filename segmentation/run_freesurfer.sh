#!/bin/bash
###########################################################
# Execute FreeSurfer segmentation with T1 and T2 using Singularity
# Usage: ./run_freesurfer.sh <subject_id>
###########################################################

# Set the path to config.py
CONFIG_FILE="./config.py"

# Read paths from config.py and export them as environment variables
eval $(PYTHONPATH=$CONFIG_DIR python3 -c 'import config; config.print_paths()')

# Set Freesurfer environment variables
export SUBJECTS_DIR=$DIR_INPUTS
echo $SUBJECTS_DIR
export FREESURFER_DIR=$DIR_FREESURFER
echo $FREESURFER_DIR

# Create output directories if necessary
mkdir -p $FREESURFER_DIR
mkdir -p $FREESURFER_OUTPUTS

# Remove existing subject folder if exists
if [ -d "$FREESURFER_OUTPUTS/$1" ]; then
  rm -r "$FREESURFER_OUTPUTS/$1"
fi

# Execute Singularity container
singularity exec -B $SUBJECTS_DIR:/mnt -B $FREESURFER_OUTPUTS:/output -B $FREESURFER_DIR/license:/license --env FS_LICENSE=/license/license.txt $DIR_CONTAINER/freesurfer-7.4.1.sif bash -c "source /usr/local/freesurfer/SetUpFreeSurfer.sh && recon-all -all -s $1 -i /mnt/$1/ses-01/anat/$1_ses-01_T1w.nii.gz -T2 /mnt/$1/ses-01/anat/$1_ses-01_T2w.nii.gz -T2pial -sd /output"