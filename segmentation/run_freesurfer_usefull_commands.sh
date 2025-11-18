#!/bin/bash
###########################################################
# Execute any FreeSurfer command using Singularity
# Usage: ./run_freesurfer_usefull_commands.sh <subject_id>
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


## Reconstruct surfaces using T2
#singularity exec -B $SUBJECTS_DIR:/mnt -B $FREESURFER_OUTPUTS:/output -B $FREESURFER_DIR/license:/license --env FS_LICENSE=/license/license.txt $DIR_CONTAINER/freesurfer-7.4.1.sif bash -c "source /usr/local/freesurfer/SetUpFreeSurfer.sh && recon-all -s $1 -T2 /mnt/$1/ses-01/anat/$1_ses-01_T2w.nii.gz -T2pial -autorecon3 -sd /output"

## Hippo-amygdala segmentation using PYTHON version beta
#singularity exec -B $SUBJECTS_DIR:/mnt -B $FREESURFER_OUTPUTS:/output -B $FREESURFER_DIR/license:/license --env FS_LICENSE=/license/license.txt $DIR_CONTAINER/freesurfer-7.4.1.sif bash -c "source /usr/local/freesurfer/SetUpFreeSurfer.sh && segment_subregions hippo-amygdala --sd /output --cross $1"

## Hippo-amygdala segmentation using MATLAB RUNTIME
## Installation du runtime matlab
#singularity exec -B $SUBJECTS_DIR:/mnt -B $FREESURFER_OUTPUTS:/output -B $FREESURFER_DIR/license:/license --env FS_LICENSE=/license/license.txt $DIR_CONTAINER/freesurfer-7.4.1.sif bash -c "source /usr/local/freesurfer/SetUpFreeSurfer.sh && fs_install_mcr R2019b"
#singularity exec -B $SUBJECTS_DIR:/mnt -B $FREESURFER_OUTPUTS:/output -B $FREESURFER_DIR/license:/license --env FS_LICENSE=/license/license.txt $DIR_CONTAINER/freesurfer-7.4.1.sif bash -c "source /usr/local/freesurfer/SetUpFreeSurfer.sh && segmentHA_T1.sh $1 --sd /output"

## Convert a volume into NIFTI
#singularity exec -B $FREESURFER_OUTPUTS:/output -B $FREESURFER_DIR/license:/license --env FS_LICENSE=/license/license.txt $DIR_CONTAINER/freesurfer-7.4.1.sif bash -c "source /usr/local/freesurfer/SetUpFreeSurfer.sh && mri_convert /output/$1/mri/brainmask.mgz /output/$1/mri/brainmask.nii.gz"
#singularity exec -B $FREESURFER_OUTPUTS:/output -B $FREESURFER_DIR/license:/license --env FS_LICENSE=/license/license.txt $DIR_CONTAINER/freesurfer-7.4.1.sif bash -c "source /usr/local/freesurfer/SetUpFreeSurfer.sh && mri_convert /output/$1/mri/T1.mgz /output/$1/mri/T1.nii.gz"

## Convert ALL volumes into NIFTI
#input_dir="/output/$1/mri"  # Dossier source contenant les fichiers .mgz
#singularity exec -B $FREESURFER_OUTPUTS:/output -B $FREESURFER_DIR/license:/license --env FS_LICENSE=/license/license.txt $DIR_CONTAINER/freesurfer-7.4.1.sif bash -c "source /usr/local/freesurfer/SetUpFreeSurfer.sh &&
#  for file in $input_dir/*.mgz; do
#    if [ -f \"\$file\" ]; then
#      output_file=\"\${file%.mgz}.nii.gz\"
#	  if [ ! -f \"\$output_file\" ]; then
#        mri_convert \"\$file\" \"\$output_file\"
#        echo \"Conversion de \$file en \$output_file\"
#	  fi
#    fi
#  done
#"

## Convert a surface into GIFTI
#singularity exec -B $FREESURFER_OUTPUTS:/output -B $FREESURFER_DIR/license:/license --env FS_LICENSE=/license/license.txt $DIR_CONTAINER/freesurfer-7.4.1.sif bash -c "source /usr/local/freesurfer/SetUpFreeSurfer.sh && mris_convert /output/$1/surf/lh.inflated /output/$1/surf/lh.inflated.gii"

## Inflate a surface
#singularity exec -B $FREESURFER_OUTPUTS:/output -B $FREESURFER_DIR/license:/license --env FS_LICENSE=/license/license.txt $DIR_CONTAINER/freesurfer-7.4.1.sif bash -c "source /usr/local/freesurfer/SetUpFreeSurfer.sh && mris_inflate -n 2 /output/$1/surf/lh.smoothwm.gii /output/$1/surf/lh.smoothwm_inflated_2.gii"

## Smooth a surface
singularity exec -B $FREESURFER_OUTPUTS:/output -B $FREESURFER_DIR/license:/license --env FS_LICENSE=/license/license.txt $DIR_CONTAINER/freesurfer-7.4.1.sif bash -c "source /usr/local/freesurfer/SetUpFreeSurfer.sh && mris_smooth -n 10 /output/$1/surf/lh.smoothwm.gii /output/$1/surf/lh.smoothwm_smoothed_10.gii"