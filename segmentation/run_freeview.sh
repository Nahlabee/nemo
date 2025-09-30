# Set the path to config.py
CONFIG_FILE="./config.py"

# Read paths from config.py and export them as environment variables
eval $(PYTHONPATH=$CONFIG_DIR python3 -c 'import config; config.print_paths()')

# Set Freesurfer environment variables
export SUBJECTS_DIR=$DIR_INPUTS
echo $SUBJECTS_DIR
export FREESURFER_DIR=$DIR_FREESURFER
echo $FREESURFER_DIR

# Execute Singularity container
singularity exec -B $SUBJECTS_DIR:/mnt -B $FREESURFER_OUTPUTS:/output -B $FREESURFER_DIR/license:/license --env FS_LICENSE=/license/license.txt $DIR_CONTAINER/freesurfer-7.4.1.sif bash -c "source /usr/local/freesurfer/SetUpFreeSurfer.sh && freeview -v mri/brainmask.mgz mri/wm.mgz:colormap=heat:opacity=0.4:visible=0 -f surf/lh.white:edgecolor=yellow surf/lh.pial:edgecolor=red surf/rh.white:edgecolor=yellow surf/rh.pial:edgecolor=red surf/lh.orig.nofix:visible=0"