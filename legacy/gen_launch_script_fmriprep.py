# subjects indices from 1 to 11
subs = range(1,12)

# global parameters
sing_version = '23.2.0'
clean_workdir = '' # '' or ' --clean-workdir'
requested_mem = 70 # in Gb (integer)
requested_time = 48 # in hours (integer)

# generate 'sub-xxx_fmriprep.sh' scripts
for sub in subs:

    header = \
'''#!/bin/bash
#SBATCH -J fprp{0:03}
#SBATCH -p skylake
#SBATCH -A b347
#SBATCH --nodes=1
#SBATCH --mem={1}gb
#SBATCH --cpus-per-task=32
#SBATCH --time={2}:00:00
#SBATCH -e /scratch/mgilson/braint/derivatives/fmriprep/sbatch_outputs/sub-{0:03}_%N_%j_%a.err
#SBATCH -o /scratch/mgilson/braint/derivatives/fmriprep/sbatch_outputs/sub-{0:03}_%N_%j_%a.out
#SBATCH --mail-type=BEGIN,END
#SBATCH --mail-user=matthieu.gilson@univ-amu.fr
'''.format(sub, requested_mem, requested_time)

    module_directory = \
'''
module purge
module load userspace/all
module load singularity

# directories
BIDS_ROOT_DIR="E:/projects_files/hr_lb/bids_dir"

cd $BIDS_ROOT_DIR
'''

    singularity_command = \
'''
# singularity command
singularity run -B $BIDS_ROOT_DIR:/data,$BIDS_ROOT_DIR/derivatives/fmriprep:/out \\
    /scratch/mgilson/braint/code/singularity/fmriprep-{1}.simg /data /out \\
        participant --participant-label {0:03} \\
        -w /out/temp_wf {2} \\
        --fs-license-file /data/code/freesurfer/license.txt \\
        --skip-bids-validation \\
        --bold2t1w-dof 6 --bold2t1w-init register \\
        --fd-spike-threshold 0.5 --dvars-spike-threshold 2.0 \\
        --cifti-output 91k \\
        --output-spaces fsLR T1w fsaverage \\
        --ignore slicetiming \\
        --low-mem --mem-mb 50000 \\
        --nthreads 32
'''.format(sub, sing_version, clean_workdir)

    ownership_sharing = \
'''
chmod -Rf 771 $BIDS_ROOT_DIR
chgrp -Rf 347 $BIDS_ROOT_DIR

echo "FINISHED OK"
'''
    
    file_content = header + module_directory + singularity_command + ownership_sharing
    
    file_dir = '../rsfmri/legacy/'
    file_name = 'sub-{0:03}_fmriprep.slurm'.format(sub)
    
    with open(file_dir+file_name, 'w') as f:
        f.write(file_content)
