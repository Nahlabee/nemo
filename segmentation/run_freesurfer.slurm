#!/bin/bash
#SBATCH -J freesurfer
#SBATCH -p skylake
#SBATCH -n 1
#SBATCH -t 9:00:00
#SBATCH -o %x_%j.out
#SBATCH -e %x_%j.err
#SBATCH --mail-type=BEGIN,END
#SBATCH --mail-user=lucile.hashimoto@adalab.fr

# load modules
module purge
module load userspace/all

# Run FreeSurfer segmentation in batch mode
sh ./segmentation/run_freesurfer.sh $1