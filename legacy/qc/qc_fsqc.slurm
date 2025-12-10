#!/bin/bash
#SBATCH -J fsqc
#SBATCH -p skylake
#SBATCH -n 1
#SBATCH -t 9:00:00
#SBATCH --mail-type=BEGIN,END
#SBATCH --mail-user=andrea.paysserand@univ-amu.fr

# load modules
module purge
module load userspace/all

source /scratch/lhashimoto/python/env/nemo/bin/activate
python3 /scratch/lhashimoto/nemo/qc/qc_fsqc.py