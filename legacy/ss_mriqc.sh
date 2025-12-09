#!/bin/bash
#SBATCH --mail-type=ALL 			# Mail events (NONE, BEGIN, END, FAIL, ALL)
#SBATCH -p skylake
#SBATCH --mail-user=henitsoa.rasoanandrianina@adalab.fr	# Your email address
#SBATCH --nodes=1					# OpenMP requires a single node
#SBATCH --mem=92GB
#SBATCH --cpus-per-task=16
#SBATCH --time=50:00:00				# Time limit hh:mm:ss
#SBATCH -o ./log_mriqc/%x-%A-%a.out
#SBATCH -e ./log_mriqc/%x-%A-%a.err
#SBATCH -J mriqc_rsfMRI		# Job name

set -eu
# Get arguments from submit_job_array.sh
args=($@)
subjs=(${args[@]:3})
base=$1
study=$2
ses=$3
sub=${subjs[${SLURM_ARRAY_TASK_ID}]}
echo $sub

singularity run --cleanenv -B /scratch/jsein/BIDS:/work \
 /scratch/jsein/my_images/mriqc-24.0.2.sif /work/$study  \
 /work/$study/derivatives/mriqc participant group 
 --participant_label $sub --n_procs 12    \
 -w /work/temp_data_${study} --fd_thres 0.5 --verbose-reports 

   # --ica --run-id 01 --task-id rest -m bold --session-id $ses