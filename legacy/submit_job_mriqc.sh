#!/bin/bash

# Submit subjects to be run through MRIQC. 

# Usages:

# - run all subjects in project base

# bash submit_job_mriqc.sh

#set -eu
study=TP_MASCO
#subjs=$@
# SET THIS TO BE THE PATH TO YOUR BIDS DIRECTORY
bids=/scratch/jsein/BIDS/$study

# if you want to process specific subjects, uncomment the line below and specify them:
#subjs=(MAS20251010 MAS20251017 MAS20251024 MAS20251107)


# if you want to process al subjects, uncomment the lines below:
pushd $bids
subjs=($(ls sub-* -d | cut -d'-' -f 2))
popd
pushd $bids/derivatives/mriqc
subjsd=($(ls sub-*/ -d | cut -d'-' -f 2 | cut -d'/' -f 1))
popd
for i in ${subjsd[@]}
do 
	subjs=( ${subjs[@]/$i} )
done


ses ='01'  #need to not be empty


# take the length of the array
# this will be useful for indexing later
len=$(expr ${#subjs[@]} - 1) # len - 1

echo Spawning ${#subjs[@]} sub-jobs.



sbatch --array=0-$len%30 $bids/code/mriqc/ss_mriqc.sh $bids $study $ses ${subjs[@]}

#alternatives

#sbatch --array=0-$len%30 $bids/code/mriqc/ss_mriqc_dwi.sh $bids $study $ses ${subjs[@]}
#sbatch $bids/code/mriqc/ss_mriqc_14p2.sh $bids $study $part $ses ${subjs[@]}
#sbatch $bids/code/mriqc/ss_mriqc_23.1.0.sh $bids $study $part $ses ${subjs[@]}
#bash affiche_param.sh $bids $study $ses ${subjs[@]}


