# nemo

MR data analyses for bipolar disorder

Please make sure to adapt the paths in the config.py file to your own disk configuration before using one of these scripts !

## Segmentation

Requirements to run the shell scripts:
- Singularity container of FreeSurfer 7.4.0
- Python 3.6+ (used to get config.py file paths)
They have been configured to run on the cluster of the MESOCENTRE, but can be adapted to run on any other cluster.

First steps on MESOCENTRE:
1) Load modules  
`module load userspace/all`  
`module load python3/3.12.0`

2) (optional) Connect to an interactive node using for instance  
`srun -p skylake --time=7:00:0 --pty bash -i`

To run freesurfer segmentation, use the following commands from the nemo root directory:
- On a single case (interactive mode) : `sh segmentation/run_freesurfer.sh SUBJECT`
- On a single case (batch mode) : `sbatch segmentation/run_freesurfer.slurm SUBJECT`
- On all new cases (batch mode) : `sh segmentation/run_freesurfer_newcases.sh`

> Note that **SUBJECT** must correspond to the name of the subject folder!

To run a specific freesurfer command on interactive mode, adapt and run the script `segmentation/run_freesurfer_usefull_commands.sh SUBJECT`

To run any command (interactive or batch) on a group of subjects, adapt and run the script `segmentation/run_loop.sh`

> Please note that when running in batch mode, you can specify an email address to get **notifications at beginning/end of the job**.
To set this email address, please modify the following line in the segmentation/run_freesurfer.slurm script:
<br>    **#SBATCH --mail-user=lucile.hashimoto@adalab.fr**


### WARNING : 

**.pial.T1 and .pial.T2 are symbolic links and may disapear when data is transfered from a user to another.**

In that case, recreate the symbolic links 

EXAMPLE: 

    for subj in *; do
        if [ -d "$subj/surf" ]; then
            [ -e "$subj/surf/rh.white.H" ] || ln -s rh.white.preaparc.H "$subj/surf/rh.white.H"
            [ -e "$subj/surf/rh.white.K" ] || ln -s rh.white.preaparc.K "$subj/surf/rh.white.K"
            [ -e "$subj/surf/rh.pial" ] || ln -s rh.pial.T2 "$subj/surf/rh.pial"
            [ -e "$subj/surf/rh.fsaverage.sphere.reg" ] || ln -s rh.sphere.reg "$subj/surf/rh.fsaverage.sphere.reg"
        fi
    done
    
    
    for subj in *; do
        if [ -d "$subj/surf" ]; then
        [ -e "$subj/surf/lh.white.H" ] || ln -s lh.white.preaparc.H "$subj/surf/lh.white.H"
        [ -e "$subj/surf/lh.white.K" ] || ln -s lh.white.preaparc.K "$subj/surf/lh.white.K"
        [ -e "$subj/surf/lh.pial" ] || ln -s lh.pial.T2 "$subj/surf/lh.pial"
        [ -e "$subj/surf/lh.fsaverage.sphere.reg" ] || ln -s lh.sphere.reg "$subj/surf/lh.fsaverage.sphere.reg"
        fi
    done

## Quality Control

Requirements:
- fsqc toolbox (https://github.com/Deep-MI/fsqc)
This package provides quality assurance / quality control scripts for FastSurfer- or FreeSurfer-processed structural MRI data. It will check outputs of these two software packages by means of quantitative and visual summaries. Prior processing of data using either FastSurfer or FreeSurfer is required, i.e. the software cannot be used on raw images.

To run the quality control, use the following commands from the nemo root directory:
1) `python3 -m qc/check_log.py`

This script will check the log files of the freesurfer segmentation and generate a csv file with segmentation errors and stats.
- Subject
- Number of folders generated
- Number of files generated
- Finished without error
- Processing time (hours)
- Euler number before topo correction LH
- Euler number after topo correction RH
- Euler number before topo correction LH
- Euler number after topo correction RH

2) `python3 -m qc/qc_fsqc.py`

This script will generate a report for each subject and a csv file for group statistics named 'fsqc-results.csv'.

Three configurations are available. Choose the one you want to use by uncommenting the corresponding lines in the qc_fsqc.py file.
- Run FSQC on a subject or a list of subjects
- Run FSQC for group-level statistics
- Run FSQC only on hippocampus and amygdala segmentations

If QC has already been performed on one or several subjects, you can run FSQC on the remaining subjects by providing a subject list. After that, the group-level analysis can be run on the subjects who have successfully completed FSQC.
> Setting group_only = true, will skip individual-level processing and run only the group-level analysis.

> To run in batch mode, use `qc_fsqc.slurm`

3) `python3 -m ./qc/qc_complete.py`

This script will recompute the group statistics of aparc and aseg segmentations after normalization of volumes by ETIV.

A new aseg_stats_norm.csv is saved for each subject.

The number of outliers is updated and all QC statistics are merged and saved in the fsqc-results-complete.csv file.


## Statistics

This folder is intended to contain notebooks for statistical analyses on the NEMO dataset.

To date, analyses have only been achieved on morphometry measurements (cortical volumes and thickness from Freesurfer segmentation) and their correlation with clinical observations.

