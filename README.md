# nemo

MR data analyses for bipolar disorder

This repository is dedicated to launch pre- and post-processing workflows on MR data acquired with the following MR protocol:
- T1w (1.0x1.0x1.0 mm3)
- T2w (1.0x1.0x1.0 mm3)
- DWI_PA_109vol (1.8x1.8x1.8 mm3)
- DWI_AP_109vol (1.8x1.8x1.8 mm3)
- rs-FMRI + fieldmap

**File system**
Data must be organized according to the BIDS format (https://bids.neuroimaging.io/index.html)
dataset/
├─ sub-01/
│  ├─ ses-01/
│  │  ├─ anat/
│  │  │  ├─sub-01_ses-01_T1w.nii.gz
│  │  │  ├─sub-01_ses-01_T2w.nii.gz
│  │  ├─ dwi/
│  │  │  ├─sub-01_ses-01_dir-AP_run-01_dwi.nii.gz
│  │  │  ├─sub-01_ses-01_dir-AP_run-01_dwi.bval
│  │  │  ├─sub-01_ses-01_dir-AP_run-01_dwi.bvec
│  │  │  ├─sub-01_ses-01_dir-PA_run-01_dwi.nii.gz
│  │  │  ├─sub-01_ses-01_dir-PA_run-01_dwi.bval
│  │  │  └─sub-01_ses-01_dir-PA_run-01_dwi.bvec
│  │  ├─ fmap/
│  │  │  ├─sub-01_ses-01_dir-AP_epi.nii.gz
│  │  │  └─sub-01_ses-01_dir-PA_epi.nii.gz
│  │  └─ func/
│  │  │  ├─sub-01_ses-01_task-rest_bold.nii.gz
│  │  │  └─sub-01_ses-01_task-rest_sbref.nii.gz

**Workflows**
- (anat) Anatomical Segmentation using FreeSurfer
- (anat) Sulcal Pits Extraction using Slam
- (dwi) Structural Connectome Estimation using QSIprep
- (func) Functional Connectome Estimation using FMRIprep and XCP-D

**Prerequisites**
BIDS version : 1.8.0
Python 3.12
FreeSurfer 7.4.1


Please make sure to adapt the paths in the config.py file to your own disk configuration before using one of these scripts !

## anat

## dwi

## func

dataset/
├─ derivatives/
│  ├─ freesurfer
│  │  ├─ sub-01/
│  │  │  ├─ ses-01/
│  │  │  │  ├─ label/
│  │  │  │  ├─ mri/
│  │  │  │  ├─ surf/
│  ├─ fsqc
│  │  ├─ fornix/
│  │  ├─ metrics/
│  │  ├─ screenshots/
│  │  │  ├─ sub-01/
│  ├─ sub-01/ (contient tous les derivatives des BIDS app)
│  │  ├─ anat/ (average across sessions)
│  │  ├─ figures/
│  │  ├─ log/
│  │  ├─ ses-01/
│  │  │  ├─ anat/
│  │  │  ├─ dwi/
│  │  │  ├─ fmap/
│  │  │  └─ func/
