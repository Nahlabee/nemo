#!/usr/bin/env python3
import json
from venv import logger
import numpy as np
import nibabel as nib
import pandas as pd
from sklearn.metrics import mutual_info_score
# from nilearn.image import mean_img
import warnings
import os
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
import utils
import glob

warnings.filterwarnings("ignore")


# -----------------------


def load_any_image(path: Path) -> np.ndarray:
    """
    Load an fMRIPrep/XCP-D output image, handling both NIfTI and GIFTI formats.
    
    Parameters
    ----------
    path : Path
        Path to the .nii(.gz) or .gii file.
    
    Returns
    -------
    img : nibabel image object
        Loaded image object.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    img = nib.load(str(path))  # type: ignore

    if isinstance(img, nib.gifti.gifti.GiftiImage):
        logger.info(f"Detected GIFTI surface file: {path.name}")
    elif isinstance(img, (nib.Nifti1Image, nib.Nifti2Image)):  # type: ignore
        logger.info(f"Detected NIfTI volumetric file: {path.name}")
    else:
        raise TypeError(f"Unsupported file type: {type(img)}")

    return img


def voxel_count(mask):
    """
    Extract voxel count from a mask (binary or multiclass).
    
    Parameters
    ----------
    mask : np.ndarray
        Mask array data.
    Returns
    -------
    int
        Number of True voxels.
    """

    return np.sum(mask)


def dice(a, b):
    """
    Compute dice similarity coefficient between two binary masks.
    
    Parameters
    ----------
    a : np.ndarray
        First binary mask array.
    b : np.ndarray
        Second binary mask array.

    Returns
    -------
    float
        Dice similarity coefficient.
    """
    a = a.astype(bool)
    b = b.astype(bool)
    inter = np.logical_and(a, b).sum()
    s = a.sum() + b.sum()
    return (2 * inter / s) if s > 0 else np.nan


def mutual_information(img1, img2, bins=64):
    """
    Compute mutual information between two images.
    
    Parameters
    ----------
    img1 : np.ndarray
        First image data array.
    img2 : np.ndarray
        Second image data array.
    bins : int, optional
        Number of bins for histogram (default is 64).
    Returns
    -------
    float
        Mutual information score.
    """
    i1 = img1.flatten()
    i2 = img2.flatten()

    if len(i1) != len(i2):
        return np.nan

    hgram, _, _ = np.histogram2d(i1, i2, bins=bins)
    return mutual_info_score(None, None, contingency=hgram)


# -----------------------
# Main extraction
# -----------------------
def run(config, subject, session):
    """
    Extract QC metrics from fMRIPrep outputs.

    Parameters
    ----------
    config : dict
        Configuration dictionary.
    fmriprep_dir : Path
        Path to the fMRIPrep derivatives directory. 
    Returns
    -------
    pd.DataFrame
        DataFrame containing QC metrics for each subject and session.
    """

    DERIVATIVES_DIR = config["common"]["derivatives"]
    output_dir = f"{DERIVATIVES_DIR}/fmriprep/outputs/{subject}/{session}"

    try:
        # Extract process status from log files
        finished_status, runtime = utils.read_log(config, subject, session, runtype="fmriprep")
        dir_count = utils.count_dirs(output_dir)
        file_count = utils.count_files(output_dir)

        # Load TSV file produced by QSIprep
        fmriprep_confounds = f'{subject}_{session}_task-rest_desc-confounds_timeseries.tsv'
        df = pd.read_csv(os.path.join(output_dir, 'dwi', fmriprep_confounds), sep='\t')

        max_framewise_displacement = df['framewise_displacement'].max()
        max_rot_x = df['rot_x'].max()
        max_rot_y = df['rot_y'].max()
        max_rot_z = df['rot_z'].max()
        max_trans_x = df['trans_x'].max()
        max_trans_y = df['trans_y'].max()
        max_trans_z = df['trans_z'].max()
        max_dvars = df['dvars'].max()
        max_rmsd = df['rmsd'].max()

        anat = Path(os.path.join(output_dir, "anat"))
        func = Path(os.path.join(output_dir, "func"))

        # todo: test fmriprep sessionwise and check directory for T1w
        # Identify required files
        t1w = next(anat.glob("*_desc-preproc_T1w.nii.gz"))
        t1w_mask = next(anat.glob("*_desc-brain_mask.nii.gz"))
        gm = next(anat.glob("*_label-GM_probseg.nii.gz"))
        wm = next(anat.glob("*_label-WM_probseg.nii.gz"))
        csf = next(anat.glob("*_label-CSF_probseg.nii.gz"))

        bold = next(func.glob("*_space-T1w_desc-preproc_bold.nii.gz"))
        bold_mask = next(func.glob("*_space-T1w_desc-brain_mask.nii.gz"))

        # Load data
        t1w_img = load_any_image(t1w)
        t1w_data = t1w_img.get_fdata()
        t1w_mask_img = load_any_image(t1w_mask)
        t1w_mask_data = t1w_mask_img.get_fdata()
        bold_img = load_any_image(bold)
        bold_data = bold_img.get_fdata()

        # Compute mean BOLD image
        mean_bold = np.mean(bold_data, axis=-1)

        # Load masks for voxel counts
        bold_mask_img = load_any_image(bold_mask)
        bold_mask_data = bold_mask_img.get_fdata()
        t1w_brain = t1w_data[t1w_mask_data > 0]
        bold_brain = bold_data[bold_mask_data > 0]

        gm_img = load_any_image(gm)
        gm_mask = gm_img.get_fdata() > 0.5
        wm_img = load_any_image(wm)
        wm_mask = wm_img.get_fdata() > 0.5
        csf_img = load_any_image(csf)
        csf_mask = csf_img.get_fdata() > 0.5

        # Resample dwi into t1w space
        bold_brain_hr = utils.resample(bold_brain, t1w_data)
        bold_mask_data_hr = utils.resample(bold_mask_data, t1w_data)

        # Compute QC metrics
        # if t1w_data.shape == t1w_mask_data.shape:
        #     t1w_brain = t1w_data[t1w_mask_data > 0]
        # else:
        #     print(f"Shape mismatch for T1w and mask: {t1w_data.shape} vs {t1w_mask_data.shape}, using all T1w data")
        #     t1w_brain = t1w_data.flatten()
        #
        # if mean_bold.shape == brain_mask.shape:
        #     bold_brain = mean_bold[brain_mask > 0]
        # else:
        #     print(f"Shape mismatch for BOLD and mask: {mean_bold.shape} vs {brain_mask.shape}, using all BOLD data")
        #     bold_brain = mean_bold.flatten()

        row = dict(
            subject=subject,
            session=session,
            Process_Run="fmriprep",
            Finished_without_error=finished_status,
            Processing_time_hours=runtime,
            Number_of_folders_generated=dir_count,
            Number_of_files_generated=file_count,
            t1w_shape=t1w_data.shape,
            brain_voxels_t1w=np.sum(t1w_mask_data > 0),
            brain_voxels_bold=np.sum(bold_mask_data > 0),
            bold_shape=bold_img.shape,
            gm_voxels=np.sum(gm_mask > 0),
            wm_voxels=np.sum(wm_mask > 0),
            csf_voxels=np.sum(csf_mask > 0),
            DICE_t1w_bold=utils.dice(t1w_mask_data, bold_mask_data_hr),
            MI_t1w_bold=mutual_information(t1w_brain, bold_brain_hr),
            max_framewise_displacement=max_framewise_displacement,
            max_rot_x=max_rot_x,
            max_rot_y=max_rot_y,
            max_rot_z=max_rot_z,
            max_trans_x=max_trans_x,
            max_trans_y=max_trans_y,
            max_trans_z=max_trans_z,
            max_dvars=max_dvars,
            max_rmsd=max_rmsd,
        )

        sub_ses = pd.DataFrame([row])
        # Save outputs to csv file
        path_to_qc = f"{DERIVATIVES_DIR}/qc/fmriprep/outputs/{subject}/{session}/{subject}_{session}_qc.csv"
        sub_ses.to_csv(path_to_qc, mode='w', header=True, index=False)
        print(f"QC saved in {path_to_qc}\n")

        print(f"Fmriprep Quality Check terminated successfully for {subject} {session}.")

    except Exception as e:
        print(f"⚠️ ERROR: QC aborted for {subject} {session}: \n{e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 4:
        raise RuntimeError(
            "Usage: python qc_fmriprep_metrics_extractions.py <config_path> <subject> <session>"
        )
    config = json.loads(sys.argv[1])
    subject = sys.argv[2]
    session = sys.argv[3]
    run(config, subject, session)

    # # todo: config not a path but a dict
    # config_path, subject, session = sys.argv[1:4]
    # with open(config_path, "rb") as f:
    #     config = tomllib.load(f)
    # run(config, subject, session)
