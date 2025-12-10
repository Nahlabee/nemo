#!/usr/bin/env python3

from venv import logger
import numpy as np
import nibabel as nib
import pandas as pd
from pathlib import Path
from sklearn.metrics import mutual_info_score
from nilearn.image import mean_img
import warnings
import re
import os

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
    data : np.ndarray
        Loaded numeric data array.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    img = nib.load(str(path)) # type: ignore

    if isinstance(img, nib.gifti.gifti.GiftiImage):
        logger.info(f"Detected GIFTI surface file: {path.name}")
        data = np.column_stack([d.data for d in img.darrays])
    elif isinstance(img, (nib.Nifti1Image, nib.Nifti2Image)): # type: ignore
        logger.info(f"Detected NIfTI volumetric file: {path.name}")
        data = img.get_fdata()
    else:
        raise TypeError(f"Unsupported file type: {type(img)}")

    return data, img.affine  # type: ignore

def voxel_count(mask):
    """
    Extract voxel count from a mask (binary or multiclass).
    
    :param mask: array data
    :return: voxel count per unique value
    """

    return np.unique(mask, return_counts=True)


def dice(a, b):
    """
    Compute dice similarity coefficient between two binary masks.
    
    :param a: array data
    :param b: array data

    :return: Dice similarity coefficient
    """
    a = a.astype(bool)
    b = b.astype(bool)
    inter = np.logical_and(a, b).sum()
    s = a.sum() + b.sum()
    return (2 * inter / s) if s > 0 else np.nan

def mutual_information(img1, img2, bins=64):
    """
    Compute mutual information between two images.
    
    :param img1: array data
    :param img2: array data
    :param bins: number of bins for histogram
    :return: Mutual information score
    """
    i1 = img1.flatten()
    i2 = img2.flatten()

    hgram, _, _ = np.histogram2d(i1, i2, bins=bins)
    return mutual_info_score(None, None, contingency=hgram)

# -----------------------
# Main extraction
# -----------------------
def extract_subject_metrics(fmriprep_dir):
    rows = []

    for sub_dir in sorted(fmriprep_dir.glob("sub-*")):
        for ses_dir in sorted(sub_dir.glob("ses-*")):

            try:
                anat = ses_dir / "anat"
                func = ses_dir / "func"

                t1w = next(anat.glob("*_desc-preproc_T1w.nii.gz"))
                t1w_mask = next(anat.glob("*_desc-brain_mask.nii.gz"))
                gm = next(anat.glob("*_label-GM_probseg.nii.gz"))
                wm = next(anat.glob("*_label-WM_probseg.nii.gz"))
                csf = next(anat.glob("*_label-CSF_probseg.nii.gz"))

                bold = next(func.glob("*_desc-preproc_bold.nii.gz"))
                bold_mask = next(func.glob("*_desc-brain_mask.nii.gz"))

                # Load data
                t1w_data, t1w_affine = load_any_image(t1w)
                bold_data, bold_affine = load_any_image(bold)

                mean_bold_img = mean_img(bold_data)
                mean_bold = mean_bold_img.get_fdata()

                brain_mask, _ = load_any_image(bold_mask)
                brain_mask = brain_mask > 0
                bg_mask = ~brain_mask

                gm_mask, _ = load_any_image(gm)
                gm_mask = gm_mask > 0.5
                wm_mask, _ = load_any_image(wm)
                wm_mask = wm_mask > 0.5
                csf_mask, _ = load_any_image(csf)
                csf_mask = csf_mask > 0.5

                row = dict(
                    subject=sub_dir.name,
                    session=ses_dir.name,
                    brain_voxels=voxel_count(brain_mask),
                    gm_voxels=voxel_count(gm_mask),
                    wm_voxels=voxel_count(wm_mask),
                    csf_voxels=voxel_count(csf_mask),
                    tSNR=compute_tsnsr(bold_data, brain_mask),
                    SNR=compute_snr(mean_bold, brain_mask, bg_mask),
                    CNR=compute_cnr(mean_bold, gm_mask, wm_mask, bg_mask),
                    MI_T1w_BOLD=mutual_information(
                        t1w_data[t1w_mask > 0],
                        mean_bold[brain_mask > 0],
                    ),
                )

                rows.append(row)

            except Exception as e:
                print(f"⚠️ Skipping {sub_dir.name} {ses_dir.name}: {e}")
    return pd.DataFrame(rows)

# -----------------------
# Entry point
# -----------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("fmriprep_dir", type=Path)
    parser.add_argument("--out", default="fmriprep_metrics.csv")
    args = parser.parse_args()

    df = extract_subject_metrics(args.fmriprep_dir)
    df.to_csv(args.out, index=False)
    print(f"✅ Metrics saved to {args.out}")
