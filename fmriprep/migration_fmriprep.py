#!/usr/bin/env python3
"""
run_fmriprep_migration.py

Purpose: Run fMRIPrep 23.2.0 and 25.2.0 using Singularity/Apptainer,
         and automatically perform QA comparisons.

Usage:
    python run_fmriprep_migration.py \
        --bids_dir /data \
        --output_dir /derivatives \
        --subject sub-01 \
        --fs_license /licenses/license.txt \
        --sif_dir /containers
"""

import subprocess
from pathlib import Path
import argparse
import json
import pandas as pd # type: ignore
import nibabel as nib # type: ignore
import logging
import numpy as np # type: ignore

# Configure logging
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("xcpd_postproc")


def run_singularity(fmriprep_sif, bids_dir, out_dir, fs_license, subject, version, extra_flags=[]):
    """Run fMRIPrep using Singularity/Apptainer.
    
     Args:
        fmriprep_sif (Path): Path to the fMRIPrep Singularity image.
        bids_dir (Path): Path to the BIDS dataset.
        out_dir (Path): Path to the output directory.
        fs_license (Path): Path to the FreeSurfer license file.
        subject (str): Subject label (e.g., 'sub-01').
        version (str): fMRIPrep version string for logging.
        extra_flags (list): Additional command-line flags for fMRIPrep."""
    

    print(f"=== Running fMRIPrep {version} for {subject} ===")
    cmd = [
        "apptainer", "run",
        "-B", f"{str(bids_dir)}:/data:ro",
        "-B", f"{str(out_dir)}:/out",
        "-B", f"{str(fs_license)}:/license.txt",
        str(fmriprep_sif),
        "/data", "/out", "participant",
        "--participant-label", str(subject),
        "--fd-spike-threshold",str(0.5),
        "--dvars-spike-threshold", str(2.0),
        "--cifti-output", "91k",
        "--subject-anatomical-reference", "sessionwise",
        "--project-goodvoxels",
        "--fs-license-file", "/license.txt",
        "--output-spaces", "fsLR:den-32k", "T1w", "fsaverage:den-164k", "MNI152NLin6Asym",
        "--ignore", "slicetiming",
        "--mem-mb", str(50000),
        "--nthreads", str(8),
        "--skip-bids-validation",
        "--clean-workdir"
    ]
    # cmd.extend(extra_flags)
    print("Running command:", " ".join(cmd))
    subprocess.run(cmd, check=True)


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


def summarize_data(data: np.ndarray, path: Path):
    """Compute and log basic statistics."""
    logger.info(f"Summary for {path.name}: shape={data.shape}")
    logger.info(f"Mean={np.mean(data):.4f}, Std={np.std(data):.4f}, Min={np.min(data):.4f}, Max={np.max(data):.4f}")


def compare_json(json1_path, json2_path, output_path):
    """
    Compare two JSON files and log differences.
    
     Args:
        json1_path (Path): Path to the first JSON file.
        json2_path (Path): Path to the second JSON file.
        output_path (Path): Path to save the differences."""
    print("=== Comparing JSON provenance ===")

    with open(json1_path) as f:
        data1 = json.load(f)
    with open(json2_path) as f:
        data2 = json.load(f)
    diffs = []
    keys1 = set(data1.keys())
    keys2 = set(data2.keys())
    all_keys = keys1.union(keys2)
    for k in all_keys:
        if data1.get(k) != data2.get(k):
            diffs.append((k, data1.get(k), data2.get(k)))
    with open(output_path, "w") as f:
        for k, v1, v2 in diffs:
            f.write(f"{k}: 23.2.0={v1} 25.0.0={v2}\n")
    print(f"JSON diff saved to {output_path}")

def compare_gifti_surfaces(file1: Path, file2: Path, output_file: Path):
    """
    Compare two GIfTI files and save the difference image.
    
     Args:
        file1 (str): Path to the first GIfTI file.
        file2 (str): Path to the second GIfTI file.
        output_file (str): Path to save the difference NIfTI file."""
    
    print(f"Comparing GIfTI: {file1} vs {file2}")
    img1, img1_aff = load_any_image(file1)
    summarize_data(img1, file1)

    img2, img2_aff = load_any_image(file2)
    summarize_data(img2, file2)
    
    data_diff = img1 - img2
    diff_img = nib.GiftiImage(data_diff, img1_aff) # type: ignore
    nib.save(diff_img, output_file) # type: ignore
    print(f"GIfTI diff saved to {output_file}")

def compare_nifti(file1: Path, file2: Path, output_file: Path):
    """
    Compare two NIfTI files and save the difference image.
    
     Args:
        file1 (str): Path to the first NIfTI file.
        file2 (str): Path to the second NIfTI file.
        output_file (str): Path to save the difference NIfTI file."""
    
    print(f"Comparing NIfTI: {file1} vs {file2}")
    img1, img1_aff = load_any_image(file1)
    summarize_data(img1, file1)
    img2, img2_aff = load_any_image(file2)
    summarize_data(img2, file2)
    data_diff = img1.get_fdata() - img2.get_fdata()
    diff_img = nib.Nifti1Image(data_diff, img1_aff) # type: ignore
    nib.save(diff_img, output_file) # type: ignore
    print(f"NIfTI diff saved to {output_file}")

def compare_confounds(csv1, csv2, output_file):
    """
    Compare two confound CSV files and save the differences.
    
     Args:
        csv1 (str): Path to the first confound CSV file.
        csv2 (str): Path to the second confound CSV file.
        output_file (str): Path to save the differences."""
    
    print(f"Comparing confounds CSV: {csv1} vs {csv2}")
    df1 = pd.read_csv(csv1, sep="\t")
    df2 = pd.read_csv(csv2, sep="\t")
    diff = df1.fillna(0) - df2.fillna(0)
    diff.to_csv(output_file, sep="\t", index=False)
    print(f"Confound CSV diff saved to {output_file}")

def check_html_report(report_path, output_file):
    """
    Check the HTML report for size and log it.
    
     Args:
        report_path (Path): Path to the HTML report file.
        output_file (Path): Path to save the size information."""
    
    size = report_path.stat().st_size
    with open(output_file, "w") as f:
        f.write(f"{report_path.name} size: {size} bytes\n")
    print(f"Report size logged to {output_file}") 
 
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bids_dir", required=True)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--subject", required=True)
    parser.add_argument("--fs_license", required=True)
    parser.add_argument("--sif_dir", required=True)
    args = parser.parse_args()

    bids_dir = Path(args.bids_dir)
    out_dir = Path(args.output_dir)
    subject = args.subject
    fs_license = Path(args.fs_license)
    sif_dir = Path(args.sif_dir)
    sif_dir.mkdir(parents=True, exist_ok=True)
     
    out_23 = out_dir / "fmriprep23"
    out_25 = out_dir / "fmriprep25.2.0"
    out_23.mkdir(parents=True, exist_ok=True)
    out_25.mkdir(parents=True, exist_ok=True)

    # Paths to Singularity images
    sif_23 = sif_dir / "fmriprep_23.2.0.sif"
    sif_25 = sif_dir / "fmriprep_25.2.0.sif"

    # Run 23.2.0
    # run_singularity(sif_23, bids_dir, out_23, fs_license, subject, "23.2.0",
    #                 extra_flags=["--bold2t1w-dof", str(6), "--bold2t1w-init", "register"])
    # Run 25.2.0
    run_singularity(sif_25, bids_dir, out_25, fs_license, subject, "25.2.0",
                    extra_flags=["--bold2anat-dof", str(6), "--bold2anat-init", "auto"])
                    #              "--write-graph", "--skip-bids-validation"])

    # QA directory
    qa_dir = out_dir / "diff_logs"
    (qa_dir / "masks").mkdir(parents=True, exist_ok=True)
    (qa_dir / "confounds").mkdir(parents=True, exist_ok=True)
    (qa_dir / "surfaces").mkdir(parents=True, exist_ok=True)
    (qa_dir / "reports").mkdir(parents=True, exist_ok=True)

    # Compare T1w JSON
    json_23 = next((out_23.rglob("*desc-preproc_T1w.json")), None)
    json_25 = next((out_25.rglob("*desc-preproc_T1w.json")), None)
    if json_23 and json_25:
        compare_json(json_23, json_25, qa_dir / "provenance_diff.txt")

    # Compare brain mask (NIFTIs)
    mask_23 = next((out_23.rglob("*brainmask.nii.gz")), None)
    mask_25 = next((out_25.rglob("*brainmask.nii.gz")), None)
    if mask_23 and mask_25:
        compare_nifti(mask_23, mask_25, qa_dir / "masks/mask_diff.nii.gz")
    
    # Compare surfaces files (GIFTIs)
    surfaces_23 = list(out_23.rglob("*.surf.gii"))
    for surf_23 in surfaces_23:
        rel = surf_23.relative_to(out_23)
        surf_25 = out_25 / rel
        if surf_25.exists():
            compare_gifti_surfaces(surf_23, surf_25, qa_dir / "surfaces" / f"{surf_23.stem}_diff.gii")

    # Compare confound CSV
    confound_23 = next((out_23.rglob("*desc-confounds_regressors.tsv")), None)
    confound_25 = next((out_25.rglob("*desc-confounds_regressors.tsv")), None)
    if confound_23 and confound_25:
        compare_confounds(confound_23, confound_25, qa_dir / "confounds/confounds_diff.tsv")


    # Check HTML report sizes
    report_23 = next((out_23.rglob("*.html")), None)
    report_25 = next((out_25.rglob("*.html")), None)
    if report_23:
        check_html_report(report_23, qa_dir / "reports/report_23_size.txt")
    if report_25:
        check_html_report(report_25, qa_dir / "reports/report_25_size.txt")

    print("✅ Full migration QA complete!")

if __name__ == "__main__":
    main()
