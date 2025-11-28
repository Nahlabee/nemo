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

from curses import version
import subprocess
from pathlib import Path
import argparse
import json
import pandas as pd # type: ignore
import nibabel as nib # type: ignore
import logging
import numpy as np # type: ignore
from rich.console import Console
console = Console()

# Configure logging
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("xcpd_postproc")

# ========================
# CONFIGURATION
# ========================

THREADS = 8

def run_fmriprep_subject(config_file, fmriprep_sif, bids_dir, out_dir, fs_license, subject, version, extra_flags=[]):
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
        "--nthreads", str(THREADS),
        "--skip-bids-validation",
        "--clean-workdir"
    ]
    # cmd.extend(extra_flags)
    print("Running command:", " ".join(cmd))
    subprocess.run(cmd, check=True)

def run_xcpd_subject(config_file, xcpd_sif, work_dir, fmriprep_dir, output_dir, subject):
    """Run XCP-D using Apptainer.
    
     Args:
        xcpd_sif (Path): Path to the XCP-D Singularity image.
        bids_dir (Path): Path to the BIDS dataset.
        fmriprep_dir (Path): Path to the fMRIPrep output directory.
        out_dir (Path): Path to the output directory.
        subject (str): Subject label (e.g., 'sub-01').
        version (str): XCP-D version string for logging.
        extra_flags (list): Additional command-line flags for XCP-D."""
    
    console.rule(f"[bold blue]Running XCP-D for {subject} ({xcpd_sif.name})[/]")

    cmd = [
    "apptainer", "run", "--cleanenv",
    "-B", f"{str(fmriprep_dir)}:/data:ro",
    "-B", f"{str(output_dir)}:/out",
    "-B", f"{str(work_dir)}:/work",
    str(xcpd_sif),
    "/data", "/out", "participant",
    "--mode", "abcd",
    "--motion-filter-type", "none",
    "--input-type", "fmriprep",
    "--participant-label", subject,
    "--bids-filter-file", "/home/henit/fmriprep_data/bids_dir/bids_filter.json",
    
    "--nuisance-regressors", "36P",
    "--smoothing", "4",
    "--despike",
    "--dummy-scans", "auto",
    "--linc-qc",
    "--abcc-qc",

    "--nthreads", str(THREADS),
    "--omp-nthreads", str(THREADS),

    "-w", "/work",                      # must be writable!
    "--stop-on-first-crash"
]
    console.print(f"[cyan]Executing:[/]\n{' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    console.print(f"[green]✔ Completed XCP-D for {subject} using {xcpd_sif.name}[/]")


def run_rsfmri_wf_subject(config_file, bids_dir, container_dir, output_dir, subject):
    """Run RS-fMRI workflow using Apptainer.
    
     Args:
        bids_dir (Path): Path to the BIDS dataset.
        output_dir (Path): Path to the output directory.
        subject (str): Subject label (e.g., 'sub-01').
        version (str): RS-fMRI version string for logging.
        extra_flags (list): Additional command-line flags for RS-fMRI."""
    
    fmriprep_sif = Path( container_dir / "fmriprep_25.2.0.sif")
    xcpd_sif = Path(container_dir / "xcp_d_0.12.0.sif")
    fs_license = Path(container_dir / "license.txt")
    work_dir = Path("/work") / subject


    output_dir_fmriprep = output_dir / "fmriprep"
    output_dir_fmriprep.mkdir(parents=True, exist_ok=True)     

    output_dir_xcpd = output_dir / "xcpd"
    output_dir_xcpd.mkdir(parents=True, exist_ok=True)
     
    fmriprep_dir = output_dir_fmriprep / subject

    console.rule(f"[bold blue]Running RS-fMRI workflow for {subject})[/]")

    run_fmriprep_subject(config_file, fmriprep_sif, bids_dir, output_dir_fmriprep, fs_license, subject)
    run_xcpd_subject(config_file, xcpd_sif, work_dir, fmriprep_dir, output_dir, subject)
    
    console.print(f"[green]✔ Completed RS-fMRI workflow for {subject}[/]")