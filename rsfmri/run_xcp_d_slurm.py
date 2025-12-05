#!/usr/bin/env python3
"""
Run XCP-D via SLURM job submission
Author: Henitsoa RASOANANDRIANINA
Date: 2025-10-22
Usage:
    python run_xcp_d_slurm.py

    """
import os, sys
import subprocess
from pathlib import Path
from utils.utils_helpers import get_subjects, get_sessions
from run_fmriprep_slurm import fmriprep_is_done
from config_loader import load_config

cfg = load_config()

# ------------------------------
# CONFIGURATION
# ------------------------------
BIDS_DIR        = cfg["project"]["bids_dir"]
WORK_DIR        = cfg["project"]["work_dir"]
SLURM_DIR       = cfg["project"]["slurm_dir"]
BIDS_FILTER_DIR = cfg["project"]["bids_filter_dir"]

XCPD_SIF        = cfg["xcp_d"]["sif"]
OUT_XCPD_DIR    = cfg["xcp_d"]["output_dir"]

N_THREADS       = cfg["partition"]["reserved_cpus"]
OMP_THREADS     = cfg["partition"]["omp_threads"]
MEM_GB          = cfg["partition"]["mem_gb"]
TIME            = cfg["partition"]["time"]               # walltime
PARTITION       = cfg["partition"]["name"]            # SLURM partition name
MAIL            = cfg["partition"]["mail"]
MAIL_FREQ       = cfg["partition"]["mail_freq"]

# ------------------------------
# HELPERS
# ------------------------------

def xcpd_is_done(sub, ses):
    """
    Determines whether an XCP-D run for (subject, session) is complete.
    We consider it DONE if:
      1. The XCP-D HTML report exists.
    """
    report = Path(OUT_XCPD_DIR) / f"{sub}" / f"{ses}"  / f"{sub}_{ses}.html"
    executive_summary = Path(OUT_XCPD_DIR) / f"{sub}" / f"{ses}" / f"{sub}_{ses}_executive_summary.html"

    if report.exists() or executive_summary.exists():
        return True
    return False

# -----------------------
# Create SLURM job scripts
# -----------------------

def make_slurm_xcpd_script(subject, session_id):
    """Generate the SLURM job script for XCP-D."""
    os.makedirs(SLURM_DIR, exist_ok=True)
    job_file = Path(SLURM_DIR) / f"slurm_xcpd_{subject}_{session_id}.slurm"

    content = f"""#!/bin/bash
#SBATCH --job-name=slurm_xcpd_{subject}_{session_id}
#SBATCH --output={SLURM_DIR}/slurm_xcpd_{subject}_{session_id}_%j.out
#SBATCH --error={SLURM_DIR}/slurm_xcpd_{subject}_{session_id}_%j.err
#SBATCH --cpus-per-task={N_THREADS}
#SBATCH --mem={MEM_GB}
#SBATCH --time={TIME}
#SBATCH --partition={PARTITION}
#SBATCH --mail-type={MAIL_FREQ}
#SBATCH --mail-user={MAIL}

module purge
module load userspace/all
module load singularity
module load python3/3.12.0

hostname

# Choose writable scratch directory
if [ -n "$SLURM_TMPDIR" ]; then
    WORK_DIR="$SLURM_TMPDIR"
elif [ -n "$TMPDIR" ]; then
    WORK_DIR="$TMPDIR"
else
    WORK_DIR=$(mktemp -d /tmp/{XCPD_SIF}/{subject}_{session_id})
fi

echo "Using WORK_DIR: $WORK_DIR"
mkdir -p $WORK_DIR

echo "------------ Running {XCPD_SIF} for subject: {subject}, session: {session_id} ---------------"

apptainer run --cleanenv \
    -B {BIDS_DIR}:/data:ro \
    -B {OUT_XCPD_DIR}:/out \
    -B {WORK_DIR}:/work \
    {XCPD_SIF} \
    /data /out participant \
    --mode abcd \
    --motion-filter-type none \
    --input-type fmriprep \
    --participant-label {subject} \
    --bids-filter-file {BIDS_FILTER_DIR}/bids_filter_{session_id}.json \
    --nuisance-regressors 36P \
    --smoothing 4 \
    --session-id {session_id} \
    --despike \
    --dummy-scans auto \
    --linc-qc \
    --abcc-qc \

    --nthreads {N_THREADS} \
    --omp-nthreads {OMP_THREADS} \
    -w {WORK_DIR}  \
    --stop-on-first-crash

echo "Finished XCP-D for subject: {subject}, session: {session_id}"
"""
    job_file.write_text(content)
    print(f"Created XCP-D SLURM job: {job_file}")
    return job_file


def submit_with_dependencies(subjects):
    
    previous_job_id = None
    for sub in subjects:
        sessions = get_sessions(BIDS_DIR, sub)

        for ses in sessions:
            # -----------------------
            # SUBMIT XCP-D JOB IF NEEDED
            # -----------------------
            if not xcpd_is_done(sub, ses):

                # Add dependency on fMRIPrep job if it was just submitted
                if fmriprep_is_done(sub, ses):

                    xcpd_job_script = make_slurm_xcpd_script(sub, ses)
                    xcpd_job = subprocess.run(["sbatch", str(xcpd_job_script)], capture_output=True, text=True, check=True)
                    job_id = xcpd_job.stdout.strip().split()[-1]
                    print(f"Submitted XCP-D for {sub} {ses} (Job ID: {job_id})")
                
                else:
                    print(f" Skipping XCP-D for {sub} {ses} (!!!!! perform fmriprep first)")
                    continue 
            else:
                print(f"✓ Skipping XCP-D for {sub} {ses}: already done")
        print(f"All XCP_D sessions for {sub} queued.\n")


def main():
    subjects = get_subjects(BIDS_DIR)
    submit_with_dependencies(subjects)

if __name__ == "__main__":
    main()
