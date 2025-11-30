#!/usr/bin/env python3
import os
import subprocess
from pathlib import Path

# ------------------------------
# CONFIGURATION
# ------------------------------
BIDS_DIR = "/scratch/hrasoanandrianina/braint_database"
OUT_DIR = "/scratch/hrasoanandrianina/derivatives/fmriprep25.2.0"
WORK_DIR = "/scratch/hrasoanandrianina/work"
LICENSE_FILE = "/scratch/hrasoanandrianina/containers/license.txt"
FMRIPREP_SIF = "/scratch/hrasoanandrianina/containers/fmriprep_25.2.0.sif"

SLURM_DIR = "./slurm_jobs"     # Where job scripts will be saved
N_THREADS = 24
OMP_THREADS = 8
MEM_GB = 64
TIME = "24:00:00"               # walltime
PARTITION = "skylake"            # SLURM partition name
RUN_ARRAY = True                # False = one job per subject
# ------------------------------

def get_subjects(bids_dir):
    """Find subjects (folders starting with 'sub-')."""
    subs = sorted([p.name for p in Path(bids_dir).glob("sub-*") if p.is_dir()])
    if not subs:
        raise RuntimeError("No subjects found in BIDS directory.")
    return subs

def make_slurm_script(subjects):
    """Generate the SLURM job script."""
    os.makedirs(SLURM_DIR, exist_ok=True)
    job_file = Path(SLURM_DIR) / "run_fmriprep.slurm"

    if RUN_ARRAY:
        array_directive = f"#SBATCH --array=1-{2*len(subjects)}"
        subj_access = 'SUB=${SUBJECTS[$SLURM_ARRAY_TASK_ID-1]}'
    else:
        array_directive = ""
        subj_access = "SUB=$1"  # passed manually

    for session_id in ['01', '02']:

        content = f"""#!/bin/bash
#SBATCH --job-name=fmriprep
#SBATCH --output=slurm_%A_%a.out
#SBATCH --error=slurm_%A_%a.err
#SBATCH --cpus-per-task={N_THREADS}
#SBATCH --mem={MEM_GB}G
#SBATCH --time={TIME}
#SBATCH --partition={PARTITION}
#SBATCH --mail-type=BEGIN,END
#SBATCH --mail-user=henitsoa.rasoanandrianina@adalab.fr
{array_directive}

SUBJECTS=({" ".join(subjects)})
{subj_access}

module purge
module load userspace/all

echo "Running fmriprep version 25.2.0 for subject: $SUB"

apptainer run \\
    -B {BIDS_DIR}:/data:ro \\
    -B {OUT_DIR}:/out \\
    -B {LICENSE_FILE}:/license.txt \\
    {FMRIPREP_SIF} \\
    /data /out participant \\
    --participant-label $SUB \\
    --session_label ses-{session_id} \\
    --fs-license-file /license.txt \\
    --bids-filter-file /home/hrasoanandrianina/bids_filter_ses-{session_id}.json \\
    --fd-spike-threshold 0.5 \\
    --dvars-spike-threshold 2.0 \\
    --cifti-output 91k \\
    --subject-anatomical-reference sessionwise \\
    --project-goodvoxels \\
    --fs-license-file /license.txt \\
    --output-spaces fsLR:den-32k T1w fsaverage:den-164k MNI152NLin6Asym \\
    --ignore slicetiming \\
    --mem-mb 25000 \\
    --skip-bids-validation \\
    --nthreads {N_THREADS} --omp-nthreads {OMP_THREADS} \\
    --work-dir {WORK_DIR} \\
    --stop-on-first-crash

echo "Done: $SUB"
"""
    job_file.write_text(content)
    print(f"Created SLURM job: {job_file}")
    return job_file


def submit(job_file, subjects):
    """Submit SLURM job(s)."""
    if RUN_ARRAY:
        print("Submitting as SLURM ARRAY job...")
        subprocess.run(["sbatch", str(job_file)], check=True)
    else:
        print("Submitting one job per subject...")
        for sub in subjects:
            subprocess.run(["sbatch", str(job_file), sub], check=True)


def main():
    subjects = get_subjects(BIDS_DIR)
    job_file = make_slurm_script(subjects)
    submit(job_file, subjects)


if __name__ == "__main__":
    main()
