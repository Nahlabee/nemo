#!/usr/bin/env python3
import sys
import subprocess
from pathlib import Path

# ------------------------------------------
# Add project root to PYTHONPATH
# ------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config
# ------------------------------
# CONFIGURATION
# ------------------------------

BIDS_DIR        = config.BIDS_DIR
OUT_MRIQC_DIR   = config.OUT_MRIQC_DIR
WORK_DIR        = config.WORK_DIR
SLURM_DIR       = config.SLURM_DIR
MRIQC_SIF       = config.MRIQC_SIF

N_THREADS       = int(config.SLURM_CPUS) // 2
MEM_GB          = config.SLURM_MEM
TIME            = config.SLURM_TIME
PARTITION       = config.SLURM_PARTITION
OMP_THREADS     = 2

# --------------------------------------------
# HELPERS
# --------------------------------------------  

def get_subjects(bids_dir):
    """Find subjects (folders starting with 'sub-')."""
    subs = sorted([p.name for p in Path(bids_dir).glob("sub-*") if p.is_dir()])
    if not subs:
        raise RuntimeError("No subjects found in BIDS directory.")
    return subs

def get_sessions(bids_dir, subject):
    """Find sessions for a given subject (folders starting with 'ses-')."""
    subj_path = Path(bids_dir) / subject
    sessions = sorted([p.name for p in subj_path.glob("ses-*") if p.is_dir()])
    
    return sessions if sessions else [None]  # Default to None if no sessions found

def mriqc_is_done(sub, ses):
    """
    Determines whether an MRIQC run for (subject, session) is complete.  
    MRIQC creates a group JSON *per modality*.
    We mark a subject as processed if at least one T1w JSON exists.
    """
    report_bold = Path(OUT_MRIQC_DIR) / f"{sub}" / f"{ses}" / f"{sub}_{ses}_task-rest_bold.html"
    report_T1w = Path(OUT_MRIQC_DIR) / f"{sub}" / f"{ses}" / f"{sub}_{ses}_T1w.html"
    report_DWI = Path(OUT_MRIQC_DIR) / f"{sub}" / f"{ses}" / f"{sub}_{ses}_dwi.html"
    
    if report_T1w.exists() or report_bold.exists() or report_DWI.exists():
        return True
    return False

# ------------------------------
# SLURM JOB SCRIPT GENERATION
# ------------------------------

def make_slurm_mriqc_script(subject, session_id):
    """Create a SLURM job script to run MRIQC for a given subject/session."""
    job_file = Path(SLURM_DIR) / f"mriqc_{subject}_{session_id}.slurm"
    
    content = f"""#!/bin/bash
#SBATCH --job-name=mriqc_{subject}_{session_id}
#SBATCH --output={SLURM_DIR}/mriqc_{subject}_{session_id}.out
#SBATCH --error={SLURM_DIR}/mriqc_{subject}_{session_id}.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={N_THREADS}
#SBATCH --mem={MEM_GB}
#SBATCH --time={TIME}
#SBATCH --partition={PARTITION}
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=henitsoa.rasoanandrianina@adalab.fr     

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
    WORK_DIR=$(mktemp -d /tmp/mriqc_${{SLURM_JOB_ID}}_XXXX)
fi

echo "Using WORK_DIR: $WORK_DIR"
mkdir -p $WORK_DIR

echo " --------------- Starting MRIQC for subject: {subject}, session: {session_id} ---------------"

apptainer run \
    --cleanenv \
    -B {BIDS_DIR}:/data:ro \
    -B {OUT_MRIQC_DIR}:/out \
    -B $WORK_DIR:/work \
    -B /scratch/hrasoanandrianina/code/nemo:/project \
    --env PYTHONPATH=/project \
    {MRIQC_SIF} \
    /data /out participant \
    --participant_label {subject} \
    --session-id {session_id} \
    --bids-filter-file /home/hrasoanandrianina/bids_filter_{session_id}.json \
    --nprocs {N_THREADS} \
    --omp-nthreads {OMP_THREADS} \
    --mem {MEM_GB} \
    -w $WORK_DIR \
    --fd_thres 0.5 \
    --verbose-reports \
    --no-datalad-get \
    --no-sub
    
echo "---------------- Finished MRIQC for subject: {subject}, session: {session_id} ---------------"
"""
    job_file.write_text(content)
    print(f"Created MRIQC SLURM job: {job_file} for subject {subject}, session {session_id}")
    return job_file

# ------------------------------
# MAIN JOB SUBMISSION LOGIC
# ------------------------------

def submit_mriqc_jobs():
    """Generate and submit SLURM job scripts for MRIQC."""
    Path(SLURM_DIR).mkdir(exist_ok=True)
    subjects = get_subjects(BIDS_DIR)

    for sub in subjects:
        sessions = get_sessions(BIDS_DIR, sub)
        for ses in sessions:
            # -----------------------
            # SKIP CHECK IF DONE
            # -----------------------
            if mriqc_is_done(sub, ses):
                print(f"✓ Skipping {sub} {ses}: already processed")
                continue
            # -----------------------

            job_script = make_slurm_mriqc_script(sub, ses)
            mriqc_job = subprocess.run(["sbatch", str(job_script)], capture_output=True, text=True)

            print(f"Submitted MRIQC job for subject {sub}, session {ses}: {mriqc_job.stdout.strip()}")

    print("\n All MRIQC jobs submitted.")

if __name__ == "__main__":
    submit_mriqc_jobs()