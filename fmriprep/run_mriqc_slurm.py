#!/usr/bin/env python3
import os
import subprocess
from pathlib import Path

# ------------------------------
# CONFIGURATION
# ------------------------------
BIDS_DIR = "/scratch/hrasoanandrianina/braint_database"
WORK_DIR = "/scratch/hrasoanandrianina/work"
LICENSE_FILE = "/scratch/hrasoanandrianina/containers/license.txt"

MRIQC_SIF = "/scratch/hrasoanandrianina/containers/mriqc_24.0.2.sif"
OUT_MRIQC_DIR = "/scratch/hrasoanandrianina/derivatives/mriqc_24.0.2"

SLURM_DIR = "./slurm_jobs"     # Where job scripts will be saved
N_THREADS = 24
OMP_THREADS = 8
MEM_GB = 64
TIME = "24:00:00"               # walltime
PARTITION = "skylake"            # SLURM partition name

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
    if not sessions:
        return [None] # No sessions found
    return [s.name for s in sessions]

def mriqc_is_done(sub, ses):
    """
    Determines whether an MRIQC run for (subject, session) is complete.  MRIQC creates a group JSON *per modality*.
    We mark a subject as processed if at least one T1w JSON exists.
    """
    report = Path(OUT_MRIQC_DIR) / f"sub-{sub}" / f"ses-{ses}" / "sub-{}_ses-{}_task-rest_bold.html".format(sub, ses)

    if report.exists():
        return True
    return False

def make_slurm_mriqc_script(subject, session_id):
    """Create a SLURM job script to run MRIQC for a given subject and session."""
    job_file = Path(SLURM_DIR) / f"mriqc_sub-{subject}_ses-{session_id}.slurm"
    content = f"""#!/bin/bash
#SBATCH --job-name=mriqc_sub-{subject}_ses-{session_id}
#SBATCH --output={SLURM_DIR}/mriqc_sub-{subject}_ses-{session_id}.out
#SBATCH --error={SLURM_DIR}/mriqc_sub-{subject}_ses-{session_id}.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={N_THREADS}
#SBATCH --mem={MEM_GB}G
#SBATCH --time={TIME}
#SBATCH --partition={PARTITION}
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=henitsoa.rasoanandrianina@adalab.fr     

echo "Starting MRIQC for subject: {subject}, session: {session_id}"

apptainer run 
    --cleanenv \\
    -B {BIDS_DIR}:/data:ro \\
    -B {OUT_MRIQC_DIR}:/out \\
    -B {WORK_DIR}:/work \\
    {MRIQC_SIF} \\
    /data /out participant \\
    --participant_label {subject} \\
    --session-id {session_id} \\
    --bids-filter-file /home/hrasoanandrianina/bids_filter_ses-{session_id}.json \\
    --n_procs {N_THREADS} \\
    --omp-nthreads {OMP_THREADS} \\
    -w {WORK_DIR} \\
    --fd_thres 0.5 \\
    --verbose-reports \\
    --no-sub
    
echo "Finished MRIQC for subject: {subject}, session: {session_id}"
"""
    job_file.write_text(content)
    print(f"Created MRIQC SLURM job: {job_file} for subject {subject}, session {session_id}")
    return job_file

# ------------------------------
# MAIN SCRIPT
# ------------------------------

def submit_mriqc_jobs():
    """Generate and submit SLURM job scripts for MRIQC."""
    os.makedirs(SLURM_DIR, exist_ok=True)
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

            print(f"Submitted MRIQC job for subject {sub}, session {ses}")
            print(mriqc_job.stdout.strip())

    print("\n All MRIQC jobs submitted.")

if __name__ == "__main__":
    submit_mriqc_jobs()