#!/usr/bin/env python3
import os, sys
import subprocess
from pathlib import Path
from utils.utils_helpers import get_subjects, get_sessions
from config_loader import load_config

cfg = load_config()

# ------------------------------
# CONFIGURATION
# ------------------------------
BIDS_DIR        = cfg["project"]["bids_dir"]
WORK_DIR        = cfg["project"]["work_dir"]
SLURM_DIR       = cfg["project"]["slurm_dir"]
BIDS_FILTER_DIR = cfg["project"]["bids_filter_dir"]

FMRIPREP_SIF       = cfg["fmriprep"]["sif"]
OUT_FMRIPREP_DIR   = cfg["fmriprep"]["output_dir"]
FS_LICENSE         = cfg["fmriprep"]["fs_license"]

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

def fmriprep_is_done(sub, ses):
    """
    Determines whether an fMRIPrep run for (subject, session) is complete.
    We consider it DONE if:
      1. The fMRIPrep HTML report exists, OR
      2. A valid anatomical output exists (T1w preprocessed file)
    """
    report = Path(OUT_FMRIPREP_DIR) / f"{sub}.html"

    t1w = Path(OUT_FMRIPREP_DIR) / f"{sub}" / f"{ses}" / "anat" / f"{sub}_{ses}_desc-preproc_T1w.nii.gz"

    if report.exists() or t1w.exists():
        return True
    return False

# ------------------------------ Create SLURM job scripts ------------------------------
def make_slurm_fmriprep_script(subject, session_id):
    """Generate the SLURM job script."""
    os.makedirs(SLURM_DIR, exist_ok=True)
    job_file = Path(SLURM_DIR) / f"slurm_fmriprep_{subject}_{session_id}.slurm"

    content = f"""#!/bin/bash
#SBATCH --job-name=slurm_fmriprep_{subject}_{session_id}
#SBATCH --output={SLURM_DIR}/slurm_fmriprep_{subject}_{session_id}_%j.out
#SBATCH --error={SLURM_DIR}/slurm_fmriprep_{subject}_{session_id}_%j.err
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

echo "------------ Running {FMRIPREP_SIF} for subject: {subject}, session: {session_id} ---------------"

apptainer run --cleanenv \
    -B {BIDS_DIR}:/data:ro \
    -B {OUT_FMRIPREP_DIR}:/out \
    -B {FS_LICENSE}:/license.txt \
    -B $WORK_DIR:/work \
    -B /scratch/hrasoanandrianina/code/nemo:/project \
    --env PYTHONPATH=/project \
    {FMRIPREP_SIF} \
    /data /out participant \
    --participant-label {subject} \
    --session-label {session_id} \
    --fs-license-file /license.txt \
    --bids-filter-file /home/hrasoanandrianina/bids_filter_{session_id}.json \
    --fd-spike-threshold 0.5 \
    --dvars-spike-threshold 2 \
    --cifti-output 91k \
    --subject-anatomical-reference sessionwise \
    --project-goodvoxels \
    --output-spaces fsLR:den-32k T1w fsaverage:den-164k MNI152NLin6Asym \
    --ignore slicetiming \
    --mem-mb {MEM_GB} \
    --skip-bids-validation \
    --nthreads {N_THREADS} --omp-nthreads {OMP_THREADS} \
    --work-dir $WORK_DIR \
    --stop-on-first-crash

echo "Finished fMRIPrep for subject: {subject}, session: {session_id}"
"""
    job_file.write_text(content)
    print(f"Created FMRIPREP SLURM job: {job_file}")
    return job_file

def submit_with_dependencies(subjects):
    
    previous_job_id = None
    for sub in subjects:
        sessions = get_sessions(BIDS_DIR, sub)

        print(f"\n=== Submitting jobs for {sub} ===")
        for ses in sessions:

            # -----------------------
            # SKIP CHECK IF BOTH DONE
            # -----------------------
            if fmriprep_is_done(sub, ses) :
                print(f"✓ Skipping {sub} {ses}: already processed")
                continue
            # -----------------------

            # -----------------------
            # SUBMIT FMRIPREP JOB IF NEEDED
            # -----------------------
            else :
                print(f"Submitting fMRIPrep job for {sub} {ses}...")
                fmriprep_job_script = make_slurm_fmriprep_script(sub, ses)
                cmd = ["sbatch"]

                # Add dependency if this is not the first job in the chain
                if previous_job_id:
                    cmd += [f"--dependency=afterok:{previous_job_id}"]

                cmd.append(str(fmriprep_job_script))
                fmriprep_job = subprocess.run(cmd, capture_output=True, text=True, check=True)
                
                # Extract SLURM job ID (last token in "Submitted batch job 12345")
                job_id = fmriprep_job.stdout.strip().split()[-1]
                previous_job_id = job_id

                print(f"Submitted fMRIPrep for {sub} {ses} (Job ID: {job_id})")               
        print(f"All FMRIPREP sessions for {sub} queued.\n")

def main():
    subjects = get_subjects(BIDS_DIR)
    submit_with_dependencies(subjects)

if __name__ == "__main__":
    main()
