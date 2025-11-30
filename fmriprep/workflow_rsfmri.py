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

FMRIPREP_SIF = "/scratch/hrasoanandrianina/containers/fmriprep_25.2.0.sif"
XCPD_SIF = "/scratch/hrasoanandrianina/containers/xcp_d_0.12.0.sif"

OUT_FMRIPREP_DIR = "/scratch/hrasoanandrianina/derivatives/fmriprep_25.2.0"
OUT_XCPD_DIR = "/scratch/hrasoanandrianina/derivatives/xcp_d_0.12.0"

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

def get_sessions(bids_dir, subject):
    """Find sessions for a given subject (folders starting with 'ses-')."""
    subj_path = Path(bids_dir) / subject
    sessions = sorted([p.name for p in subj_path.glob("ses-*") if p.is_dir()])
    if not sessions:
        return [None] # No sessions found
    return [s.name for s in sessions]

def fmriprep_is_done(sub, ses):
    """
    Determines whether an fMRIPrep run for (subject, session) is complete.
    We consider it DONE if:
      1. The fMRIPrep HTML report exists, OR
      2. A valid anatomical output exists (T1w preprocessed file)
    """
    report = Path(OUT_FMRIPREP_DIR) / f"sub-{sub}" / f"ses-{ses}" / "sub-{}_ses-{}_desc-preproc_T1w.html".format(sub, ses)
    t1w = Path(OUT_FMRIPREP_DIR) / f"sub-{sub}" / f"ses-{ses}" / "anat" / f"sub-{sub}_ses-{ses}_desc-preproc_T1w.nii.gz"

    if report.exists() or t1w.exists():
        return True
    return False

def xcpd_is_done(sub, ses):
    """
    Determines whether an XCP-D run for (subject, session) is complete.
    We consider it DONE if:
      1. The XCP-D HTML report exists.
    """
    report = Path(OUT_XCPD_DIR) / f"sub-{sub}" / f"ses-{ses}" / "figures" / f"{sub}_{ses}_carpetplot.png"

    if report.exists():
        return True
    return False

# ------------------------------ Create SLURM job scripts ------------------------------
def make_slurm_fmriprep_script(subject, session_id):
    """Generate the SLURM job script."""
    os.makedirs(SLURM_DIR, exist_ok=True)
    job_file = Path(SLURM_DIR) / f"job_fmriprep_{subject}_{session_id}.slurm"

    content = f"""#!/bin/bash
#SBATCH --job-name=job_fmriprep_{subject}_{session_id}
#SBATCH --output={SLURM_DIR}/slurm_fmriprep_{subject}_{session_id}.out
#SBATCH --error={SLURM_DIR}/slurm_fmriprep_{subject}_{session_id}.err
#SBATCH --cpus-per-task={N_THREADS}
#SBATCH --mem={MEM_GB}G
#SBATCH --time={TIME}
#SBATCH --partition={PARTITION}
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=henitsoa.rasoanandrianina@adalab.fr


module purge
module load userspace/all
module load singularity
module load python3/3.12.0

echo "Running fmriprep version 25.2.0 for subject: {subject}, session: {session_id}"

apptainer run \\
    -B {BIDS_DIR}:/data:ro \\
    -B {OUT_FMRIPREP_DIR}:/out \\
    -B {LICENSE_FILE}:/license.txt \\
    {FMRIPREP_SIF} \\
    /data /out participant \\
    --participant-label {subject} \\
    --session_label {session_id} \\
    --fs-license-file /license.txt \\
    --bids-filter-file /home/hrasoanandrianina/bids_filter_{session_id}.json \\
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

echo "Finished fMRIPrep for subject: {subject}, session: {session_id}"
"""
    job_file.write_text(content)
    print(f"Created FMRIPREP SLURM job: {job_file}")
    return job_file

def make_slurm_xcpd_script(subject, session_id):
    """Generate the SLURM job script for XCP-D."""
    os.makedirs(SLURM_DIR, exist_ok=True)
    job_file = Path(SLURM_DIR) / f"job_xcpd_{subject}_{session_id}.slurm"

    content = f"""#!/bin/bash
#SBATCH --job-name=job_xcpd_{subject}_{session_id}
#SBATCH --output={SLURM_DIR}/slurm_xcpd_{subject}_{session_id}.out
#SBATCH --error={SLURM_DIR}/slurm_xcpd_{subject}_{session_id}.err
#SBATCH --cpus-per-task={N_THREADS}
#SBATCH --mem={MEM_GB}G
#SBATCH --time={TIME}
#SBATCH --partition={PARTITION}
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=henitsoa.rasoanandrianina@adalab.fr

module purge
module load userspace/all
module load singularity
module load python3/3.12.0

echo "Running XCP-D version 0.12.0 for subject: {subject}, session: {session_id}"

apptainer run \\
    --cleanenv \\
    -B {BIDS_DIR}:/data:ro \\
    -B {OUT_XCPD_DIR}:/out \\
    -B {WORK_DIR}:/work \\
    {XCPD_SIF} \\
    /data /out participant \\
    --mode abcd \\
    --motion-filter-type none \\
    --input-type fmriprep \\
    --participant-label {subject} \\
    --bids-filter-file /home/hrasoanandrianina/bids_filter_{session_id}.json \\
    --nuisance-regressors 36P \\
    --smoothing 4 \\
    --session-id {session_id} \\
    --despike \\
    --dummy-scans auto \\
    --linc-qc \\
    --abcc-qc \\

    --nthreads {N_THREADS} \\
    --omp-nthreads {OMP_THREADS} \\
    -w {WORK_DIR}  \\
    --stop-on-first-crash

echo "Finished XCP-D for subject: {subject}, session: {session_id}"
"""
    job_file.write_text(content)
    print(f"Created XCP-D SLURM job: {job_file}")
    return job_file


def submit_with_dependencies(subjects):
    
    for sub_path in subjects:
        sub = sub_path.name
        sessions = get_sessions(sub_path)

        print(f"\n=== Submitting jobs for {sub} ===")

        previous_job_id = None
        for ses in sessions:

            # -----------------------
            # SKIP CHECK IF BOTH DONE
            # -----------------------
            if fmriprep_is_done(sub, ses) and xcpd_is_done(sub, ses):
                print(f"✓ Skipping {sub} {ses}: already fully processed")
                continue
            # -----------------------

            # -----------------------
            # SUBMIT FMRIPREP JOB IF NEEDED
            # -----------------------
            if not fmriprep_is_done(sub, ses):
                print(f"Submitting fMRIPrep job for {sub} {ses}...")
                fmriprep_job_script = make_slurm_fmriprep_script(sub, ses)
                cmd = ["sbatch"]

                # Add dependency if this is not the first session
                if previous_job_id:
                    cmd += [f"--dependency=afterok:{previous_job_id}"]

                cmd.append(str(fmriprep_job_script))
                fmriprep_job = subprocess.run(cmd, capture_output=True, text=True, check=True)
                
                # Extract SLURM job ID (last token in "Submitted batch job 12345")
                job_id = fmriprep_job.stdout.strip().split()[-1]
                previous_job_id = job_id

                print(f"Submitted fMRIPrep for {sub} {ses} (Job ID: {job_id})")
            else:
                fmriprep_job = None
                print(f"✓ Skipping fMRIPrep for {sub} {ses}: already done")
            
            # -----------------------
            # SUBMIT XCP-D JOB IF NEEDED
            # -----------------------
            if not xcpd_is_done(sub, ses):
                print(f"Submitting XCP-D job for {sub} {ses}...")
                xcpd_job_script = make_slurm_xcpd_script(sub, ses)
                cmd = ["sbatch"]

                # Add dependency on fMRIPrep job if it was just submitted
                if fmriprep_job:
                    cmd = ["sbatch", f"--dependency=afterok:{fmriprep_job}", str(xcpd_job_script)]
                    print(f"Submitting XCP-D for {sub} {ses} with dependency {fmriprep_job}")
                else:
                    cmd = ["sbatch", str(xcpd_job_script)]
                    print(f"Submitting XCP-D for {sub} {ses} (fmriprep already done)")
                    
                xcpd_job = subprocess.run(cmd, capture_output=True, text=True, check=True)
                
                # Extract SLURM job ID (last token in "Submitted batch job 12345")
                job_id = xcpd_job.stdout.strip().split()[-1]
                previous_job_id = job_id

                print(f"Submitted XCP-D for {sub} {ses} (Job ID: {job_id})")
            else:
                print(f"✓ Skipping XCP-D for {sub} {ses}: already done")
    
        print(f"All sessions for {sub} queued.\n")


def main():
    subjects = get_subjects(BIDS_DIR)
    submit_with_dependencies(subjects)

if __name__ == "__main__":
    main()
