#!/usr/bin/env python3
import os, sys
import subprocess
from datetime import datetime
from types import SimpleNamespace
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
import utils
import config_files

# --------------------------------------------
# HELPERS
# --------------------------------------------  

def is_already_processed(subject, session):
    """
    Check if subject_session is already processed successfully.
    Note: Even if FMRIprep put files in cache, some steps are recomputed which require several hours of ressources.

    Parameters
    ----------
    subject : str
        Subject identifier (e.g., "sub-01").
    session : str
        Session identifier (e.g., "ses-01").

    Returns
    -------
    bool
        True if already processed, False otherwise.
    """

    # Check if fmriprep already processed without error
    stdout_dir = f"{config_files.config['common']['derivatives']}/mriqc/stdout"
    if not os.path.exists(stdout_dir):
        return False

    prefix = f"mriqc_{subject}_{session}"
    stdout_files = [f for f in os.listdir(stdout_dir) if (f.startswith(prefix) and f.endswith('.out'))]
    if not stdout_files:
        return False

    for file in stdout_files:
        file_path = os.path.join(stdout_dir, file)
        with open(file_path, 'r') as f:
            if 'MRIQC completed' in f.read():
                print(f"[MRIQC] Skip already processed subject {subject}_{session}")
                return True

    return False

# ------------------------
# Create SLURM job scripts 
# ------------------------
def generate_slurm_mriqc_script(subject, session, job_ids=None):
    """Generate the SLURM job script.
    Parameters
    ----------
   
    subject : str
        Subject identifier.
    session : str
        Session identifier.
    job_ids : list, optional
        List of SLURM job IDs to set as dependencies (default is None).
    """

    # Create output (derivatives) directories
    os.makedirs(f"{config_files.config['common']['derivatives']}/mriqc", exist_ok=True)
    os.makedirs(f"{config_files.config['common']['derivatives']}/mriqc/stdout", exist_ok=True)
    os.makedirs(f"{config_files.config['common']['derivatives']}/mriqc/scripts", exist_ok=True)

    if job_ids is None:
        job_ids = []

    header = (
        f'#!/bin/bash\n'
        f'#SBATCH --job-name=mriqc_{subject}_{session}\n'
        f'#SBATCH --output={config_files.config["common"]["derivatives"]}/mriqc/stdout/mriqc_{subject}_{session}_%j.out\n'
        f'#SBATCH --error={config_files.config["common"]["derivatives"]}/mriqc/stdout/mriqc_{subject}_{session}_%j.err\n'
        f'#SBATCH --cpus-per-task={config_files.config["mriqc"]["requested_cpus"]}\n'
        f'#SBATCH --mem={config_files.config["mriqc"]["requested_mem"]}\n'
        f'#SBATCH --time={config_files.config["mriqc"]["requested_time"]}\n'
        f'#SBATCH --partition={config_files.config["mriqc"]["partition"]}\n'
    )

    if job_ids:
        header += (
            f'#SBATCH --dependency=afterok:{":".join(job_ids)}\n'
        )

    if config_files.config["common"].get("email"):
        header += (
            f'#SBATCH --mail-type={config_files.config["common"]["email_frequency"]}\n'
            f'#SBATCH --mail-user={config_files.config["common"]["email"]}\n'
        )

    if config_files.config["common"].get("account"):
        header += f'#SBATCH --account={config_files.config["common"]["account"]}\n'

    module_export = (
        f'\nmodule purge\n'
        f'module load userspace/all\n'
        f'module load singularity\n'

        f'echo "------ Running {config_files.config["mriqc"]["container"]} for subject: {subject}, session: {session} --------"\n'
    )

    tmp_dir_setup = (
        f'\nhostname\n'
        f'# Choose writable scratch directory\n'
        f'if [ -n "$SLURM_TMPDIR" ]; then\n'
        f'    TMP_WORK_DIR="$SLURM_TMPDIR"\n'
        f'elif [ -n "$TMPDIR" ]; then\n'
        f'    TMP_WORK_DIR="$TMPDIR"\n'
        f'else\n'
        f'    TMP_WORK_DIR=$(mktemp -d /tmp/mriqc_{subject}_{session})\n'
        f'fi\n'
        f'mkdir -p $WORK_DIR\n'
        f'echo "Using TMP_WORK_DIR = $TMP_WORK_DIR"\n'
        f'echo "Using OUT_MRIQC_DIR = {config_files.config["common"]["derivatives"]}/mriqc"\n'
    )

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
    {MRIQC_SIF} \
    /data /out participant \
    --participant_label {subject} \
    --session-id {session_id} \
    --bids-filter-file {BIDS_FILTER_DIR}/bids_filter_{session_id}.json \
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