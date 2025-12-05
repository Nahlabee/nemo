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

def is_already_processed(subject, session, data_type="raw"):
    """
    Check if subject_session is already processed successfully.

    Parameters
    ----------
    subject : str
        Subject identifier (e.g., "sub-01").
    session : str
        Session identifier (e.g., "ses-01").
    data_type : str
        Type of data to process (e.g., "raw" or "fmriprep" or "qsiprep").

    Returns
    -------
    bool
        True if already processed, False otherwise.
    """

    # Check if mriqc already processed without error
    if data_type not in ["raw", "fmriprep", "xcp_d","qsiprep", "qsirecon"]:
        raise ValueError(f"Invalid data_type: {data_type}. Must be 'raw', 'fmriprep', or 'qsiprep'.")
    
    stdout_dir = f"{config_files.config['common']['derivatives']}/mriqc_{data_type}/stdout"
    if not os.path.exists(stdout_dir):
        print(f"[MRIQC] Could not read standard outputs from MRIQC, recomputing ....")
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
def generate_slurm_mriqc_script(subject, session, data_type="raw", job_ids=None):
    """Generate the SLURM job script.
    Parameters
    ----------
   
    subject : str
        Subject identifier.
    session : str
        Session identifier.
    data_type : str
        Type of data to process (e.g., "raw" or "fmriprep" or "qsiprep").
    job_ids : list, optional
        List of SLURM job IDs to set as dependencies (default is None).
    """

    # Create output (derivatives) directories
    os.makedirs(f"{config_files.config['common']['derivatives']}/mriqc_{data_type}", exist_ok=True)
    os.makedirs(f"{config_files.config['common']['derivatives']}/mriqc_{data_type}/stdout", exist_ok=True)
    os.makedirs(f"{config_files.config['common']['derivatives']}/mriqc_{data_type}/scripts", exist_ok=True)

    if job_ids is None:
        job_ids = []

    header = (
        f'#!/bin/bash\n'
        f'#SBATCH --job-name=mriqc_{subject}_{session}\n'
        f'#SBATCH --output={config_files.config["common"]["derivatives"]}/mriqc_{data_type}/stdout/mriqc_{subject}_{session}_%j.out\n'
        f'#SBATCH --error={config_files.config["common"]["derivatives"]}/mriqc_{data_type}/stdout/mriqc_{subject}_{session}_%j.err\n'
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
        f'echo "Using OUT_MRIQC_DIR = {config_files.config["common"]["derivatives"]}/mriqc_{data_type}"\n'
    )

    # Define the Singularity command for running MRIQC
    # Note: Unlike fmriprep, no config file is used here, the option doesn't exist for mriqc
    singularity_cmd = (
        f'\napptainer run \\\n'
        f'    --cleanenv \\\n'
        f'    -B {config_files.config["common"]["input_dir"]}:/data:ro \\\n'
        f'    -B {config_files.config["common"]["derivatives"]}/mriqc_{data_type}:/out \\\n'
        f'    {config_files.config["mriqc"]["container"]} /data /out participant \\\n'
        f'    --participant_label {subject} \\\n'
        f'    --session-id {session} \\\n'
        f'    --bids-filter-file /bids_filter_dir/bids_filter_{session}.json \\\n'
        f'    --mem {config_files.config["mriqc"]["requested_mem"]} \\\n'
        f'    -w $TMP_WORK_DIR \\\n'
        f'    --fd_thres 0.5 \\\n'
        f'    --verbose-reports \\\n'
        f'    --no-datalad-get \\\n'
        f'    --no-sub\n'
    )

    save_work = (
        f'\necho "Cleaning up temporary work directory..."\n'
        f'\nchmod -Rf 771 {config_files.config["common"]["derivatives"]}/mriqc_{data_type}\n'
        f'\ncp -r $TMP_WORK_DIR/* {config_files.config["common"]["derivatives"]}/mriqc_{data_type}/work\n'
        f'\nrm -rf $TMP_WORK_DIR\n' 
        f'echo "Finished MRIQC for subject: {subject}, session: {session}"\n'
    )
    
    # Combine all parts into the final script 
    path_to_script = f"{config_files.config['common']['derivatives']}/mriqc_{data_type}/scripts/{subject}_{session}_mriqc.slurm"

    with open(path_to_script, 'w') as f:
        f.write(header + module_export + tmp_dir_setup + singularity_cmd + save_work)
    print(f"Created MRIQC SLURM job: {path_to_script} for subject {subject}, session {session}")

    return path_to_script

# ------------------------------
# MAIN JOB SUBMISSION LOGIC
# ------------------------------

def submit_mriqc_jobs(input_dir, data_type="raw", job_ids=None):
    """Generate and submit SLURM job scripts for MRIQC.
    Parameters
    ----------
    input_dir : str
        Path to the BIDS formatted input directory.
    data_type : str
        Type of data to process (e.g., "raw" or "fmriprep" or "qsiprep").
    job_ids : list, optional
        List of SLURM job IDs to set as dependencies (default is None).
    """

    subjects = utils.get_subjects(input_dir)
    
    if job_ids is None:
        job_ids = []

    for sub in subjects:
        previous_job_id = None
        sessions = utils.get_sessions(input_dir, sub)
        
        for ses in sessions:
            # -----------------------
            # SKIP CHECK IF BOTH DONE
            # -----------------------
            if is_already_processed(sub, ses) :
                print(f"✓ Skipping {sub} {ses}: already processed")
                continue
            # -----------------------

            # -----------------------
            # SUBMIT FMRIPREP JOB IF NEEDED
            # -----------------------
            else :
                print(f"Submitting MRIQC job for {sub} {ses}...")

                # Add dependency if this is not the first job in the chain
                path_to_script = generate_slurm_mriqc_script(sub, ses, data_type=data_type, job_ids=[previous_job_id])
                cmd = f"sbatch {path_to_script}"

                # Extract SLURM job ID (last token in "Submitted batch job 12345")
                job_id = utils.submit_job(cmd)
                previous_job_id = job_id
        print(f"All FMRIPREP sessions for {sub} queued.\n")

if __name__ == "__main__":
    submit_mriqc_jobs(config_files.config["common"]["input_dir"], data_type="raw")