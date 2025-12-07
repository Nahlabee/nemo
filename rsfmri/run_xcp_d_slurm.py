#!/usr/bin/env python3
"""
Run XCP-D via SLURM job submission
Author: Henitsoa RASOANANDRIANINA
Date: 2025-10-22
Usage:
    python run_xcp_d_slurm.py

    """
#!/usr/bin/env python3
import os, sys
import subprocess
from datetime import datetime
from types import SimpleNamespace
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
import utils
import config_files
from rsfmri.run_fmriprep_slurm import is_already_processed as fmriprep_is_already_processed


# -------------------------------
# Load configuration
# -------------------------------
common = config_files.config["common"]
xcp_d = config_files.config["xcp_d"]

BIDS_DIR = common["input_dir"]
DERIVATIVES_DIR = common["derivatives"]

# ------------------------------
# HELPERS
# ------------------------------

def is_already_processed(subject, session):
    """
    Check if subject_session is already processed successfully.
    if not, also check that prerequisites are met: FMRIprep is done.

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

    # Check if xcp_d already processed without error
    stdout_dir = f"{DERIVATIVES_DIR}/xcp_d/stdout"
    if not os.path.exists(stdout_dir):    
        return False

        
    prefix = f"xcp_d_{subject}_{session}"
    stdout_files = [f for f in os.listdir(stdout_dir) if (f.startswith(prefix) and f.endswith('.out'))]
    if not stdout_files:
        return False

    for file in stdout_files:
        file_path = os.path.join(stdout_dir, file)
        with open(file_path, 'r') as f:
            if 'XCP-D finished successfully!' in f.read():
                print(f"[XCP-D] Skip already processed subject {subject}_{session}")
                return True
            else:    
                return False
                
# -----------------------

# -----------------------
# Generate SLURM job scripts
# -----------------------

def generate_slurm_xcpd_script(subject, session, path_to_script, job_ids=None):
    """Generate the SLURM job script.
    Parameters
    ----------
    
    subject : str
            Subject identifier.
    session : str
            Session identifier.
    path_to_script : str
            Path where the SLURM script will be saved.
    job_ids : list, optional
            List of SLURM job IDs to set as dependencies (default is None).
    """

    header = (
        f'#!/bin/bash\n'
        f'#SBATCH --job-name=xcp_d_{subject}_{session}\n'
        f'#SBATCH --output={DERIVATIVES_DIR}/xcp_d/stdout/xcp_d_{subject}_{session}_%j.out\n'
        f'#SBATCH --error={DERIVATIVES_DIR}/xcp_d/stdout/xcp_d_{subject}_{session}_%j.err\n'
        f'#SBATCH --mem={xcp_d["requested_mem"]}\n'
        f'#SBATCH --time={xcp_d["requested_time"]}\n'
        f'#SBATCH --partition={xcp_d["partition"]}\n'
    )

    if job_ids:
          valid_ids = [jid for jid in job_ids if jid]
          if valid_ids:
            header += f'#SBATCH --dependency=afterok:{":".join(valid_ids)}\n'
            
    if config_files.config["common"].get("email"):
        header += (
            f'#SBATCH --mail-type={common["email_frequency"]}\n'
            f'#SBATCH --mail-user={common["email"]}\n'
        )

    if config_files.config["common"].get("account"):
        header += f'#SBATCH --account={common["account"]}\n'

    module_export = (
        f'\nmodule purge\n'
        f'module load userspace/all\n'
        f'module load singularity\n'

        f'echo "------ Running {xcp_d["xcp_d_container"]} for subject: {subject}, session: {session} --------"\n'
    )

    tmp_dir_setup = (
        f'\nhostname\n'
        f'# Choose writable scratch directory\n'
        f'if [ -n "$SLURM_TMPDIR" ]; then\n'
        f'    TMP_WORK_DIR="$SLURM_TMPDIR"\n'
        f'elif [ -n "$TMPDIR" ]; then\n'
        f'    TMP_WORK_DIR="$TMPDIR"\n'
        f'else\n'
        f'    TMP_WORK_DIR=$(mktemp -d /tmp/xcp_d_{subject}_{session})\n'
        f'fi\n'
        f'mkdir -p "$TMP_WORK_DIR"\n'
        f'echo "Using TMP_WORK_DIR = $TMP_WORK_DIR"\n'
        f'echo "Using OUT_XCPD_DIR = {DERIVATIVES_DIR}/xcp_d"\n'
    )
    
    # Define the Singularity command for running FMRIPrep
    singularity_command = (
        f'\napptainer run --cleanenv \\\n'
        f'    -B {xcp_d["xcp_d_input_dir"]}:/data:ro \\\n'
        f'    -B {DERIVATIVES_DIR}/xcp_d/outputs:/out \\\n'
        f'    -B {common["freesurfer_license"]}:/license.txt \\\n'
        f'    -B {xcp_d["bids_filter_dir"]}:/bids_filter_dir\\\n'
        f'    -B {xcp_d["xcp_d_config"]}:/xcp_d_config.toml \\\n'
        f'    {xcp_d["xcp_d_container"]} /data /out participant \\\n'
        f'      --input-type fmriprep \\\n'
        f'      --participant-label {subject} \\\n'
        f'      --session-id {session} \\\n'
        f'      --fs-license-file /license.txt \\\n'
        f'      --mode abcd\\\n'
        f'      --motion-filter-type none\\\n'
        f'      --bids-filter-file /bids_filter_dir/bids_filter_{session}.json \\\n'
        f'      --nuisance-regressors 36P \\\n'
        f'      --work-dir $TMP_WORK_DIR \\\n'
        f'      --config-file /xcp_d_config.toml \\\n'

    )

    save_work = (
        f'\necho "Cleaning up temporary work directory..."\n'
        f'\nchmod -Rf 771 {DERIVATIVES_DIR}/xcp_d\n'
        f'\ncp -r $TMP_WORK_DIR/* {DERIVATIVES_DIR}/xcp_d/work\n'
        f'echo "Finished XCP-D for subject: {subject}, session: {session}"\n'
    )

    # Write the complete SLURM script to the specified file
    with open(path_to_script, 'w') as f:
        f.write(header + module_export + tmp_dir_setup + singularity_command + save_work)
    print(f"Created xcp_d SLURM job: {path_to_script} for subject {subject}, session {session}")


def run_xcpd(subject, session, job_ids=None):
    
    """
    Run the XCP-D for a given subject and session.
    Parameters
    ----------
    subject : str
        Subject identifier.
    session : str
        Session identifier.
    job_ids : list, optional
        List of SLURM job IDs to set as dependencies (default is None).
    
    Returns
    -------
    str or None
        SLURM job ID if the job is submitted successfully, None otherwise.
    """
            
    # Create output (derivatives) directories
    os.makedirs(f"{DERIVATIVES_DIR}/xcp_d", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/xcp_d/outputs", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/xcp_d/stdout", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/xcp_d/scripts", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/xcp_d/work", exist_ok=True)
    
    if is_already_processed(subject, session):
        return None
    
    if job_ids is None:
        job_ids = []
    
    path_to_script = f"{DERIVATIVES_DIR}/xcp_d/scripts/{subject}_{session}_xcp_d.slurm"
    generate_slurm_xcpd_script(subject, session, path_to_script, job_ids=job_ids)
                
    cmd = f"sbatch {path_to_script}"
                
    # Extract SLURM job ID (last token in "Submitted batch job 12345")
    job_id = utils.submit_job(cmd)
    print(f"[XCP_D] Submitting job {cmd}\n")
    return job_id