#!/usr/bin/env python3
"""
Run XCP-D via SLURM job submission
Author: Henitsoa RASOANANDRIANINA
Date: 2025-10-22
Usage:
    python run_xcpd.py

    """
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
import utils
from rsfmri.run_fmriprep import is_already_processed as is_fmriprep_done


# ------------------------------
# HELPERS
# ------------------------------
def is_already_processed(config, subject, session):
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
    DERIVATIVES_DIR = config["common"]["derivatives"]
    stdout_dir = f"{DERIVATIVES_DIR}/xcp_d/stdout"
    if not os.path.exists(stdout_dir):    
        print(f"[XCP-D] Could not read standard outputs from xcp_d, XCP-D cannot proceed.")
        return False
        
    prefix = f"xcp_d_{subject}_{session}"
    stdout_files = [f for f in os.listdir(stdout_dir) if (f.startswith(prefix) and f.endswith('.out'))]
    if not stdout_files:
        print(f"[XCP-D] Could not read standard outputs from xcp_d, XCP-D cannot proceed.")
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
# Generate SLURM job scripts
# -----------------------
def generate_slurm_xcpd_script(config, subject, session, path_to_script, job_ids=None):
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

    common = config["common"]
    xcp_d = config["xcp_d"]
    DERIVATIVES_DIR = common["derivatives"]

    header = (
        f'#!/bin/bash\n'
        f'#SBATCH --job-name=xcpd_{subject}_{session}\n'
        f'#SBATCH --output={DERIVATIVES_DIR}/xcp_d/stdout/xcp_d_{subject}_{session}_%j.out\n'
        f'#SBATCH --error={DERIVATIVES_DIR}/xcp_d/stdout/xcp_d_{subject}_{session}_%j.err\n'
        f'#SBATCH --mem={xcp_d["requested_mem"]}\n'
        f'#SBATCH --time={xcp_d["requested_time"]}\n'
        f'#SBATCH --partition={xcp_d["partition"]}\n'
    )

    # todo : do it in run_workflow
    if job_ids:
        valid_ids = [str(jid) for jid in job_ids if isinstance(jid, str) and jid.strip()]
        if valid_ids:
            header += f'#SBATCH --dependency=afterok:{":".join(valid_ids)}\n'
            
    if common.get("email"):
        header += (
            f'#SBATCH --mail-type={common["email_frequency"]}\n'
            f'#SBATCH --mail-user={common["email"]}\n'
        )

    if common.get("account"):
        header += f'#SBATCH --account={common["account"]}\n'

    module_export = (
        f'\nmodule purge\n'
        f'module load userspace/all\n'
        f'module load singularity\n'

        f'echo "------ Running {xcp_d["xcp_d_container"]} for subject: {subject}, session: {session} --------"\n'
    )

    # tmp_dir_setup = (
    #     f'\nhostname\n'
    #     f'# Choose writable scratch directory\n'
    #     f'if [ -n "$SLURM_TMPDIR" ]; then\n'
    #     f'    TMP_WORK_DIR="$SLURM_TMPDIR"\n'
    #     f'elif [ -n "$TMPDIR" ]; then\n'
    #     f'    TMP_WORK_DIR="$TMPDIR"\n'
    #     f'else\n'
    #     f'    TMP_WORK_DIR=$(mktemp -d /tmp/xcp_d_{subject}_{session})\n'
    #     f'fi\n'
    #     f'mkdir -p "$TMP_WORK_DIR"\n'
    #     f'echo "Using TMP_WORK_DIR = $TMP_WORK_DIR"\n'
    #     f'echo "Using OUT_XCPD_DIR = {DERIVATIVES_DIR}/xcp_d"\n'
    # )
    
    # Define the Singularity command for running FMRIPrep
    # todo: remove options that are in config file !!
    singularity_command = (
        f'\napptainer run --cleanenv \\\n'
        f'    -B {DERIVATIVES_DIR}/fmriprep/outputs:/data:ro \\\n'
        f'    -B {DERIVATIVES_DIR}/xcp_d:/out \\\n'
        f'    -B {common["freesurfer_license"]}/license.txt:/opt/freesurfer/license.txt \\\n'
        f'    -B {xcp_d["bids_filter_dir"]}:/bids_filter_dir\\\n'
        f'    -B {xcp_d["xcp_d_config"]}:/config/xcp_d_config.toml \\\n'
        f'    {xcp_d["xcp_d_container"]} /data /out/outputs participant \\\n'
        f'      --input-type fmriprep \\\n'
        f'      --participant-label {subject} \\\n'
        f'      --session-id {session} \\\n'
        f'      --fs-license-file /opt/freesurfer/license.txt \\\n'
        f'      --mode abcd\\\n'
        f'      --motion-filter-type none\\\n'
        f'      --bids-filter-file /bids_filter_dir/bids_filter_{session}.json \\\n'
        f'      --nuisance-regressors 36P \\\n'
        f'      --work-dir /out/work \\\n'
        f'      --config-file /config/xcp_d_config.toml \\\n'
    )

    save_work = (
        # f'\necho "Cleaning up temporary work directory..."\n'
        f'\nchmod -Rf 771 {DERIVATIVES_DIR}/xcp_d\n'
        # f'\ncp -r $TMP_WORK_DIR/* {DERIVATIVES_DIR}/xcp_d/work\n'
        # f'echo "Finished XCP-D for subject: {subject}, session: {session}"\n'
    )

    # Write the complete SLURM script to the specified file
    with open(path_to_script, 'w') as f:
        # f.write(header + module_export + tmp_dir_setup + singularity_command + save_work)
        f.write(header + module_export + singularity_command + save_work)
    # print(f"Created xcp_d SLURM job: {path_to_script} for subject {subject}, session {session}")


def run_xcpd(config, subject, session, job_ids=None):
    
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

    DERIVATIVES_DIR = config["common"]["derivatives"]

    # Create output (derivatives) directories
    os.makedirs(f"{DERIVATIVES_DIR}/xcp_d", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/xcp_d/outputs", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/xcp_d/stdout", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/xcp_d/scripts", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/xcp_d/work", exist_ok=True)
    
    if is_already_processed(config, subject, session):
        return None

    #todo: move check in slurm script
    if is_fmriprep_done(config, subject, session) is False and job_ids is None:
        print(f"[XCP_D] FMRIprep not yet completed for subject {subject}_{session}. Cannot proceed with XCP-D.\n")
        return None
    
    else:
    
        path_to_script = f"{DERIVATIVES_DIR}/xcp_d/scripts/{subject}_{session}_xcp_d.slurm"
        generate_slurm_xcpd_script(config, subject, session, path_to_script, job_ids=job_ids)
                    
        cmd = f"sbatch {path_to_script}"
                    
        # Extract SLURM job ID (last token in "Submitted batch job 12345")
        job_id = utils.submit_job(cmd)
        return job_id