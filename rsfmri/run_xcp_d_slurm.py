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
from rsfmri.run_fmriprep_slurm import is_already_processed

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

    # Check if mriqc already processed without error
    if not os.path.exists(f"{config_files.config['common']['derivatives']}/fmriprep/{subject}_{session}"):
        print(f"[XCP-D] Please run fMRIPrep before XCP-D.")
        return False

    elif not is_already_processed(subject, session):
        print(f"[XCP-D] FMRIPrep did not terminate successfully. \n Please re-run fMRIPrep before XCP-D.")
        return False

    else:
        # Check if xcp-d already processed without error
        stdout_dir = f"{config_files.config['common']['derivatives']}/xcp_d/stdout"
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
    return False
# -----------------------

# -----------------------
# Generate SLURM job scripts
# -----------------------

def make_slurm_xcpd_script(subject, session, job_ids=None):
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
        
    # Create output (derivatives) directories
    os.makedirs(f"{config_files.config['common']['derivatives']}/xcp_d", exist_ok=True)
    os.makedirs(f"{config_files.config['common']['derivatives']}/xcp_d/stdout", exist_ok=True)
    os.makedirs(f"{config_files.config['common']['derivatives']}/xcp_d/scripts", exist_ok=True)

    if job_ids is None:
        job_ids = []

    header = (
        f'#!/bin/bash\n'
        f'#SBATCH --job-name=xcp_d_{subject}_{session}\n'
        f'#SBATCH --output={config_files.config["common"]["derivatives"]}/xcp_d/stdout/xcp_d_{subject}_{session}_%j.out\n'
        f'#SBATCH --error={config_files.config["common"]["derivatives"]}/xcp_d/stdout/xcp_d_{subject}_{session}_%j.err\n'
        f'#SBATCH --cpus-per-task={config_files.config["xcp_d"]["requested_cpus"]}\n'
        f'#SBATCH --mem={config_files.config["xcp_d"]["requested_mem"]}\n'
        f'#SBATCH --time={config_files.config["xcp_d"]["requested_time"]}\n'
        f'#SBATCH --partition={config_files.config["xcp_d"]["partition"]}\n'
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

        f'echo "------ Running {config_files.config["fmriprep"]["container"]} for subject: {subject}, session: {session} --------"\n'
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
        f'mkdir -p $WORK_DIR\n'
        f'echo "Using TMP_WORK_DIR = $TMP_WORK_DIR"\n'
        f'echo "Using OUT_XCPD_DIR = {config_files.config["common"]["derivatives"]}/xcp_d"\n'
    )
    
    # Define the Singularity command for running FMRIPrep
    singularity_command = (
        f'\napptainer run --cleanenv \\\n'
        f'    -B {config_files.config["common"]["input_dir"]}:/data:ro \\\n'
        f'    -B {config_files.config["common"]["derivatives"]}/xcp_d:/out \\\n'
        f'    -B {config_files.config["xcp_d"]["bids_filter_dir"]}:/bids_filter_dir\\\n'
        f'    {config_files.config["xcp_d"]["xcp_d_container"]} /data /out participant \\\n'
        f'      --input-type fmriprep \\\n'
        f'      --participant-label {subject} \\\n'
        f'      --session-id {session} \\\n'
        f'      --bids-filter-file /bids_filter_dir/bids_filter_{session}.json \\\n'
        f'      --nuisance-regressors 36P \\\n'
        f'      --work-dir $TMP_WORK_DIR \\\n'
        f'      --config-file {config_files.config["xcp_d"]["xcp_d_config"]} \\\n'

    )

    save_work = (
        f'\necho "Cleaning up temporary work directory..."\n'
        f'\nchmod -Rf 771 {config_files.config["common"]["derivatives"]}/xcp_d\n'
        f'\ncp -r $TMP_WORK_DIR/* {config_files.config["common"]["derivatives"]}/xcp_d/work\n'
        f'\nrm -rf $TMP_WORK_DIR\n' 
        f'echo "Finished XCP-D for subject: {subject}, session: {session}"\n'
    )

    # Write the complete SLURM script to the specified file
    path_to_script = f"{config_files.config['common']['derivatives']}/xcp_d/scripts/{subject}_{session}_xcp_d.slurm"

    with open(path_to_script, 'w') as f:
        f.write(header + module_export + tmp_dir_setup + singularity_command + save_work)
    print(f"Created xcp_d SLURM job: {path_to_script} for subject {subject}, session {session}")

    return path_to_script

def submit_with_dependencies(input_dir, job_ids=None):
    
    subjects = utils.get_subjects(input_dir)

    previous_job_id = None
    for sub in subjects:
        sessions = utils.get_sessions(input_dir, sub)

        for ses in sessions:
            # -----------------------
            # SUBMIT XCP-D JOB IF NEEDED
            # -----------------------
            if is_already_processed(sub, ses):

                print(f"✓ Skipping {sub} {ses}: already processed")
                continue
            else:
                print(f"Submitting XCP-D job for {sub} {ses}...")

                # Add dependency if this is not the first job in the chain
                path_to_script = make_slurm_xcpd_script(sub, ses, job_ids=[previous_job_id])
                cmd = f"sbatch {path_to_script}"

                # Extract SLURM job ID (last token in "Submitted batch job 12345")
                job_id = utils.submit_job(cmd)
                previous_job_id = job_id
                print(f"✓ Skipping XCP-D for {sub} {ses}: already done")

        print(f"All XCP_D sessions for {sub} queued.\n")


def main():
    submit_with_dependencies(config_files.config["common"]["input_dir"])

if __name__ == "__main__":
    main()
