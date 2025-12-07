#!/usr/bin/env python3
import os, sys
import subprocess
from datetime import datetime
from types import SimpleNamespace
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
import utils
import config_files

# -------------------------------
# Load configuration
# -------------------------------
common = config_files.config["common"]
fmriprep = config_files.config["fmriprep"]

BIDS_DIR = common["input_dir"]
DERIVATIVES_DIR = common["derivatives"]

# ------------------------------
# HELPERS
# ------------------------------
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
    stdout_dir = f"{DERIVATIVES_DIR}/fmriprep/stdout"
    if not os.path.exists(stdout_dir):
        return False

    prefix = f"fmriprep_{subject}_{session}"
    stdout_files = [f for f in os.listdir(stdout_dir) if (f.startswith(prefix) and f.endswith('.out'))]
    if not stdout_files:
        return False

    for file in stdout_files:
        file_path = os.path.join(stdout_dir, file)
        with open(file_path, 'r') as f:
            if 'fMRIPrep finished successfully!' in f.read():
                print(f"[FMRIPREP] Skip already processed subject {subject}_{session}")
                return True
            else: 
                return False

def is_freesurfer_done(subject, session):
    """
    Check that FreeSurfer recon-all finished successfully.

    Parameters
    ----------
    subject : str
        Subject identifier (e.g., "sub-01").
    session : str
        Session identifier (e.g., "ses-01").

    Returns
    -------
    bool
        True if FreeSurfer is done, False otherwise.
    """

    # Check that FreeSurfer finished without error
    if not os.path.exists(f"{DERIVATIVES_DIR}/freesurfer/{subject}_{session}"):
        print(f"[FMRIPREP] No FreeSurfer outputs found - Running full fmriprep.")
        return False

    else:
        logs = f"{DERIVATIVES_DIR}/freesurfer/{subject}_{session}/scripts/recon-all-status.log"
        with open(logs, 'r') as f:
            lines = f.readlines()
        for l in lines:
            if not 'finished without error' in l:
                print(f"[FMRIPREP] FreeSurfer did not terminate.")
                return False
            else: return True

# ------------------------
# Create SLURM job scripts 
# ------------------------
def generate_slurm_fmriprep_script(subject, session, path_to_script, fs_done=False, job_ids=None):
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
        f'#SBATCH --job-name=fmriprep_{subject}_{session}\n'
        f'#SBATCH --output={common["derivatives"]}/fmriprep/stdout/fmriprep_{subject}_{session}_%j.out\n'
        f'#SBATCH --error={common["derivatives"]}/fmriprep/stdout/fmriprep_{subject}_{session}_%j.err\n'
        f'#SBATCH --mem={fmriprep["requested_mem"]}\n'
        f'#SBATCH --time={fmriprep["requested_time"]}\n'
        f'#SBATCH --partition={fmriprep["partition"]}\n'
    )
    
    if job_ids:
          valid_ids = [jid for jid in job_ids if jid]
          if valid_ids:
            header += f'#SBATCH --dependency=afterok:{":".join(valid_ids)}\n'
    
    if common.get("email"):
        header += (
            f'#SBATCH --mail-type={common["email_frequency"]}\n'
            f'#SBATCH --mail-user={common["email"]}\n'
        )

    module_export = (
        f'\nmodule purge\n'
        f'module load userspace/all\n'
        f'module load singularity\n'

        f'echo "------ Running {fmriprep["fmriprep_container"]} for subject: {subject}, session: {session} --------"\n'
    )

    tmp_dir_setup = (
        f'\nhostname\n'
        f'# Choose writable scratch directory\n'
        f'if [ -n "$SLURM_TMPDIR" ]; then\n'
        f'    TMP_WORK_DIR="$SLURM_TMPDIR"\n'
        f'elif [ -n "$TMPDIR" ]; then\n'
        f'    TMP_WORK_DIR="$TMPDIR"\n'
        f'else\n'
        f'    TMP_WORK_DIR=$(mktemp -d /tmp/fmriprep_{subject}_{session})\n'
        f'fi\n'
        f'mkdir -p "$TMP_WORK_DIR"\n'
        f'echo "Using TMP_WORK_DIR = $TMP_WORK_DIR"\n'
        f'echo "Using OUT_FMRIPREP_DIR = {common["derivatives"]}/fmriprep"\n'
    )
    
    if not fs_done:
        # Define the Singularity command for running FMRIPrep
        singularity_command = (
            f'\napptainer run --cleanenv \\\n'
            f'    -B {common["input_dir"]}:/data:ro \\\n'
            f'    -B {common["derivatives"]}/fmriprep/outputs:/out \\\n'
            f'    -B {common["freesurfer_license"]}:/license.txt \\\n'
            f'    -B {fmriprep["fmriprep_config"]}:/fmriprep_config.toml \\\n'
            f'    -B {fmriprep["bids_filter_dir"]}:/bids_filter_dir \\\n'
            f'    {fmriprep["fmriprep_container"]} /data /out participant \\\n'
            f'    --participant-label {subject} \\\n'
            f'    --session-label {session} \\\n'
            f'    --fs-license-file /license.txt \\\n'
            f'    --bids-filter-file /bids_filter_dir/bids_filter_{session}.json \\\n'
            f'    --project-goodvoxels \\\n'
            f'    --mem-mb {fmriprep["requested_mem"]} \\\n'
            f'    --output-spaces fsLR:den-32k T1w fsaverage:den-164k MNI152NLin6Asym:res-native \\\n'
            f'    --skip-bids-validation \\\n'
            f'    --work-dir $TMP_WORK_DIR \\\n'
            f'    --config-file /fmriprep_config.toml \n'
        )

    else:
        # Define the Singularity command for running FMRIPrep (skip FreeSurfer)
        singularity_command = (
            f'\napptainer run --cleanenv \\\n'
            f'    -B {common["input_dir"]}:/data:ro \\\n'
            f'    -B {common["derivatives"]}/freesurfer:/fs_dir \\\n'
            f'    -B {common["derivatives"]}/fmriprep/outputs:/out \\\n'
            f'    -B {common["freesurfer_license"]}:/license.txt \\\n'
            f'    -B {fmriprep["fmriprep_config"]}:/fmriprep_config.toml \\\n'
            f'    -B {fmriprep["bids_filter_dir"]}:/bids_filter_dir \\\n'
            f'    {fmriprep["fmriprep_container"]} /data /out participant \\\n'
            f'    --participant-label {subject} \\\n'
            f'    --session-label {session} \\\n'
            f'    --fs-subjects-dir /fs_dir \\\n'
            f'    --fs-license-file /license.txt \\\n'
            f'    --bids-filter-file /bids_filter_dir/bids_filter_{session}.json \\\n'
            f'    --project-goodvoxels \\\n'
            f'    --mem-mb {fmriprep["requested_mem"]} \\\n'
            f'    --output-spaces fsLR:den-32k T1w fsaverage:den-164k MNI152NLin6Asym:res-native \\\n'
            f'    --skip-bids-validation \\\n'
            f'    --work-dir $TMP_WORK_DIR \\\n'
            f'    --config-file /fmriprep_config.toml \\\n'
            f'    --skip-bids-validation \\\n'
            f'    --fs-no-reconall\n'
        )

    save_work = (
        f'\necho "Cleaning up temporary work directory..."\n'
        f'\nchmod -Rf 771 {DERIVATIVES_DIR}/fmriprep\n'
        f'\ncp -r $TMP_WORK_DIR/* {DERIVATIVES_DIR}/fmriprep/work\n'
        f'\nrsync -av {DERIVATIVES_DIR}/fmriprep/outputs/sub-{subject}/anat/ {DERIVATIVES_DIR}/fmriprep/outputs/sub-{subject}/ses-{session}/anat/\n'
        f'\nrm -rf {DERIVATIVES_DIR}/fmriprep/outputs/sub-{subject}/anat\n'
        f'echo "Finished fMRIPrep for subject: {subject}, session: {session}"\n'
    )
    
    # Write the complete SLURM script to the specified file
    with open(path_to_script, 'w') as f:
        f.write(header + module_export + tmp_dir_setup + singularity_command + save_work)
    print(f"Created FMRIPREP SLURM job: {path_to_script} for subject {subject}, session {session}")


def run_fmriprep(input_dir, subject, job_ids=None):
    """
    Run the FMRIPrep for a given subject and session.
    Parameters
    ----------
    input_dir : str
        Path to the BIDS input directory.
    subject : str
        Subject identifier.
    session : str
        Session identifier.
    job_ids : list, optional
        List of job IDs to set as dependencies for this job.

    Returns
    -------
    str or None
        SLURM job ID if the job is submitted successfully, None otherwise.
    """

    
    # Create output (derivatives) directories if they do not exist
    os.makedirs(f"{config_files.config['common']['derivatives']}/fmriprep", exist_ok=True)
    os.makedirs(f"{config_files.config['common']['derivatives']}/fmriprep/outputs", exist_ok=True)
    os.makedirs(f"{config_files.config['common']['derivatives']}/fmriprep/work", exist_ok=True)
    os.makedirs(f"{config_files.config['common']['derivatives']}/fmriprep/stdout", exist_ok=True)
    os.makedirs(f"{config_files.config['common']['derivatives']}/fmriprep/scripts", exist_ok=True)

    previous_job_id = None
    job_ids = []

    sessions = utils.get_sessions(input_dir,subject)
    for ses in sessions:
        if is_already_processed(subject, ses) :
            print(f"✓ Skipping {subject} {ses}: already processed")
            return None
    
        print(f"Submitting fMRIPrep job for {subject} {ses}...")
        path_to_script = f"{DERIVATIVES_DIR}/fmriprep/scripts/{subject}_{ses}_fmriprep.slurm"
        job_ids.append(previous_job_id)

        if is_freesurfer_done(subject, ses):
            generate_slurm_fmriprep_script(subject, ses, path_to_script, fs_done=True, job_ids=[job_ids])
        else:
            generate_slurm_fmriprep_script(subject, ses, path_to_script, fs_done=False, job_ids=[job_ids])
                
        cmd = f"sbatch {path_to_script}"
        # Extract SLURM job ID (last token in "Submitted batch job 12345")
        job_id = utils.submit_job(cmd)
        previous_job_id = job_id
    return job_id
