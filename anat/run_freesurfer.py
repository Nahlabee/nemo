import os
import shutil
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
import utils


# todo: separate is_already_processed part
def check_prerequisites(config, subject, session):
    """
    Check that required T1w (and optionally T2w) NIfTI files exist.
    Check if subject_session is already processed successfully.

    Parameters
    ----------
    args : Namespace
        Configuration arguments.
    subject : str
        Subject identifier (e.g., "sub-01").
    session : str
        Session identifier (e.g., "ses-01").

    Returns
    -------
    bool
        True if all requirements are met, False otherwise.
    """

    common = config.config["common"]
    freesurfer = config.config["freesurfer"]
    BIDS_DIR = common["input_dir"]
    DERIVATIVES_DIR = common["derivatives"]

    # Check required files
    required_files = [
        f"{BIDS_DIR}/{subject}/{session}/anat/{subject}_{session}_T1w.nii.gz"
    ]
    if freesurfer["use_t2"]:
        required_files.append(
            f"{BIDS_DIR}/{subject}/{session}/anat/{subject}_{session}_T2w.nii.gz"
        )
    for file in required_files:
        if not os.path.exists(file):
            print(f"[FREESURFER] ERROR - Missing file: {file}")
            return False

    # Check if already processed
    # todo: change output directory subject/session or session/subject
    path_to_output = f"{DERIVATIVES_DIR}/freesurfer/{subject}_{session}"
    if os.path.exists(path_to_output):
        logs = os.path.join(path_to_output, 'scripts/recon-all-status.log')
        with open(logs, 'r') as f:
            lines = f.readlines()
        for l in lines:
            if 'finished without error' in l and freesurfer["skip_processed"]:
                print(f"[FREESURFER] Skip already processed subject {subject}_{session}")
                return False
        # Remove existing subject folder
        shutil.rmtree(path_to_output)

    return True


def generate_slurm_script(config, subject, session, path_to_script):
    """
    Generate the SLURM script for FreeSurfer processing.

    Parameters
    ----------
    args : Namespace
        Configuration arguments.
    subject : str
        Subject identifier.
    session : str
        Session identifier.
    path_to_script : str
        Path where the SLURM script will be saved.
    """

    common = config.config["common"]
    freesurfer = config.config["freesurfer"]
    BIDS_DIR = common["input_dir"]
    DERIVATIVES_DIR = common["derivatives"]

    header = (
        f'#!/bin/bash\n'
        f'#SBATCH -J freesurfer_{subject}_{session}\n'
        f'#SBATCH -p {freesurfer["partition"]}\n'
        f'#SBATCH --nodes=1\n'
        f'#SBATCH --mem={freesurfer["requested_mem"]}gb\n'
        f'#SBATCH -t {freesurfer["requested_time"]}\n'
        f'#SBATCH -e {DERIVATIVES_DIR}/freesurfer/stdout/%x_job-%j.err\n'
        f'#SBATCH -o {DERIVATIVES_DIR}/freesurfer/stdout/%x_job-%j.out\n'
    )

    if common.get("email"):
        header += (
            f'#SBATCH --mail-type=BEGIN,END\n'
            f'#SBATCH --mail-user={common["email"]}\n'
        )

    if common.get("account"):
        header += f'#SBATCH --account={common["account"]}\n'

    module_export = (
        f'\nmodule purge\n'
        f'module load userspace/all\n'
        f'module load singularity\n'
        f'export SUBJECTS_DIR={BIDS_DIR}\n'
    )

    singularity_command = (
        f'\napptainer run \\\n'
        f'    --cleanenv \\\n'
        f'    -B {BIDS_DIR}:/data \\\n'
        f'    -B {DERIVATIVES_DIR}/freesurfer:/out \\\n'
        f'    -B {common["freesurfer_license"]}:/license \\\n'
        f'    --env FS_LICENSE=/license/license.txt \\\n'
        f'    {freesurfer["freesurfer_container"]} bash -c \\\n'
        f'        "source /usr/local/freesurfer/SetUpFreeSurfer.sh && \\\n'
        f'        recon-all \\\n'
        f'            -all \\\n'
        f'            -s {subject}_{session} \\\n'
        f'            -i /data/{subject}/{session}/anat/{subject}_{session}_T1w.nii.gz \\\n'
        f'            -sd /out'
    )

    if common.get("use_t2"):
        singularity_command += (
            f' \\\n            -T2 /data/{subject}/{session}/anat/{subject}_{session}_T2w.nii.gz \\\n'
            f'            -T2pial'
        )

    singularity_command += '"\n'  # terminate the command pipe

    ownership_sharing = f'\nchmod -Rf 771 {DERIVATIVES_DIR}/freesurfer\n'

    with open(path_to_script, 'w') as f:
        f.write(header + module_export + singularity_command + ownership_sharing)


def run_freesurfer(config, subject, session):
    """
    Run the FreeSurfer processing for a given subject and session.

    Parameters
    ----------
    args : Namespace
        Configuration arguments.
    subject : str
        Subject identifier.
    session : str
        Session identifier.

    Returns
    -------
    str or None
        SLURM job ID if the job is submitted successfully, None otherwise.
    """

    if not check_prerequisites(config, subject, session):
        return None

    # Create output (derivatives) directories
    DERIVATIVES_DIR = config.config["common"]["derivatives"]
    os.makedirs(f"{DERIVATIVES_DIR}/freesurfer", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/freesurfer/stdout", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/freesurfer/scripts", exist_ok=True)

    path_to_script = f"{DERIVATIVES_DIR}/freesurfer/scripts/{subject}_{session}_freesurfer.slurm"
    generate_slurm_script(config, subject, session, path_to_script)

    cmd = f"sbatch {path_to_script}"
    print(f"[FREESURFER] Submitting job: {cmd}")
    job_id = utils.submit_job(cmd)
    return job_id
