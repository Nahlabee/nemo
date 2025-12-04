import json
import os
from datetime import datetime
from types import SimpleNamespace
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
import utils


def is_already_processed(args, subject, session):
    """
    Check if subject_session is already processed successfully.
    Note: Even if QSIprep put files in cache, some steps are recomputed which require several hours of ressources.

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
        True if already processed, False otherwise.
    """

    # Check if QSIprep already processed without error
    stdout_dir = f"{args.derivatives}/qsiprep/stdout"
    if not os.path.exists(stdout_dir):
        return False

    prefix = f"qsiprep_{subject}_{session}"
    stdout_files = [f for f in os.listdir(stdout_dir) if (f.startswith(prefix) and f.endswith('.out'))]
    if not stdout_files:
        return False

    for file in stdout_files:
        file_path = os.path.join(stdout_dir, file)
        with open(file_path, 'r') as f:
            if 'QSIPrep finished successfully!' in f.read():
                print(f"[QSIPREP] Skip already processed subject {subject}_{session}")
                return True

    return False


def generate_slurm_script(args, subject, session, path_to_script, job_ids=None):
    """
    Generate the SLURM script for QSIprep processing.

    Parameters
    ----------
    args : Namespace
        Configuration arguments containing parameters for SLURM and QSIprep.
    subject : str
        Subject identifier.
    session : str
        Session identifier.
    path_to_script : str
        Path where the SLURM script will be saved.
    job_ids : list, optional
        List of SLURM job IDs to set as dependencies (default is None).
    """
    if job_ids is None:
        job_ids = []

    header = (
        f'#!/bin/bash\n'
        f'#SBATCH -J qsiprep_{subject}_{session}\n'
        f'#SBATCH -p {args.partition}\n'
        f'#SBATCH --gpus-per-node={args.gpu_per_node}\n'
        f'#SBATCH --nodes=1\n'
        f'#SBATCH --mem={args.requested_mem}gb\n'
        f'#SBATCH -t {args.requested_time}:00:00\n'
        f'#SBATCH -e {args.derivatives}/qsiprep/stdout/%x_job-%j.err\n'
        f'#SBATCH -o {args.derivatives}/qsiprep/stdout/%x_job-%j.out\n'
    )

    if job_ids:
        header += (
            f'#SBATCH --dependency=afterok:{":".join(job_ids)}\n'
        )

    if args.email:
        header += (
            f'#SBATCH --mail-type=BEGIN,END\n'
            f'#SBATCH --mail-user={args.email}\n'
        )

    if args.account:
        header += f'#SBATCH --account={args.account}\n'

    module_export = (
        f'\nmodule purge\n'
        f'module load userspace/all\n'
        f'module load singularity\n'
    )

    # Define the Singularity command for running QSIprep
    # Note: Temporary binding to a local FreeSurfer version is included
    # todo: After PR accepted and new container built, remove bound to local freesurfer 7.4.1 and env variable
    singularity_command = (
        f'\napptainer run \\\n'
        f'    --nv --cleanenv \\\n'
        f'    -B {args.input_dir}:/data \\\n'
        f'    -B {args.derivatives}/qsiprep:/out \\\n'
        f'    -B {args.freesurfer_license}:/license \\\n'
        f'    -B {args.config_eddy}:/config/eddy_params.json \\\n'
        f'    -B {args.qsiprep_config}:/config/config-file.toml \\\n'
        f'    -B /scratch/lhashimoto/freesurfer-7.4.1/usr/local/freesurfer:/opt/freesurfer:ro \\\n'
        f'    --env FREESURFER_HOME=/opt/freesurfer \\\n'
        f'    {args.qsiprep_container} /data /out participant \\\n'
        f'    --participant-label {subject} --session-id {session} \\\n'
        f'    --skip-bids-validation -v -w /out/temp_qsiprep \\\n'
        f'    --fs-license-file /opt/freesurfer/license.txt \\\n'
        f'    --eddy-config /config/eddy_params.json \\\n'
        f'    --config-file /config/config-file.toml \\\n'
        f'    --output-resolution {args.output_resolution}\n'
    )

    # Add permissions for shared ownership of the output directory
    ownership_sharing = f'\nchmod -Rf 771 {args.derivatives}/qsiprep\n'

    # Write the complete SLURM script to the specified file
    with open(path_to_script, 'w') as f:
        f.write(header + module_export + singularity_command + ownership_sharing)


def run_qsiprep(args, subject, session, job_ids=None):
    """
    Run the QSIprep for a given subject and session.

    Parameters
    ----------
    job_ids
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

    # QSIprep manages already processed subjects.
    # No need to remove existing folder or skip subjects.
    # Required files are checked inside the process.
    if is_already_processed(args, subject, session):
        return None

    if job_ids is None:
        job_ids = []

    # Create output (derivatives) directories
    os.makedirs(f"{args.derivatives}/qsiprep", exist_ok=True)
    os.makedirs(f"{args.derivatives}/qsiprep/stdout", exist_ok=True)
    os.makedirs(f"{args.derivatives}/qsiprep/scripts", exist_ok=True)

    path_to_script = f"{args.derivatives}/qsiprep/scripts/{subject}_{session}_qsiprep.slurm"
    generate_slurm_script(args, subject, session, path_to_script, job_ids)

    cmd = f"sbatch {path_to_script}"
    print(f"[QSIPREP] Submitting job: {cmd}")
    job_id = utils.submit_job(cmd)
    return job_id
