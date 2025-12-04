import json
import os
from datetime import datetime
from types import SimpleNamespace
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))
import utils


def check_preprocessing_completion(args, subject, session):
    """
    Check that FreeSurfer recon-all finished successfully.
    Check that QSIprep finished successfully.

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

    # Check that FreeSurfer finished without error
    if not os.path.exists(f"{args.derivatives}/freesurfer/{subject}_{session}"):
        print(f"[QSIRECON] Please run FreeSurfer recon-all command before QSIrecon.")
        return False

    logs = f"{args.derivatives}/freesurfer/{subject}_{session}/scripts/recon-all-status.log"
    with open(logs, 'r') as f:
        lines = f.readlines()
    for l in lines:
        if not 'finished without error' in l:
            print(f"[QSIRECON] FreeSurfer did not terminate.")
            return False

    # Check that QSIprep finished without error
    stdout_dir = f"{args.derivatives}/qsiprep/stdout"
    if not os.path.exists(stdout_dir):
        print(f"[QSIRECON] Could not read standard outputs from QSIprep.")
        return False

    prefix = f"qsiprep_{subject}_{session}"
    stdout_files = [f for f in os.listdir(stdout_dir) if (f.startswith(prefix) and f.endswith('.out'))]
    if not stdout_files:
        print(f"[QSIRECON] Could not read standard outputs from QSIprep.")
        return False

    for file in stdout_files:
        file_path = os.path.join(stdout_dir, file)
        with open(file_path, 'r') as f:
            if 'QSIPrep finished successfully!' in f.read():
                return True

    print("[QSIRECON] QSIprep did not terminate. Please run QSIprep command before QSIrecon.")
    return False


def generate_slurm_script(args, subject, session, path_to_script, job_ids=None):
    """
    Generate the SLURM script for QSIrecon processing.

    Parameters
    ----------
    args : Namespace
        Configuration arguments containing parameters for SLURM and QSIrecon.
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
        f'#SBATCH -J qsirecon_{subject}_{session}\n'
        f'#SBATCH -p {args.partition}\n'
        f'#SBATCH --gpus-per-node={args.gpu_per_node}\n'
        f'#SBATCH --nodes=1\n'
        f'#SBATCH --mem={args.requested_mem}gb\n'
        f'#SBATCH -t {args.requested_time}:00:00\n'
        f'#SBATCH -e {args.derivatives}/qsirecon/stdout/%x_job-%j.err\n'
        f'#SBATCH -o {args.derivatives}/qsirecon/stdout/%x_job-%j.out\n'
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

    prereq_check = (
        f'\n# Check that FreeSurfer finished without error\n'
        f'if [ ! -d "{args.derivatives}/freesurfer/{subject}_{session}" ]; then\n'
        f'    echo "[QSIRECON] Please run FreeSurfer recon-all command before QSIrecon."\n'
        f'    exit 1\n'
        f'fi\n'
        f'if ! grep -q "finished without error" {args.derivatives}/freesurfer/{subject}_{session}/scripts/recon-all.log; then\n'
        f'    echo "[QSIRECON] FreeSurfer did not terminate for {subject} {session}."\n'
        f'    exit 1\n'
        f'fi\n'
        f'\n# Check that QSIprep finished without error\n'
        f'prefix="{args.derivatives}/qsiprep/stdout/qsiprep_{subject}_{session}"\n'
        f'found_success=false\n'
        f'for file in $(ls $prefix*.out 2>/dev/null); do\n'
        f'    if grep -q "QSIPrep finished successfully" $file; then\n'
        f'        found_success=true\n'
        f'        break\n'
        f'    fi\n'
        f'done\n'
        f'if [ "$found_success" = false ]; then\n'
        f'    echo "[QSIRECON] QSIprep did not terminate for {subject} {session}. Please run QSIprep command before '
        f'QSIrecon."\n'
        f'    exit 1\n'
        f'fi\n'
    )

    singularity_command = (
        f'\napptainer run \\\n'
        f'    --nv --cleanenv --containall \\\n'
        f'    -B {args.derivatives}/qsiprep:/data \\\n'
        f'    -B {args.derivatives}/qsirecon:/out \\\n'
        f'    -B {args.derivatives}/freesurfer:/freesurfer \\\n'
        f'    -B {args.freesurfer_license}/license.txt:/opt/freesurfer/license.txt \\\n'
        f'    -B {args.qsirecon_config}:/config/config-file.toml \\\n'
        f'    {args.qsirecon_container} /data /out participant \\\n'
        f'    --participant-label {subject} --session-id {session} \\\n'
        f'    -v -w /out/temp_qsirecon \\\n'
        f'    --fs-license-file /opt/freesurfer/license.txt \\\n'
        f'    --fs-subjects-dir /freesurfer \\\n'
        f'    --config-file /config/config-file.toml\n'
    )
    # singularity_command = (
    #     f'\napptainer run \\\n'
    #     f'    --nv --cleanenv \\\n'
    #     f'    -B {args.derivatives}:/data \\\n'
    #     f'    -B {args.freesurfer_license}/license.txt:/opt/freesurfer/license.txt \\\n'
    #     f'    -B {args.qsirecon_config}:/config/config-file.toml \\\n'
    #     f'    {args.qsirecon_container} /data/qsiprep /data/qsirecon participant \\\n'
    #     f'    --participant-label {subject} --session-id {session} \\\n'
    #     f'    -v -w /data/qsirecon/temp_qsirecon \\\n'
    #     f'    --fs-license-file /opt/freesurfer/license.txt \\\n'
    #     f'    --fs-subjects-dir /data/freesurfer \\\n'
    #     f'    --config-file /config/config-file.toml \\\n'
    # )

    # Add permissions for shared ownership of the output directory
    ownership_sharing = f'\nchmod -Rf 771 {args.derivatives}/qsirecon\n'

    # Write the complete SLURM script to the specified file
    with open(path_to_script, 'w') as f:
        f.write(header + module_export + prereq_check + singularity_command + ownership_sharing)


def run_qsirecon(args, subject, session, job_ids=None):
    """
    Run the QSIrecon for a given subject and session.

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

    # QSIrecon manages already processed subjects.
    # No need to remove existing folder or skip subjects.

    if job_ids is None:
        # Check if preprocessing are terminated only outside a workflow submission. Otherwise, the job is never
        # submitted.
        if not check_preprocessing_completion(args, subject, session):
            return None
        job_ids = []

    # Create output (derivatives) directories
    os.makedirs(f"{args.derivatives}/qsirecon", exist_ok=True)
    os.makedirs(f"{args.derivatives}/qsirecon/stdout", exist_ok=True)
    os.makedirs(f"{args.derivatives}/qsirecon/scripts", exist_ok=True)

    path_to_script = f"{args.derivatives}/qsirecon/scripts/{subject}_{session}_qsirecon.slurm"
    generate_slurm_script(args, subject, session, path_to_script, job_ids)

    cmd = f"sbatch {path_to_script}"
    print(f"[QSIRECON] Submitting job: {cmd}")
    job_id = utils.submit_job(cmd)
    return job_id
