import os
import shutil
from types import SimpleNamespace
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
import utils


def check_prerequisites(args, subject, session):
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
    # Check required files
    required_files = [
        f"{args.input_dir}/{subject}/{session}/anat/{subject}_{session}_T1w.nii.gz"
    ]
    if args.use_t2:
        required_files.append(
            f"{args.input_dir}/{subject}/{session}/anat/{subject}_{session}_T2w.nii.gz"
        )
    for file in required_files:
        if not os.path.exists(file):
            print(f"ERROR - Missing file: {file}")
            return False

    # Check if already processed
    path_to_output = f"{args.derivatives}/freesurfer/{subject}_{session}"
    if os.path.exists(path_to_output):
        logs = os.path.join(path_to_output, 'scripts/recon-all-status.log')
        with open(logs, 'r') as f:
            lines = f.readlines()
        for l in lines:
            if 'finished without error' in l and args.skip_processed:
                print(f"Skip already processed subject {subject}_{session}")
                return False
        # Remove existing subject folder
        shutil.rmtree(path_to_output)

    return True


def generate_slurm_script(args, subject, session, path_to_script):
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
    header = (
        f'#!/bin/bash\n'
        f'#SBATCH -J freesurfer_{subject}_{session}\n'
        f'#SBATCH -p {args.partition}\n'
        f'#SBATCH --nodes=1\n'
        f'#SBATCH --mem={args.requested_mem}gb\n'
        f'#SBATCH -t {args.requested_time}:00:00\n'
        f'#SBATCH -e {args.derivatives}/freesurfer/stdout/%x_job-%j.err\n'
        f'#SBATCH -o {args.derivatives}/freesurfer/stdout/%x_job-%j.out\n'
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
        f'export SUBJECTS_DIR={args.input_dir}\n'
    )

    singularity_command = (
        f'\napptainer run \\\n'
        f'    --cleanenv \\\n'
        f'    -B {args.input_dir}:/data \\\n'
        f'    -B {args.derivatives}/freesurfer:/out \\\n'
        f'    -B {args.freesurfer_license}:/license \\\n'
        f'    --env FS_LICENSE=/license/license.txt \\\n'
        f'    {args.freesurfer_container} bash -c \\\n'
        f'        "source /usr/local/freesurfer/SetUpFreeSurfer.sh && \\\n'
        f'        recon-all \\\n'
        f'            -all \\\n'
        f'            -s {subject}_{session} \\\n'
        f'            -i /data/{subject}/{session}/anat/{subject}_{session}_T1w.nii.gz \\\n'
        f'            -sd /out'
    )

    if args.use_t2:
        singularity_command += (
            f' \\\n            -T2 /data/{subject}/{session}/anat/{subject}_{session}_T2w.nii.gz \\\n'
            f'            -T2pial'
        )

    singularity_command += '"\n'  # terminate the command pipe

    ownership_sharing = f'\nchmod -Rf 771 {args.derivatives}/freesurfer\n'

    with open(path_to_script, 'w') as f:
        f.write(header + module_export + singularity_command + ownership_sharing)


def run_freesurfer(args, subject, session):
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

    if not check_prerequisites(args, subject, session):
        return None

    # Create output (derivatives) directories
    os.makedirs(f"{args.derivatives}/freesurfer", exist_ok=True)
    os.makedirs(f"{args.derivatives}/freesurfer/stdout", exist_ok=True)
    os.makedirs(f"{args.derivatives}/freesurfer/scripts", exist_ok=True)

    path_to_script = f"{args.derivatives}/freesurfer/scripts/{subject}_{session}_freesurfer.slurm"
    generate_slurm_script(args, subject, session, path_to_script)

    cmd = f"sbatch {path_to_script}"
    print(f"Submitting job: {cmd}")
    job_id = utils.submit_job(cmd)
    return job_id


def main():
    """
    Main function to execute FreeSurfer processing for a list of subjects and sessions.
    """
    config = utils.load_config('config.json')
    args = SimpleNamespace()
    sub_keys = ['common', 'freesurfer']
    for sub_key in sub_keys:
        step_config = config.get(sub_key, {})
        for key, value in step_config.items():
            setattr(args, key, value)

    # Check dataset directory
    if not os.path.exists(args.input_dir):
        print("Dataset directory does not exist.")
        return 0

    # Loop over subjects and sessions
    subjects = utils.get_subjects(args.input_dir, args.subjects)
    for subject in subjects:
        sessions = utils.get_sessions(args.input_dir, subject, args.sessions)
        for session in sessions:
            run_freesurfer(args, subject, session)


if __name__ == "__main__":
    main()