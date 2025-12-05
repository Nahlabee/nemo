import json
import os
import shutil
import sys
from pathlib import Path

import utils

sys.path.append(str(Path(__file__).parent.parent))
from utils import load_config


def check_prerequisites(args, subject, session):
    """
    Check that T1w (and T2w if necessary) nifti files exist.

    Parameters
    ----------
    args
    subject
    session

    Returns
    -------
    bool
        True if files exist, else False.
    """
    required_files = [
        f"{args.input_dir}/{subject}/{session}/anat/{subject}_{session}_T1w.nii.gz"
    ]
    if args.use_t2:
        required_files.append(
            f"{args.input_dir}/{subject}/{session}/anat/{subject}_{session}_T2w.nii.gz"
        )
    for file in required_files:
        if not os.path.exists(file):
            print(f"ERROR - Missing file : {file}")
            return False
    return True


def run_segmentation(args):
    """

    Parameters
    ----------
    args

    Returns
    -------

    """
    job_ids = {}

    # Create output (derivatives) directories
    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.output_dir + '/stdout', exist_ok=True)
    os.makedirs(args.output_dir + '/scripts', exist_ok=True)

    # Define subjects list
    if not args.subjects:
        subjects = [d for d in os.listdir(args.input_dir) if
                    d.startswith("sub-") and os.path.isdir(os.path.join(args.input_dir, d))]
        print(f"Subjects found in {args.input_dir}:\n"
              f"{subjects}")
    else:
        subjects = args.subjects

    for subject in subjects:
        # Add sub prefix if not given by the user
        if not subject.startswith('sub-'):
            subject = 'sub-' + subject

        job_ids[subject] = {}

        # Define sessions list
        path_to_subject = os.path.join(args.input_dir, subject)
        if not args.sessions:
            sessions = [d for d in os.listdir(path_to_subject) if
                        d.startswith("ses-") and os.path.isdir(os.path.join(path_to_subject, d))]
            print(f"Sessions found in {path_to_subject}:\n"
                  f"{sessions}")
        else:
            sessions = args.sessions

        for session in sessions:
            # Add ses prefix if not given by the user
            if not session.startswith('ses-'):
                session = 'ses-' + session

            print(subject, ' - ', session)

            # Check input files
            if not check_prerequisites(args, subject, session):
                return 0

            # Manage subject folder if already processed and finished successfully
            path_to_output = os.path.join(args.output_dir, f"{subject}_{session}")
            if os.path.exists(path_to_output):
                logs = os.path.join(path_to_output, 'scripts/recon-all-status.log')
                with open(logs, 'r') as f:
                    lines = f.readlines()
                for l in lines:
                    if 'finished without error' in l and args.skip_processed:
                        print(f"Skip already processed subject {subject}_{session}")
                        continue
                # Remove existing subject folder
                shutil.rmtree(path_to_output)

            # write and launch slurm commands
            header = (
                f'#!/bin/bash\n'
                f'#SBATCH -J freesurfer_{subject}_{session}\n'
                f'#SBATCH -p {args.partition}\n'
                f'#SBATCH --nodes=1\n'
                f'#SBATCH --mem={args.requested_mem}gb\n'
                f'#SBATCH -t {args.requested_time}:00:00\n'
                f'#SBATCH -e {args.output_dir}/stdout/%x_job-%j.err\n'
                f'#SBATCH -o {args.output_dir}/stdout/%x_job-%j.out\n'
            )

            if args.email:
                header += (
                    f'#SBATCH --mail-type=BEGIN,END\n'
                    f'#SBATCH --mail-user={args.email}\n'
                )

            if args.account:
                header += (
                    f'#SBATCH --account={args.account}\n'
                )

            module_export = (
                f'\n'
                f'module purge\n'
                f'module load userspace/all\n'
                f'module load singularity\n'
                f'\n'
                f'# export FreeSurfer environment variables\n'
                f'export SUBJECTS_DIR={args.input_dir}\n'
            )

            if args.use_t2:
                singularity_command = (
                    f'\n'
                    f'apptainer run \\\n'
                    f'    --cleanenv \\\n'
                    f'    -B {args.input_dir}:/data \\\n'
                    f'    -B {args.output_dir}:/out \\\n'
                    f'    -B {args.freesurfer_license}:/license \\\n'
                    f'    --env FS_LICENSE=/license/license.txt \\\n'
                    f'    {args.freesurfer_container} bash -c \\\n'
                    f'        "source /usr/local/freesurfer/SetUpFreeSurfer.sh && \\\n'
                    f'        recon-all \\\n'
                    f'            -all \\\n'
                    f'            -s {subject}_{session} \\\n'
                    f'            -i /data/{subject}/{session}/anat/{subject}_{session}_T1w.nii.gz \\\n'
                    f'            -sd /out \\\n'
                    f'            -T2 /data/{subject}/{session}/anat/{subject}_{session}_T2w.nii.gz \\\n'
                    f'            -T2pial"\n'
                )

            else:
                singularity_command = (
                    f'\n'
                    f'apptainer run \\\n'
                    f'    --cleanenv \\\n'
                    f'    -B {args.input_dir}:/data \\\n'
                    f'    -B {args.output_dir}:/out \\\n'
                    f'    -B {args.freesurfer_license}:/license \\\n'
                    f'    --env FS_LICENSE=/license/license.txt \\\n'
                    f'    {args.freesurfer_container} bash -c \\\n'
                    f'        "source /usr/local/freesurfer/SetUpFreeSurfer.sh && \\\n'
                    f'        recon-all \\\n'
                    f'            -all \\\n'
                    f'            -s {subject}_{session} \\\n'
                    f'            -i /data/{subject}/{session}/anat/{subject}_{session}_T1w.nii.gz \\\n'
                    f'            -sd /out \\\n'
                )
                # todo : simplifier si possible l'ajout des otpions t2

            ownership_sharing = (
                f'\n'
                f'chmod -Rf 771 {args.output_dir}\n'
            )

            path_to_script = f'{args.output_dir}/scripts/{subject}_{session}_freesurfer.slurm'
            with open(path_to_script, 'w') as f:
                f.write(header + module_export + singularity_command + ownership_sharing)

            # launch slurm script
            cmd = f"sbatch {path_to_script}"
            print(cmd)
            job_id = utils.submit_job(cmd)
            job_ids[subject][session] = job_id
            # a = os.system(cmd)
            # subprocess.run(cmd, shell=True, check=True)


def segmentation_qc():
    """

    Returns
    -------

    """
    # Call fsqc
    # Case 1 : par défaut sur tous les sujets segmentés

    # Case 2 : sur une liste de sujets

    # +Group-wise QC avec normalisation et calcul définitif des outliers


def main_old(raw_args=None):
    """
    The argument parser allow to launch FreeSurfer via a command line.
    However, by default it will rely on the content of the config file.

    Parameters
    ----------
    raw_args

    Returns
    -------

    """
    import argparse

    # Execution
    p = argparse.ArgumentParser("FreeSurfer recon-all")

    p.add_argument("--input_dir", type=str,
                   help="Input directory containing dataset images in BIDS format.")
    p.add_argument("--output_dir", type=str,
                   help="Output directory for FreeSurfer.")

    p.add_argument("--subjects", "-sub",
                   help="List of subjects to process (the sub- prefix can be removed). If None, all subjects "
                        "in the dataset directory will be processed.")
    p.add_argument("--sessions", "-ses",
                   help="List of sessions to process (the ses- prefix can be removed). If None, all sessions "
                        "in the subject directory will be processed.")
    p.add_argument("--skip_processed", "-skip", type=bool,
                   help="If True, subjects with existing output files will be skipped. Overwrite if False.")

    p.add_argument("--freesurfer_container", type=str,
                   help="Path to FreeSurfer container.")
    p.add_argument("--freesurfer_license", type=str,
                   help="Path to FreeSurfer license folder.")
    p.add_argument("--use_t2", "-t2", type=bool,
                   help="Use T2 if available to improve Pial surface reconstruction.")

    # SLURM
    p.add_argument("--interactive",
                   help="Use interactive mode to perform segmentation. Default is batch mode.")
    p.add_argument("--partition", "-p", type=str,
                   help="Request a specific partition for the resource allocation.")
    p.add_argument("--requested_mem", "-mem", type=int,
                   help="Requested RAM on cluster node (in GB). Default is 16GB (minimum recommended for FreeSurfer).")
    p.add_argument("--requested_time", "-time", type=int,
                   help="Requested time on cluster node (in hours). Default is 9h.")
    p.add_argument("--email", "-em", type=str,
                   help="To receive begin/end job notifications. No notification by default.")
    p.add_argument("--account", "-acc", type=str,
                   help="Charge resources used by this job to specified account.")

    args = p.parse_args(raw_args)

    # Read arguments from config file. Values in file will be overridden by command-line arguments.
    general_config_file = f"{Path(__file__).parent.parent}/config.json"
    config = load_config(general_config_file)
    sub_keys = ['common', 'slurm', 'freesurfer']
    for sub_key in sub_keys:
        workflow_config = config.get(sub_key, {})
        for key, value in workflow_config.items():
            if getattr(args, key, None) is None:
                setattr(args, key, value)

    # Save config in json
    config = vars(args)
    print(args)
    with open(os.path.join(args.output_dir, 'config.json'), "w") as f:
        json.dump(config, f, indent=4)

    run_segmentation(args)


def main():
    from types import SimpleNamespace
    args = SimpleNamespace()

    # Read arguments from config file.
    general_config_file = f"{Path(__file__).parent.parent}/config.json"
    config = load_config(general_config_file)
    sub_keys = ['common', 'slurm', 'freesurfer']
    for sub_key in sub_keys:
        workflow_config = config.get(sub_key, {})
        for key, value in workflow_config.items():
            if getattr(args, key, None) is None:
                setattr(args, key, value)

    args.output_dir = f"{args.derivatives}/freesurfer"

    # Save config in json
    config = vars(args)
    print(args)
    with open(os.path.join(args.output_dir, 'config.json'), "w") as f:
        json.dump(config, f, indent=4)

    run_segmentation(args)


if __name__ == '__main__':
    main()
