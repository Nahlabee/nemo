import os
import json
import sys
import subprocess
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from utils import load_config


def submit_qsiprep_job(cmd):
    """
    Soumet un job SLURM pour qsiprep et retourne l'ID du job.

    Parameters
    ----------
    script_path : str
        Chemin vers le script SLURM à soumettre.

    Returns
    -------
    job_id : str
        ID du job SLURM soumis.
    """
    try:
        # Exécute la commande sbatch et capture la sortie
        result = subprocess.run(cmd, shell=True, check=True, text=True, capture_output=True)
        output = result.stdout.strip()

        # Analyse la sortie pour extraire l'ID du job
        if output.startswith("Submitted batch job"):
            job_id = output.split()[-1]
            print(f"Job SLURM soumis avec succès : ID {job_id}")
            return job_id
        else:
            print("Impossible de récupérer l'ID du job SLURM.")
            return None
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de la soumission du job SLURM : {e}")
        return None


def do_qsiprep(args):
    """

    Parameters
    ----------
    args

    Returns
    -------

    """
    job_ids = {}

    # Check dataset directory
    if not os.path.exists(args.input_dir):
        print("Dataset directory does not exist.")
        return job_ids

    # Create output (derivatives) directories
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
        os.makedirs(args.output_dir + '/stdout')
        os.makedirs(args.output_dir + '/scripts')

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
            path_to_output = os.path.join(args.output_dir, f"{subject}/{session}")

            # QSIprep manages already processed subjects.
            # No need to remove existing folder or skip subjects.

            # Write and launch slurm commands
            header = \
                ('#!/bin/bash\n'
                 '#SBATCH -J qsiprep_{0}_{1}\n'
                 '#SBATCH -p {2}\n'
                 '#SBATCH --gpus-per-node={3}\n'
                 '#SBATCH --nodes=1\n'
                 '#SBATCH --mem={4}gb\n'
                 '#SBATCH -t {5}:00:00\n'
                 '#SBATCH -e {6}/stdout/%x_job-%j.err\n'
                 '#SBATCH -o {6}/stdout/%x_job-%j.out\n').format(subject, session,
                                                                 args.partition,
                                                                 args.gpu_per_node,
                                                                 args.requested_mem,
                                                                 args.requested_time,
                                                                 args.output_dir)

            if args.email:
                header += \
                    ('#SBATCH --mail-type=BEGIN,END\n'
                     '#SBATCH --mail-user={}\n').format(args.email)

            if args.account:
                header += \
                    '#SBATCH --account={}\n'.format(args.account)

            module_export = \
                ('\n'
                 'module purge\n'
                 'module load userspace/all\n'
                 'module load singularity\n').format(args.input_dir)

            # todo: After PR accepted and new container built, remove bound to local freesurfer 7.4.1 and env variable
            singularity_command = \
                ('\n'
                 'apptainer run \\\n'
                 '    --nv --cleanenv \\\n'
                 '    -B {0}:/data \\\n'
                 '    -B {1}:/out \\\n'
                 '    -B /scratch/lhashimoto/freesurfer-7.4.1/usr/local/freesurfer:/opt/freesurfer:ro \\\n'
                 '    -B {2}/license.txt:/opt/freesurfer/license.txt \\\n'
                 '    -B {6}:/config/eddy-config.json \\\n'
                 '    -B {7}:/config/config-file.toml \\\n'
                 '    --env FREESURFER_HOME=/opt/freesurfer \\\n'
                 '    {3} /data /out participant \\\n'
                 '    --participant-label {4} --session-id {5} \\\n'
                 '    --skip-bids-validation -v -w /out/temp_qsiprep \\\n'
                 '    --fs-license-file /opt/freesurfer/license.txt \\\n'
                 '    --eddy-config /config/eddy_params.json \\\n'
                 '    --config-file /config/config-file.toml \\\n'
                 '    --output-resolution {8}\n').format(args.input_dir,
                                                         args.output_dir,
                                                         args.freesurfer_license,
                                                         args.qsiprep_container,
                                                         subject, session,
                                                         args.config_eddy,
                                                         args.config_qsiprep,
                                                         args.output_resolution)

            ownership_sharing = \
                ('\n'
                 'chmod -Rf 771 {0}\n').format(args.output_dir)

            path_to_script = f'{args.output_dir}/scripts/{subject}_{session}_qsiprep.slurm'
            with open(path_to_script, 'w') as f:
                f.write(header + module_export + singularity_command + ownership_sharing)

            # launch slurm script
            cmd = f"sbatch {path_to_script}"
            print(cmd)
            job_id = submit_qsiprep_job(cmd)
            job_ids[subject][session] = job_id
            # subprocess.run(cmd, shell=True, check=True)

    return job_ids


def check_preprocessing_completion(args, subject, session):
    """
    Check that FreeSurfer recon-all finished successfully.
    Check that QSIprep finished successfully.

    Parameters
    ----------
    args
    subject
    session

    Returns
    -------
    bool
        True or False.
    """

    # Check that FreeSurfer finished without error
    if not os.path.exists(f"{args.derivatives}/freesurfer/{subject}_{session}"):
        print(f"Please run FreeSurfer recon-all command before QSIrecon.")
        return False

    logs = f"{args.derivatives}/freesurfer/{subject}_{session}/scripts/recon-all-status.log"
    with open(logs, 'r') as f:
        lines = f.readlines()
    for l in lines:
        if not 'finished without error' in l:
            print(f"FreeSurfer did not terminate.")
            return False

    stdout_dir = f"{args.derivatives}/qsiprep/stdout"
    if not os.path.exists(stdout_dir):
        print(f"Could not read standard outputs from QSIprep.")
        return False

    prefix = f"qsiprep_{subject}_{session}"
    stdout_files = [f for f in os.listdir(stdout_dir) if (f.startswith(prefix) and f.endswith('.out'))]
    if not stdout_files:
        print(f"Could not read standard outputs from QSIprep.")
        return False

    for file in stdout_files:
        file_path = os.path.join(stdout_dir, file)
        with open(file_path, 'r') as f:
            content = f.read()
            if 'QSIPrep finished successfully!' in content:
                return True

    print("QSIprep did not terminate.")
    return False


def do_qsirecon(args, qsiprep_job_ids=None):
    """
    Executes the postprocessing step with QSIrecon after the preprocessing step with QSIprep has successfully completed.

    Parameters
    ----------
    args : Namespace
        Configuration arguments required to execute the workflow.
    qsiprep_job_ids : dict, optional
        Dict of SLURM job IDs for the preprocessing step (QSIprep). If provided,
        it ensures that QSIrecon jobs are submitted only after the successful
        completion of the QSIprep jobs.
        {'sub-01': {'ses-01': '12345', 'ses-02': '12346'}

    Returns
    -------
    None
        This function does not return any value. It generates and submits SLURM
        scripts to execute the postprocessing step with QSIrecon.
    """

    # Check dataset directory
    if not os.path.exists(args.input_dir):
        print("Dataset directory does not exist.")
        return

    # Create output (derivatives) directories
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
        os.makedirs(args.output_dir + '/stdout')
        os.makedirs(args.output_dir + '/scripts')

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
            path_to_output = os.path.join(args.output_dir, f"{subject}/{session}")

            # QSIrecon manages already processed subjects.
            # No need to remove existing folder or skip subjects.

            # Check input files
            if not check_preprocessing_completion(args, subject, session):
                return

            # Write and launch slurm commands
            header = (
                f'#!/bin/bash\n'
                f'#SBATCH -J qsirecon_{subject}_{session}\n'
                f'#SBATCH -p {args.partition}\n'
                f'#SBATCH --gpus-per-node={args.gpu_per_node}\n'
                f'#SBATCH --nodes=1\n'
                f'#SBATCH --mem={args.requested_mem}gb\n'
                f'#SBATCH -t {args.requested_time}:00:00\n'
                f'#SBATCH -e {args.output_dir}/stdout/%x_job-%j.err\n'
                f'#SBATCH -o {args.output_dir}/stdout/%x_job-%j.out\n'

            )
            if qsiprep_job_ids:
                header += (
                    f'#SBATCH --dependency=afterok:{":".join(qsiprep_job_ids)}\n'
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
                'module purge\n'
                'module load userspace/all\n'
                'module load singularity\n'
            )

            singularity_command = (
                f'\n'
                f'apptainer run \\\n'
                f'    --nv --cleanenv \\\n'
                f'    -B {args.derivatives}/qsiprep:/in \\\n'
                f'    -B {args.derivatives}/qsirecon:/out \\\n'
                f'    -B {args.derivatives}/freesurfer:/freesurfer \\\n'
                f'    -B {args.freesurfer_license}/license.txt:/opt/freesurfer/license.txt \\\n'
                f'    -B {args.config_qsirecon}:/config/config-file.toml \\\n'
                f'    {args.qsirecon_container} /in /out participant \\\n'
                f'    --participant-label {subject} --session-id {session} \\\n'
                f'    -v -w /out/temp_qsirecon \\\n'
                f'    --fs-license-file /opt/freesurfer/license.txt \\\n'
                f'    --fs-subjects-dir /freesurfer \\\n'
                f'    --config-file /config/config-file.toml\n'
            )

            ownership_sharing = (
                f'\n'
                f'chmod -Rf 771 {args.output_dir}\n'
            )

            path_to_script = f'{args.output_dir}/scripts/{subject}_{session}_qsirecon.slurm'
            with open(path_to_script, 'w') as f:
                f.write(header + module_export + singularity_command + ownership_sharing)

            # launch slurm script
            cmd = f"sbatch {path_to_script}"
            print(cmd)
            a = os.system(cmd)
            # subprocess.run(cmd, shell=True, check=True)


def main_old(raw_args=None):
    """

    Parameters
    ----------
    raw_args

    Returns
    -------

    """
    import argparse

    p = argparse.ArgumentParser("QSIprep")

    # Execution
    p.add_argument("--input_dir", type=str,
                   help="Input directory containing dataset images in BIDS format.")
    p.add_argument("--output_dir", type=str,
                   help="Output directory for QSIprep.")  # Leave the choice to set output name (ex: with version nb)

    p.add_argument("--subjects",
                   help="List of subjects to process (the sub- prefix can be removed). If None, all subjects "
                        "in the dataset directory will be processed.")
    p.add_argument("--sessions",
                   help="List of sessions to process (the ses- prefix can be removed). If None, all sessions "
                        "in the subject directory will be processed.")

    p.add_argument("--qsiprep_container", type=str,
                   help="Path to QSIprep container.")  # Full path (not just version) to be able to run it from anywhere
    p.add_argument("--freesurfer_license", type=str,
                   help="Path to FreeSurfer license folder.")
    p.add_argument("--config_eddy", type=str,
                   help="Configuration file containing all the workflow settings.")
    p.add_argument("--config_qsiprep", type=str,
                   help="Configuration file containing all the workflow settings.")
    p.add_argument("--output_resolution",
                   help="The isotropic voxel size in mm the data will be resampled to after preprocessing.")  # Mandatory

    # SLURM
    p.add_argument("--interactive",
                   help="Use interactive mode to perform segmentation. Default is batch mode.")
    p.add_argument("--partition", "-p", type=str,
                   help="Request a specific partition for the resource allocation.")
    p.add_argument("--gpu_per_node", "-gpu", type=int,
                   help="Number of available GPUs.")
    p.add_argument("--requested_mem", "-mem", type=int,
                   help="Requested RAM on cluster node (in GB). Default is 70GB (minimum recommended for QSIprep).")
    p.add_argument("--requested_time", "-time", type=int,
                   help="Requested time on cluster node (in hours). Default is 24h.")
    p.add_argument("--email", "-em", type=str,
                   help="To receive begin/end job notifications. No notification by default.")
    p.add_argument("--account", "-acc", type=str,
                   help="Charge resources used by this job to specified account.")

    args = p.parse_args(raw_args)

    # Read arguments from config file. Values in file will be overridden by command-line arguments.
    general_config_file = f"{Path(__file__).parent.parent}/config.json"
    config = load_config(general_config_file)
    sub_keys = ['common', 'slurm', 'qsiprep']
    for sub_key in sub_keys:
        workflow_config = config.get(sub_key, {})
        for key, value in workflow_config.items():
            if getattr(args, key, None) is None:
                setattr(args, key, value)

    # Save config in json
    config = vars(args)
    with open(os.path.join(args.output_dir, 'config.json'), "w") as f:
        json.dump(config, f, indent=4)

    # todo: check gpu available?
    do_qsiprep(args)


def main():
    from types import SimpleNamespace

    ###########################################################################
    #                       QSIprep
    ###########################################################################

    # Read arguments from config file.
    args = SimpleNamespace()
    general_config_file = f"{Path(__file__).parent.parent}/config.json"
    config = load_config(general_config_file)
    sub_keys = ['common', 'slurm', 'qsiprep']
    for sub_key in sub_keys:
        workflow_config = config.get(sub_key, {})
        for key, value in workflow_config.items():
            if getattr(args, key, None) is None:
                setattr(args, key, value)

    args.output_dir = f"{args.derivatives}/qsiprep"

    # Save config in json
    config = vars(args)
    with open(os.path.join(args.output_dir, 'config.json'), "w") as f:
        json.dump(config, f, indent=4)

    # todo: check gpu available?
    do_qsiprep(args)

    ###########################################################################
    #                       QSIrecon
    ###########################################################################

    # Read arguments from config file.
    args = SimpleNamespace()
    general_config_file = f"{Path(__file__).parent.parent}/config.json"
    config = load_config(general_config_file)
    sub_keys = ['common', 'slurm', 'qsirecon']
    for sub_key in sub_keys:
        workflow_config = config.get(sub_key, {})
        for key, value in workflow_config.items():
            if getattr(args, key, None) is None:
                setattr(args, key, value)

    args.output_dir = f"{args.derivatives}/qsirecon"

    # Save config in json
    config = vars(args)
    with open(os.path.join(args.output_dir, 'config.json'), "w") as f:
        json.dump(config, f, indent=4)

    do_qsirecon(args)


if __name__ == '__main__':
    print('Current working directory: ', os.getcwd())
    main()
