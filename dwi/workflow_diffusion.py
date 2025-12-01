import os
import json
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from utils import load_config


def preprocessing(args):
    """

    Parameters
    ----------
    args

    Returns
    -------

    """
    # Check dataset directory
    if not os.path.exists(args.input_dir):
        print("Dataset directory does not exist.")
    else:
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
            if not 'sub-' in subject:
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
                if not 'ses-' in session:
                    session = 'ses-' + session

                print(subject, ' - ', session)
                path_to_output = os.path.join(args.output_dir, f"{subject}/{session}")

                # QSIprep manage already processed subjects.
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
                     '# singularity command\n'
                     'apptainer run \\\n'
                     '    --nv --cleanenv \\\n'
                     '    -B {0}:/data \\\n'
                     '    -B {1}:/out \\\n'
                     '    -B /scratch/lhashimoto/freesurfer-7.4.1/usr/local/freesurfer:/opt/freesurfer:ro \\\n'
                     '    -B {2}/license.txt:/opt/freesurfer/license.txt \\\n'
                     '    --env FREESURFER_HOME=/opt/freesurfer \\\n'
                     '    {3} /data /out participant \\\n'
                     '    --participant-label {4} --session-id {5} \\\n'
                     '    -w /out/temp_qsiprep \\\n'
                     '    --fs-license-file /opt/freesurfer/license.txt \\\n'
                     '    --eddy-config {6} \\\n'
                     '    --config-file {7}\n').format(args.input_dir,
                                                       args.output_dir,
                                                       args.freesurfer_license,
                                                       args.qsiprep_container,
                                                       subject, session,
                                                       args.config_eddy,
                                                       args.config_qsiprep)

                ownership_sharing = \
                    ('\n'
                     'chmod -Rf 771 {0}\n'
                     '\n'
                     'echo "ANATOMICAL SEGMENTATION DONE"\n').format(args.output_dir)

                if args.interactive:
                    file_content = module_export + singularity_command + ownership_sharing
                    path_to_script = f'{args.output_dir}/scripts/{subject}_{session}_qsiprep.sh'
                    cmd = ("sh %s" % path_to_script)
                else:
                    file_content = header + module_export + singularity_command + ownership_sharing
                    path_to_script = f'{args.output_dir}/scripts/{subject}_{session}_qsiprep.slurm'
                    cmd = ("sbatch %s" % path_to_script)

                with open(path_to_script, 'w') as f:
                    f.write(file_content)

                # launch slurm script
                print(cmd)
                a = os.system(cmd)
                print(a)


def main(raw_args=None):
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
    preprocessing(args)


if __name__ == '__main__':
    print('Current working directory: ', os.getcwd())
    main()
