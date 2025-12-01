import json
import os
import shutil
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from utils import load_config


def segmentation(args):
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
                path_to_output = os.path.join(args.output_dir, f"{subject}_{session}")

                # Manage subject folder if already processed and finished successfully
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
                header = \
                    ('#!/bin/bash\n'
                     '#SBATCH -J freesurfer_{0}_{1}\n'
                     '#SBATCH -p skylake\n'
                     '#SBATCH --nodes=1\n'
                     '#SBATCH --mem={2}gb\n'
                     '#SBATCH -t {3}:00:00\n'
                     '#SBATCH -e {4}/stdout/%x_job-%j.err\n'
                     '#SBATCH -o {4}/stdout/%x_job-%j.out\n').format(subject, session,
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
                     'module load singularity\n'
                     '\n'
                     '# export FreeSurfer environment variables\n'
                     'export SUBJECTS_DIR={}\n').format(args.input_dir)

                if args.use_t2:
                    singularity_command = \
                        ('\n'
                         '# singularity command\n'
                         'apptainer run -B {0}:/data,{1}:/out,{2}:/license --env FS_LICENSE=/license/license.txt \\\n'
                         '    {3} bash -c \\\n'
                         '        "source /usr/local/freesurfer/SetUpFreeSurfer.sh && \\\n'
                         '        recon-all \\\n'
                         '            -all \\\n'
                         '            -s {4}_{5} \\\n'
                         '            -i /data/{4}/{5}/anat/{4}_{5}_T1w.nii.gz \\\n'
                         '            -sd /out \\\n'
                         '            -T2 /data/{4}/{5}/anat/{4}_{5}_T2w.nii.gz \\\n'
                         '            -T2pial"\n').format(args.input_dir,
                                                          args.output_dir,
                                                          args.freesurfer_license,
                                                          args.freesurfer_container,
                                                          subject, session)

                else:
                    singularity_command = \
                        ('\n'
                         '# singularity command\n'
                         'apptainer run -B {0}:/data,{1}:/out,{2}:/license --env FS_LICENSE=/license/license.txt \\\n'
                         '    {3} bash -c \\\n'
                         '        "source /usr/local/freesurfer/SetUpFreeSurfer.sh && \\\n'
                         '        recon-all \\\n'
                         '            -all \\\n'
                         '            -s {4}_{5} \\\n'
                         '            -i /data/{4}/{5}/anat/{4}_{5}_T1w.nii.gz \\\n'
                         '            -sd /out"\n').format(args.input_dir,
                                                           args.output_dir,
                                                           args.freesurfer_license,
                                                           args.freesurfer_container,
                                                           subject, session)

                # todo: voir comment intégrer les autres args** de la commande FS via la config

                ownership_sharing = \
                    ('\n'
                     'chmod -Rf 771 {0}\n'
                     '\n'
                     'echo "ANATOMICAL SEGMENTATION DONE"\n').format(args.output_dir)

                if args.interactive:
                    file_content = module_export + singularity_command + ownership_sharing
                    path_to_script = f'{args.output_dir}/scripts/{subject}_{session}_freesurfer.sh'
                    cmd = ("sh %s" % path_to_script)
                else:
                    file_content = header + module_export + singularity_command + ownership_sharing
                    path_to_script = f'{args.output_dir}/scripts/{subject}_{session}_freesurfer.slurm'
                    cmd = ("sbatch %s" % path_to_script)

                with open(path_to_script, 'w') as f:
                    f.write(file_content)

                # launch slurm script
                print(cmd)
                a = os.system(cmd)
                print(a)


def segmentation_qc():
    """

    Returns
    -------

    """
    # Call fsqc
    # Case 1 : par défaut sur tous les sujets segmentés

    # Case 2 : sur une liste de sujets

    # +Group-wise QC avec normalisation et calcul définitif des outliers


def main(raw_args=None):
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
    p.add_argument("--requested_mem", "-mem", type=int,
                   help="Requested RAM on cluster node (in GB). Default is 16GB (minimum recommended for FreeSurfer).")
    p.add_argument("--requested_time", "-time", type=int,
                   help="Requested time on cluster node (in hours). Default is 9h.")
    p.add_argument("--email", "-em", type=str,
                   help="To receive begin/end job notifications. No notification by default.")
    p.add_argument("--account", "-acc", type=str,
                   help="Charge resources used by this job to specified account.")

    args = p.parse_args(raw_args)

    general_config_file = f"{os.path.dirname(__file__)}/config.json"
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

    segmentation(args)


if __name__ == '__main__':
    print('Current working directory: ', os.getcwd())
    main()
