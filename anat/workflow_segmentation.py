import os
import shutil
import sys
sys.path.extend([os.getcwd()])
from config import (DATA_BIDS_DIR, DERIVATIVES_BIDS_DIR,
                    FREESURFER_CONTAINER, FREESURFER_LICENSE, FREESURFER_STDOUT, FREESURFER_OUTPUTS, FREESURFER_QC,
                    FREESURFER_DIR)


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
        # Create output (derivatives) directory
        if not os.path.exists(args.output_dir):
            os.makedirs(args.output_dir)

        # Define subjects list
        if not args.subjects:
            print(f"Looking for subjects sub-* in {args.input_dir}")
            subjects = [d for d in os.listdir(args.input_dir) if d.startswith("sub-") and os.path.isdir(os.path.join(args.input_dir, d))]
        else:
            subjects = args.subjects

        for subject in subjects[:1]:

            # Add sub prefix if not given by the user
            if not 'sub-' in subject:
                subject = 'sub-' + subject

            # Define sessions list
            path_to_subject = os.path.join(args.input_dir, subject)
            if not args.sessions:
                print(f"Looking for sessions ses-* in {path_to_subject}")
                sessions = [d for d in os.listdir(path_to_subject) if
                            d.startswith("ses-") and os.path.isdir(os.path.join(path_to_subject, d))]
            else:
                sessions = args.sessions

            for session in sessions:
                # Add ses prefix if not given by the user
                if not 'ses-' in session:
                    session = 'ses-' + session

                # todo: check if T2 exists + adapt singularity cmd with 'useT2' option

                print(subject, ' - ', session)
                path_to_output = os.path.join(args.output_dir, f"{subject}_{session}")

                # Manage subject folder if already processed
                if os.path.exists(path_to_output):
                    if args.skip_processed:
                        # Skip subject
                        print(f"Skip already processed subject {subject}")
                        continue
                    else:
                        # Remove existing subject folder
                        shutil.rmtree(path_to_output)

                # write and launch slurm commands
                header = \
    '''#!/bin/bash
    #SBATCH -J freesurfer_{0}
    #SBATCH -p skylake
    #SBATCH --nodes=1
    #SBATCH --mem={1}gb
    #SBATCH --cpus-per-task=32
    #SBATCH -t {2}:00:00
    #SBATCH -e {3}/%x_job-%j.err
    #SBATCH -o {3}/%x_job-%j.out
    '''.format(subject, args.requested_mem, args.requested_time, args.stdout)

                if args.email:
                    header += \
    '''#SBATCH --mail-type=BEGIN,END
    #SBATCH --mail-user={}
    '''.format(args.email)

                if args.account:
                    header += \
    '''#SBATCH --account={}
    '''.format(args.account)

                module_export = \
    '''
    module purge
    module load userspace/all
    module load singularity
    
    # export FreeSurfer environment variables
    export SUBJECTS_DIR={}
    '''.format(args.input_dir)
                # todo: test if FREESURFER_HOME is necessary or not

                singularity_command = \
    '''
    # singularity command
    singularity exec -B {0}:/data,{1}:/out,{2}:/license --env FS_LICENSE=/license/license.txt \\
        {3} bash -c \\
            source /usr/local/freesurfer/SetUpFreeSurfer.sh && \\
            recon-all \\
                -all \\
                -s {4} \\
                -i /data/{4}/{5}/anat/{4}_{5}_T1w.nii.gz \\
                -sd /out
    '''.format(args.input_dir, args.output_dir, args.freesurfer_license, args.freesurfer_container,
                               subject, session)

                if args.useT2:
                    singularity_command += \
                        ('            -T2 /data/{4}/{5}/anat/{4}_{5}_T2w.nii.gz \\\n'
                         '            -T2pial \\')

                # todo: vérifier l'option -s = sub-01 ou sub-01_ses-01
                # todo: voir comment intégrer les autres args** de la commande FS via la config

                ownership_sharing = \
    '''
    chmod -Rf 771 {0}
    
    echo "ANATOMICAL SEGMENTATION DONE"
    '''.format(args.output_dir)
                # todo: chgrp -Rf 347 ${0}

                if args.interactive:
                    file_content = module_export + singularity_command + ownership_sharing
                    path_to_script = './{}_freesurfer.sh'.format(subject)
                    cmd = ("sbatch %s" % path_to_script)
                else:
                    file_content = header + module_export + singularity_command + ownership_sharing
                    path_to_script = './{}_freesurfer.slurm'.format(subject)
                    cmd = ("sh %s" % path_to_script)

                with open(path_to_script, 'w') as f:
                    f.write(file_content)

                # launch slurm script
                # print(cmd)
                # a = os.system(cmd)
                # print(a)

                # Delete script
                # todo


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

    p = argparse.ArgumentParser("Launch anatomical segmentation on a dataset")

    p.add_argument("--input_dir", default=DATA_BIDS_DIR,
                   help="Input directory containing dataset images in BIDS format.")
    p.add_argument("--output_dir", default=FREESURFER_DIR,
                   help="Output directory for FreeSurfer.")
    p.add_argument("--subjects", "-sub", default=[],
                   help="List of subjects to process (the sub- prefix can be removed). If None, all subjects "
                        "in the dataset directory will be processed.")
    p.add_argument("--sessions", "-ses", default=[],
                   help="List of sessions to process (the ses- prefix can be removed). If None, all sessions "
                        "in the subject directory will be processed.")
    p.add_argument("--interactive", default=False,
                   help="Use interactive mode to perform segmentation. Default is batch mode.")
    p.add_argument("--freesurfer_container", default=FREESURFER_CONTAINER,
                   help="Path to FreeSurfer container.")
    p.add_argument("--freesurfer_license", default=FREESURFER_LICENSE,
                   help="Path to FreeSurfer license folder.")
    p.add_argument("--useT2", "-t2", default=True,
                   help="Use T2 if available to improve Pial surface reconstruction.")
    p.add_argument("--skip_processed", "-skip", type=bool, default=False,
                   help="If True, subjects with existing output files will be skipped. Overwrite if False.")
    p.add_argument("--stdout", "-std", type=str, default=FREESURFER_STDOUT,
                   help="Standard output directory.")
    p.add_argument("--requested_mem", "-mem", type=int, default=16,
                   help="Requested RAM on cluster node (in GB). Default is 16GB (minimum recommended for FreeSurfer).")
    p.add_argument("--requested_time", "-time", type=int, default=9, choices=range(10),
                   help="Requested time on cluster node (in hours). Default is 9h.")
    p.add_argument("--email", "-em", type=str, default=None,
                   help="To receive begin/end job notifications. No notification by default.")
    p.add_argument("--account", "-acc", type=str, default=None,
                   help="Charge resources used by this job to specified account.")

    args = p.parse_args(raw_args)

    # todo : charger un jason avec le jeu de parametres pré-configuré

    segmentation(args)


if __name__ == '__main__':
    main()
    print(os.getcwd())

