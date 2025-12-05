import os
import fsqc
import utils


def generate_slurm_script(args, subjects_sessions, path_to_script, job_ids=None):
    """
    Generate the SLURM script for FSQC processing.

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
        f'#SBATCH -J fsqc\n'
        f'#SBATCH -p visu\n'
        f'#SBATCH --nodes=1\n'
        f'#SBATCH --mem={args.requested_mem}gb\n'
        f'#SBATCH -t {args.requested_time}:00:00\n'
        f'#SBATCH -e {args.derivatives}/qc/fsqc/stdout/%x_job-%j.err\n'
        f'#SBATCH -o {args.derivatives}/qc/fsqc/stdout/%x_job-%j.out\n'
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
        f'module load python3/3.12.0\n'
    )

    subjects_sessions_str = " ".join(subjects_sessions)
    singularity_command = (
        f'\napptainer run \\\n'
        f'    --writable-tmpfs --cleanenv \\\n'
        f'    -B {args.derivatives}/freesurfer:/data \\\n'
        f'    -B {args.derivatives}/qc/fsqc:/out \\\n'
        f'    {args.fsqc_container} \\\n'
        f'      --subjects_dir /data \\\n'
        f'      --output_dir /out \\\n'
        f'      --subjects {subjects_sessions_str}  \\\n'
    )

    if args.qc_screenshots:
        singularity_command += (
            f'      --screenshots \\\n'
        )
    if args.qc_surfaces:
        singularity_command += (
            f'      --surfaces \\\n'
        )
    if args.qc_skullstrip:
        singularity_command += (
            f'      --skullstrip \\\n'
        )
    if args.qc_fornix:
        singularity_command += (
            f'      --fornix \\\n'
        )
    if args.qc_hypothalamus:
        singularity_command += (
            f'      --hypothalamus \\\n'
        )
    if args.qc_hippocampus:
        singularity_command += (
            f'      --hippocampus \\\n'
        )
    if args.qc_skip_existing:
        singularity_command += (
            f'      --skip-existing \\\n'
        )
    if args.qc_outlier:
        singularity_command += (
            f'      --outlier \\\n'
        )

    ownership_sharing = f'\nchmod -Rf 771 {args.derivatives}/qc/fsqc\n'

    with open(path_to_script, 'w') as f:
        f.write(header + module_export + singularity_command + ownership_sharing)


def qc_freesurfer(args, subjects_sessions, job_ids=None):
    """
    Note : Note that a minimum of 10 supplied subjects is required for running outlier analyses,
    otherwise NaNs will be returned.

    Parameters
    ----------
    args
    subject
    session

    Returns
    -------

    """
    # if not check_prerequisites(args, subject, session):
    #     return None

    # Create output (derivatives) directories
    os.makedirs(f"{args.derivatives}/qc", exist_ok=True)
    os.makedirs(f"{args.derivatives}/qc/fsqc", exist_ok=True)
    os.makedirs(f"{args.derivatives}/qc/fsqc/stdout", exist_ok=True)
    os.makedirs(f"{args.derivatives}/qc/fsqc/scripts", exist_ok=True)

    # # Run FSQC on a list of subjects
    # fsqc.run_fsqc(subjects_dir=f"{args.derivatives}/freesurfer",
    #               output_dir=f"{args.derivatives}/qc/fsqc",
    #               subjects=subjects_sessions,
    #               screenshots=args.qc_screenshots,
    #               surfaces=args.qc_surfaces,
    #               skullstrip=args.qc_skullstrip,
    #               fornix=args.qc_fornix,
    #               hypothalamus=args.qc_hypothalamus,
    #               hippocampus=args.qc_hippocampus,
    #               # shape=args.qc_screenshots,  # Runs a specific freesurfer commands not compatible with container
    #               outlier=args.qc_outlier,  # Outliers are recomputed later after normalization by ETIV
    #               skip_existing=args.qc_skip_existing
    #               )
    # return None

    path_to_script = f"{args.derivatives}/qc/fsqc/scripts/fsqc.sh"
    generate_slurm_script(args, subjects_sessions, path_to_script, job_ids)

    cmd = f"nohup sh {path_to_script} > {args.derivatives}/qc/fsqc/stdout/fsqc.out 2>&1 &"
    print(f"[FSQC] Submitting job: {cmd}")
    os.system(cmd)
    return None
