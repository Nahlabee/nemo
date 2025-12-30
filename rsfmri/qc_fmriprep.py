#!/usr/bin/env python3
import warnings
import os
import sys
from pathlib import Path
# todo: homogeneization
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from rsfmri.qc_fmriprep_metrics_extractions import run as extract_qc_metrics
import utils
warnings.filterwarnings("ignore")


# ------------------------
# Create SLURM job script for MRIQC 
# ------------------------
def generate_slurm_script(config, subject, session, path_to_script, job_ids=None):
    """
    Generate the SLURM job script for MRIQC FMRIPREP processing.

    Parameters
    ----------
    config : dict
        Configuration dictionary.
    subject : str
        Subject identifier.
    session : str
        Session identifier.
    path_to_script : str
        Path to save the generated SLURM script.
    job_ids : list, optional
        List of SLURM job IDs to set as dependencies (default is None).
    """

    common = config["common"]
    mriqc = config["mriqc"]
    DERIVATIVES_DIR = common["derivatives"]

    header = (
        f'#!/bin/bash\n'
        f'set -euo pipefail\n'
        f'#SBATCH --job-name=qc_fmriprep_{subject}_{session}\n'
        f'#SBATCH --output={DERIVATIVES_DIR}/qc/fmriprep/stdout/qc_fmriprep_{subject}_{session}_%j.out\n'
        f'#SBATCH --error={DERIVATIVES_DIR}/qc/fmriprep/stdout/qc_fmriprep_{subject}_{session}_%j.err\n'
        f'#SBATCH --mem={mriqc["requested_mem"]}\n'
        f'#SBATCH --time={mriqc["requested_time"]}\n'
        f'#SBATCH --partition={mriqc["partition"]}\n'
    )

    if job_ids:
        header += f'#SBATCH --dependency=afterok:{":".join(job_ids)}\n'

    if common.get("email"):
        header += (
            f'#SBATCH --mail-type={common["email_frequency"]}\n'
            f'#SBATCH --mail-user={common["email"]}\n'
        )

    if common.get("account"):
        header += f'#SBATCH --account={common["account"]}\n'

    module_export = (
        f'\nmodule purge\n'
        f'module load userspace/all\n'
        f'module load python3/3.12.0\n'
        f'module load singularity\n'
        f'source /scratch/hrasoanandrianina/python_env/fmriprep_env/bin/activate\n'  # todo: env as parameter

        f'echo "------ Running {mriqc["mriqc_container"]} for subject: {subject}, session: {session} --------"\n'
    )

    # tmp_dir_setup = (
    #     f'\nhostname\n'
    #     f'# Choose writable scratch directory\n'
    #     f'if [ -n "$SLURM_TMPDIR" ]; then\n'
    #     f'    TMP_WORK_DIR="$SLURM_TMPDIR"\n'
    #     f'elif [ -n "$TMPDIR" ]; then\n'
    #     f'    TMP_WORK_DIR="$TMPDIR"\n'
    #     f'else\n'
    #     f'    TMP_WORK_DIR=$(mktemp -d /tmp/qc_fmriprep_{subject}_{session})\n'
    #     f'fi\n'

    #     f'mkdir -p $TMP_WORK_DIR\n'
    #     f'chmod -Rf 771 $TMP_WORK_DIR\n'
    #     f'echo "Using TMP_WORK_DIR = $TMP_WORK_DIR"\n'
    #     f'echo "Using OUT_MRIQC_DIR = {DERIVATIVES_DIR}/qc/fmriprep"\n'
    # )

    prereq_check = (
        f'\n# Check that FMRIPREP outputs exists\n'
        f'if [ ! -d "{DERIVATIVES_DIR}/fmriprep/outputs/{subject}/{session}" ]; then\n'
        f'    echo "[QC-FMRIPREP] Please run Fmriprep command before QC."\n'
        f'    exit 1\n'
        f'fi\n'
        
        f'\n# Check that FMRIPREP finished without error\n'
        f'prefix="{DERIVATIVES_DIR}/fmriprep/stdout/fmriprep_{subject}_{session}"\n'
        f'found_success=false\n'
        f'for file in $(ls $prefix*.out 2>/dev/null); do\n'
        f'    if grep -q "fMRIPrep finished successfully" $file; then\n'
        f'        found_success=true\n'
        f'    fi\n'
        f'done\n'
        f'if [ "$found_success" = false ]; then\n'
        f'    echo "[QC-FMRIPREP] fMRIPrep did not terminate for {subject} {session}. Please run fMRIPrep command before QC."\n'
        f'    exit 1\n'
        f'fi\n'
    )

    # Define the Singularity command for running MRIQC
    # Note: Unlike other BIDS apps, no config file is used here, the option doesn't exist for mriqc

    #todo: voir version Heni
    singularity_cmd = (
            f'\napptainer run \\\n'
            f'    --cleanenv \\\n'
            f'    -B {DERIVATIVES_DIR}/fmriprep/outputs:/data:ro \\\n'
            f'    -B {DERIVATIVES_DIR}/qc/fmriprep:/out \\\n'
            f'    -B {mriqc["bids_filter_dir"]}:/bids_filter_dir \\\n'
            f'    {mriqc["mriqc_container"]} /data /out/outputs participant \\\n'
            f'    --participant_label {subject} \\\n'
            f'    --session-id {session} \\\n'
            f'    --bids-filter-file /bids_filter_dir/bids_filter_{session}.json \\\n'
            f'    --mem {mriqc["requested_mem"]} \\\n'
            f'    -w /out/work \\\n'
            f'    --fd_thres 0.5 \\\n'
            f'    --verbose-reports \\\n'
            f'    --verbose \\\n'
            f'    --no-sub --notrack\n'
        )
    
    # Call to python scripts for the rest of QC
    # todo: mettre les fonctions dans ce script
    # todo: voir version Heni
    python_command = (
        f'\necho "Running QC metrics extraction"\n'
        f'python3 rsfmri/qc_fmriprep_metrics_extractions.py {config} {subject} {session}\n'
                )

    # Add permissions for shared ownership of the output directory
    ownership_sharing = (
        f'\nchmod -Rf 771 {DERIVATIVES_DIR}/qc/fmriprep\n'
    )

    # Write the complete SLURM script to the specified file
    with open(path_to_script, 'w') as f:
        f.write(header + module_export + prereq_check + singularity_cmd + python_command + ownership_sharing)


def run_qc_fmriprep(config, subject, session, job_ids=None):
    """
    Run the qc_fmriprep for a given subject and session.

    Parameters
    ----------
    config : dict
        Configuration arguments.
    subject : str
        Subject identifier.
    session : str
        Session identifier.
    job_ids : list, optional
        List of SLURM job IDs to set as dependencies (default is None).
    Returns
    -------
    str or None
        SLURM job ID if the job is submitted successfully, None otherwise.
    """

    common = config["common"]
    DERIVATIVES_DIR = common["derivatives"]

    # Create output (derivatives) directories
    os.makedirs(f"{DERIVATIVES_DIR}/qc/fmriprep", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/fmriprep/outputs", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/fmriprep/stdout", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/fmriprep/scripts", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/fmriprep/work", exist_ok=True)

    if not utils.is_mriqc_done(config, subject, session, runtype='fmriprep'):
        path_to_script = f"{DERIVATIVES_DIR}/qc/fmriprep/scripts/qc_fmriprep_{subject}_{session}.slurm"
        generate_slurm_script(config, subject, session, path_to_script, job_ids=job_ids)
        cmd = f"sbatch {path_to_script}"
        print(f"[QC-FMRIPREP] Submitting job: {cmd}")
        job_id = utils.submit_job(cmd)
        return job_id

    else:
        print(f"[QC-FMRIPREP] Skip already processed MRIQC")
        print(f"[QC-FMRIPREP] Performing only python command extraction for {subject}_{session}")
        try:
            # todo: config not a path but a dict
            extract_qc_metrics(config, subject, session)
        except Exception as e:
            print(f"[QC-FMRIPREP] ERROR during QC extraction: {e}", file=sys.stderr)
            raise