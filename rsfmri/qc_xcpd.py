#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
import utils
from rsfmri.qc_xcpd_metrics_extractions import run as extract_qc_metrics


def generate_slurm_script(config, subject, session, path_to_script, job_ids=None):
    """
    Generate the SLURM script for MRIQC XCPD processing .

    Parameters
    ----------
    args : Namespace
        Configuration arguments containing parameters for MRIQC.
    subject : str
        Subject identifier.
    session : str
        Session identifier.
    path_to_script : str
        Path where the SLURM script will be saved.
    job_ids : list, optional
        List of SLURM job IDs to set as dependencies (default is None).
    """

    common = config["common"]
    mriqc = config["mriqc"]
    DERIVATIVES_DIR = common["derivatives"]

    header = (
        f'#!/bin/bash\n'
        # f'set -euo pipefail\n'
        f'#SBATCH --job-name=qc_xcpd_{subject}_{session}\n'
        f'#SBATCH --output={DERIVATIVES_DIR}/qc/xcpd/stdout/qc_xcpd_{subject}_{session}_%j.out\n'
        f'#SBATCH --error={DERIVATIVES_DIR}/qc/xcpd/stdout/qc_xcpd_{subject}_{session}_%j.err\n'
        f'#SBATCH --mem={mriqc["requested_mem"]}\n'
        f'#SBATCH --time={mriqc["requested_time"]}\n'
        f'#SBATCH --partition={mriqc["partition"]}\n'
    )

    if job_ids:
        header += (
            f'#SBATCH --dependency=afterok:{":".join(job_ids)}\n'
        )

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
        f'module load singularity\n'
        f'module load python3/3.12.0\n'
        f'source {common["python_env"]}/bin/activate\n'
    )

    # tmp_dir_setup = (
    #     f'\nhostname\n'
    #     f'# Choose writable scratch directory\n'
    #     f'if [ -n "$SLURM_TMPDIR" ]; then\n'
    #     f'    TMP_WORK_DIR="$SLURM_TMPDIR"\n'
    #     f'elif [ -n "$TMPDIR" ]; then\n'
    #     f'    TMP_WORK_DIR="$TMPDIR"\n'
    #     f'else\n'
    #     f'    TMP_WORK_DIR=$(mktemp -d /tmp/qc_xcpd_{subject}_{session})\n'
    #     f'fi\n'

    #     f'mkdir -p $TMP_WORK_DIR\n'
    #     f'chmod -Rf 771 $TMP_WORK_DIR\n'
    #     f'echo "Using TMP_WORK_DIR = $TMP_WORK_DIR"\n'
    #     f'echo "Using OUT_MRIQC_DIR = {DERIVATIVES_DIR}/qc/xcpd/{subject}_{session}"\n'
    # )

    prereq_check = (
        f'\n# Check that XCP-D finished without error\n'
        f'deriv_data_type_dir="{DERIVATIVES_DIR}/xcpd/outputs/{subject}/{session}" \n'
        f'if [ ! -d "$deriv_data_type_dir" ]; then\n'
        f'    exit 1\n'
        f'fi\n'

        f'stdout_dir="{DERIVATIVES_DIR}/xcpd/stdout"\n'
        f'prefix="{DERIVATIVES_DIR}/xcpd/stdout/xcpd_{subject}_{session}"\n'
        f'success_string="XCP-D finished successfully"\n'
        f'found_success=false\n'
        f'for file in $(ls $prefix*.out 2>/dev/null); do\n'
        f'    if grep -q "$success_string" $file; then\n'
        f'        found_success=true\n'
        f'        break\n'
        f'    fi\n'
        f'done\n'
        f'if [ "$found_success" = false ]; then\n'
        f'    echo "[QC-XCPD] XCP-D did not terminate for {subject} {session}. Please run XCP-D command before."\n '
        f'    exit 1\n'
        f'fi\n'
    
    )
    # Define the Singularity command for running MRIQC
    # todo: voir version Heni
    singularity_cmd = (
        f'\napptainer run \\\n'
        f'    --cleanenv \\\n'
        f'    -B {DERIVATIVES_DIR}/xcpd/outputs:/data:ro \\\n'
        f'    -B {DERIVATIVES_DIR}/qc/xcpd:/out \\\n'
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

    # todo: mettre les fonctions dans ce script ou dans un script qc ou metrics
    # todo: voir version Heni
    python_command = (
        f'\necho "Running QC metrics extraction"\n'
        f'python3 rsfmri/qc_xcpd_metrics_extractions.py '
        f"'{json.dumps(config)}' '{subject}' '{session}'\n"
    )

    ownership_sharing = f'\nchmod -Rf 771 {DERIVATIVES_DIR}/qc/xcpd\n'

    # Write the complete SLURM script to the specified file
    with open(path_to_script, 'w') as f:
        f.write(header + module_export + prereq_check + singularity_cmd + python_command + ownership_sharing)


def run_qc_xcpd(config, subject, session, job_ids=None):
    """
    Run QC and MRIQC on XCP-D outputs for a given subject and session.

    Parameters
    ----------
    config: 
        Configuration object.
    subject: str
        Subject identifier.
    session: str
        Session identifier.
    job_ids: list, optional
        List of SLURM job IDs to set as dependencies (default is None).
    """

    common = config["common"]
    DERIVATIVES_DIR = common["derivatives"]

    # Create output (derivatives) directories
    os.makedirs(f"{DERIVATIVES_DIR}/qc/xcpd", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/xcpd/outputs", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/xcpd/work", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/xcpd/stdout", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/xcpd/scripts", exist_ok=True)

    if not utils.is_mriqc_done(config, subject, session, runtype='xcpd'):
        path_to_script = f"{DERIVATIVES_DIR}/qc/xcpd/scripts/qc_xcpd_{subject}_{session}.slurm"
        generate_slurm_script(config, subject, session, path_to_script, job_ids=job_ids)
        cmd = f"sbatch {path_to_script}"
        print(f"[QC-XCPD] Submitting job: {cmd}")
        job_id = utils.submit_job(cmd)
        return job_id

    else:
        print(f"[QC-XCPD] Skip already processed MRIQC")
        print(f"[QC-XCPD] Performing only python command extraction for {subject}_{session}")
        try:
            extract_qc_metrics(config, subject, session)
        except Exception as e:
            print(f"[QC-XCPD] ERROR during QC extraction: {e}", file=sys.stderr)
            raise

    # todo: qc group récupérer les valeurs de sub-003_ses-01_task-rest_space-fsLR_den-91k_desc-linc_qc.tsv