import json
import os
import sys
from pathlib import Path
import pandas as pd

from rsfmri.run_mriqc_group import run_mriqc_group

sys.path.append(str(Path(__file__).resolve().parent.parent))
import utils
from dwi.qc_qsiprep_metrics_extractions import run as extract_qc_metrics


def generate_slurm_script(config, subject, session, path_to_script, job_ids=None):
    """
    Generate the SLURM job script for QC QCIprep processing.

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
        f'#SBATCH --job-name=qc_qsiprep_{subject}_{session}\n'
        f'#SBATCH --output={DERIVATIVES_DIR}/qc/qsiprep/stdout/qc_qsiprep_{subject}_{session}_%j.out\n'
        f'#SBATCH --error={DERIVATIVES_DIR}/qc/qsiprep/stdout/qc_qsiprep_{subject}_{session}_%j.err\n'
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
        f'module load singularity\n'
        f'module load python3/3.12.0\n'
        f'source {common["python_env"]}/bin/activate\n'
    )

    prereq_check = (
        f'\n# Check that QSIPREP outputs exists\n'
        f'if [ ! -d "{DERIVATIVES_DIR}/qsiprep/outputs/{subject}/{session}" ]; then\n'
        f'    echo "[QC-QSIPREP] Please run QSIprep command before QC."\n'
        f'    exit 1\n'
        f'fi\n'

        f'\n# Check that QSIPREP finished without error\n'
        f'prefix="{DERIVATIVES_DIR}/qsiprep/stdout/qsiprep_{subject}_{session}"\n'
        f'found_success=false\n'
        f'for file in $(ls $prefix*.out 2>/dev/null); do\n'
        f'    if grep -q "QSIPrep finished successfully" $file; then\n'
        f'        found_success=true\n'
        f'        break\n'
        f'    fi\n'
        f'done\n'
        f'if [ "$found_success" = false ]; then\n'
        f'    echo "[QC-QSIPREP] QSIPrep did not terminate for {subject} {session}. Please run QSIPrep command before QC."\n'
        f'    exit 1\n'
        f'fi\n'
    )

    # Define the Singularity command for running MRIQC
    # Note: Unlike other BIDS apps, no config file is used here, the option doesn't exist for mriqc
    singularity_command = (
        f'\napptainer run \\\n'
        f'    --cleanenv \\\n'
        f'    -B {DERIVATIVES_DIR}/qsiprep/outputs:/data:ro \\\n'
        f'    -B {DERIVATIVES_DIR}/qc/qsiprep:/out \\\n'
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
    python_command = (
        f'\necho "Running QC metrics extraction"\n'
        f'python3 dwi/qc_qsiprep_metrics_extractions.py '
        f"'{json.dumps(config)}' '{subject}' '{session}'\n"
    )

    # Add permissions for shared ownership of the output directory
    ownership_sharing = f'\nchmod -Rf 771 {DERIVATIVES_DIR}/qc/qsiprep\n'

    # Write the complete SLURM script to the specified file
    with open(path_to_script, 'w') as f:
        f.write(header + module_export + prereq_check + singularity_command + python_command + ownership_sharing)


def run(config, subject, session, job_ids=None):

    common = config["common"]
    DERIVATIVES_DIR = common["derivatives"]

    # Create output (derivatives) directories
    os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep/outputs", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep/stdout", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep/scripts", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep/work", exist_ok=True)

    if not utils.is_mriqc_done(config, subject, session, runtype='qsiprep'):
        path_to_script = f"{DERIVATIVES_DIR}/qc/qsiprep/scripts/qc_qsiprep_{subject}_{session}.slurm"
        generate_slurm_script(config, subject, session, path_to_script, job_ids=job_ids)
        cmd = f"sbatch {path_to_script}"
        print(f"[QC-QSIPREP] Submitting job: {cmd}")
        job_id = utils.submit_job(cmd)
        return job_id

    else:
        print(f"[QC-QSIPREP] Skip already processed MRIQC")
        print(f"[QC-QSIPREP] Performing only python command extraction for {subject}_{session}")
        try:
            extract_qc_metrics(config, subject, session)
        except Exception as e:
            print(f"[QC-QSIPREP] ERROR during QC extraction: {e}", file=sys.stderr)
            raise


def run_group_qc(config, job_ids=None):

    common = config["common"]
    BIDS_DIR = common["input_dir"]
    DERIVATIVES_DIR = common["derivatives"]

    qc_inhouse = []
    qc_qsiprep = []

    # List all subjects and sessions in the QSIprep BIDS output directory
    subjects = utils.get_subjects(f"{DERIVATIVES_DIR}/qsiprep/outputs")
    for subject in subjects:
        sessions = utils.get_sessions(f"{DERIVATIVES_DIR}/qsiprep/outputs", subject)
        for session in sessions:

            # Concatenate in-house metrics
            path_to_qc = Path(f"{DERIVATIVES_DIR}/qc/qsiprep/outputs/{subject}/{session}/{subject}_{session}_qc.csv")
            if not path_to_qc.is_file():
                continue
            qc_inhouse.append(pd.read_csv(path_to_qc))

            # Concatenate QSIPrep metrics
            path_to_dwi = Path(f"{DERIVATIVES_DIR}/qsiprep/outputs/{subject}/{session}/dwi")
            path_to_qc = next(path_to_dwi.glob("*_desc-image_qc.tsv"))
            if not path_to_qc.is_file():
                continue
            qc_qsiprep.append(pd.read_csv(path_to_qc, sep='\t'))

    if qc_inhouse:
        group_qc = pd.concat(qc_inhouse, ignore_index=True)
        path_to_group_qc = f"{DERIVATIVES_DIR}/qc/qsiprep/group_additional_qc.csv"
        group_qc.to_csv(path_to_group_qc, index=False)

    if qc_qsiprep:
        group_qc = pd.concat(qc_qsiprep, ignore_index=True)
        path_to_group_qc = f"{DERIVATIVES_DIR}/qc/qsiprep/group_qsiprep_image_qc.csv"
        group_qc.to_csv(path_to_group_qc, index=False)

    # Run group-level MRIQC
    run_mriqc_group(config, f"{DERIVATIVES_DIR}/qsiprep/outputs", data_type="qsiprep", job_ids=job_ids)

    print(f"[QC-QSIPREP] Group-level QC saved in {DERIVATIVES_DIR}/qc/qsiprep\n")

