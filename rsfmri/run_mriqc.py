#!/usr/bin/env python3
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
import utils
from rsfmri.run_fmriprep import is_already_processed as is_fmriprep_done
from rsfmri.run_xcpd import is_already_processed as is_xcpd_done
from dwi.run_qsiprep import is_already_processed as is_qsiprep_done
from dwi.run_qsirecon import is_already_processed as is_qsirecon_done


# --------------------------------------------
# HELPERS
# --------------------------------------------
def is_already_processed(config, subject, session, data_type="raw"):
    """
    Check if subject_session is already processed successfully.

    Parameters
    ----------
    subject : str
        Subject identifier (e.g., "sub-01").
    session : str
        Session identifier (e.g., "ses-01").
    data_type : str
        Type of data to process (possible choices: "raw", "fmriprep", "xcp_d", "qsirecon" or "qsiprep").

    Returns
    -------
    bool
        True if already processed, False otherwise.
    """

    # Check if mriqc already processed without error

    DERIVATIVES_DIR = config["common"]["derivatives"]
    stdout_dir = f"{DERIVATIVES_DIR}/mriqc_{data_type}/stdout"
    if not os.path.exists(stdout_dir):
        return False

    prefix = f"mriqc_{subject}_{session}"
    stdout_files = [f for f in os.listdir(stdout_dir) if (f.startswith(prefix) and f.endswith('.out'))]
    if not stdout_files:
        return False

    for file in stdout_files:
        file_path = os.path.join(stdout_dir, file)
        with open(file_path, 'r') as f:
            if 'MRIQC completed' in f.read():
                print(f"[MRIQC] Skip already processed subject {subject}_{session}")
                return True

    return False


def derivatives_datatype_exists(config, subject, session, data_type="raw"):
    """
    Check if derivatives data type directory exists for a given subject and session.

    Parameters
    ----------
    subject : str
        Subject identifier (e.g., "sub-01").
    session : str
        Session identifier (e.g., "ses-01").
    data_type : str
        Type of data to process (possible choices: "raw", "fmriprep", "xcp_d", "qsirecon" or "qsiprep").

    Returns
    -------
    bool
        True if derivatives data type directory exists, False otherwise.
    """
    DERIVATIVES_DIR = config["common"]["derivatives"]
    deriv_dir = f"{DERIVATIVES_DIR}/{data_type}/outputs/{subject}/{session}"
    stdout_dir = f"{DERIVATIVES_DIR}/{data_type}/stdout"
    # Check if derivatives data type directory exists
    if not os.path.exists(deriv_dir):
        print(f"[MRIQC] Derivatives directory {deriv_dir} does not exist. MRIQC cannot proceed.")
        return False
    else:
        prefix = f"{data_type}_{subject}_{session}"
        stdout_files = [f for f in os.listdir(stdout_dir) if (f.startswith(prefix) and f.endswith('.out'))]
        if not stdout_files:
            print(f"[MRIQC] Could not read standard outputs from {data_type}, MRIQC cannot proceed.")
            return False

        for file in stdout_files:
            file_path = os.path.join(stdout_dir, file)
            with open(file_path, 'r') as f:
                if f'{data_type} completed' or 'successful' in f.read():
                    print(f"[MRIQC] Skip already processed subject {subject}_{session}")
                    return True
                else:
                    return False
    return


# ------------------------
# Create SLURM job scripts 
# ------------------------
def generate_slurm_mriqc_script(config, subject, session, path_to_script, data_type="raw", job_ids=None):
    """Generate the SLURM job script.
    Parameters
    ----------
   
    subject : str
        Subject identifier.
    session : str
        Session identifier.
    data_type : str
        Type of data to process (e.g., "raw" or "fmriprep" or "qsiprep").
    job_ids : list, optional
        List of SLURM job IDs to set as dependencies (default is None).
    """

    common = config["common"]
    mriqc = config["mriqc"]
    BIDS_DIR = common["input_dir"]
    DERIVATIVES_DIR = common["derivatives"]

    header = (
        f'#!/bin/bash\n'
        f'#SBATCH --job-name=mriqc_{data_type}_{subject}_{session}\n'
        f'#SBATCH --output={DERIVATIVES_DIR}/mriqc_{data_type}/stdout/mriqc_{data_type}_{subject}_{session}_%j.out\n'
        f'#SBATCH --error={DERIVATIVES_DIR}/mriqc_{data_type}/stdout/mriqc_{data_type}_{subject}_{session}_%j.err\n'
        f'#SBATCH --mem={mriqc["requested_mem"]}\n'
        f'#SBATCH --time={mriqc["requested_time"]}\n'
        f'#SBATCH --partition={mriqc["partition"]}\n'
    )

    # todo: simplify just like qsirecon in run_workflow ?
    if job_ids is None:
        valid_ids = []
    else:
        valid_ids = [str(jid) for jid in job_ids if isinstance(jid, str) and jid.strip()]
        if valid_ids:
            header += f'#SBATCH --dependency=afterok:{":".join(valid_ids)}\n'

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
    #     f'    TMP_WORK_DIR=$(mktemp -d /tmp/mriqc_{subject}_{session})\n'
    #     f'fi\n'
    #
    #     f'mkdir -p $TMP_WORK_DIR\n'
    #     f'chmod -Rf 771 $TMP_WORK_DIR\n'
    #     f'echo "Using TMP_WORK_DIR = $TMP_WORK_DIR"\n'
    #     f'echo "Using OUT_MRIQC_DIR = {DERIVATIVES_DIR}/mriqc_{data_type}"\n'
    # )

    # Define the Singularity command for running MRIQC
    # Note: Unlike fmriprep, no config file is used here, the option doesn't exist for mriqc
    #todo: just a line to define INPUT variable as BIDS_DIR or DERIVTIVES

    if data_type == "raw":
        MRIQC_INPUT = BIDS_DIR
    else:
        MRIQC_INPUT = f"{DERIVATIVES_DIR}/{data_type}/outputs"

    singularity_cmd = (
        f'\napptainer run \\\n'
        f'    --cleanenv \\\n'
        f'    -B {MRIQC_INPUT}:/data:ro \\\n'
        f'    -B {DERIVATIVES_DIR}/mriqc_{data_type}/outputs:/out \\\n'
        f'    -B {mriqc["bids_filter_dir"]}:/bids_filter_dir \\\n'
        f'    {mriqc["mriqc_container"]} /data /out participant \\\n'
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

    save_work = (
        f'\necho "Cleaning up temporary work directory..."\n'
        f'\nchmod -Rf 771 {DERIVATIVES_DIR}/mriqc_{data_type}\n'
        # f'\ncp -r $TMP_WORK_DIR/* {DERIVATIVES_DIR}/mriqc_{data_type}/work\n'
        f'echo "Finished MRIQC for subject: {subject}, session: {session}"\n'
    )

    # Write the complete SLURM script to the specified file
    with open(path_to_script, 'w') as f:
        # todo
        # f.write(header + module_export + tmp_dir_setup + singularity_cmd + save_work)
        f.write(header + module_export + singularity_cmd + save_work)


# ------------------------------
# MAIN JOB SUBMISSION LOGIC
# ------------------------------
def run_mriqc(config, subject, session, data_type="raw", job_ids=None):
    """
    Run the MRIQC for a given subject and session.
    Parameters
    ----------
    subject : str
        Subject identifier.
    session : str
        Session identifier.
    data_type : str
        Type of data to process (possible choices: "raw", "fmriprep", "xcp_d", "qsirecon" or "qsiprep").
    job_ids : list, optional
        List of SLURM job IDs to set as dependencies (default is None).
    """

    if data_type not in ["raw", "fmriprep", "xcp_d", "qsiprep", "qsirecon"]:
        raise ValueError(f"Invalid data_type: {data_type}. Must be 'raw', 'fmriprep', or 'qsiprep'.")

    if is_already_processed(config, subject, session):
        return None

    DERIVATIVES_DIR = config["common"]["derivatives"]

    # Create output (derivatives) directories
    os.makedirs(f"{DERIVATIVES_DIR}/mriqc_{data_type}", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/mriqc_{data_type}/outputs", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/mriqc_{data_type}/stdout", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/mriqc_{data_type}/scripts", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/mriqc_{data_type}/work", exist_ok=True)

    if job_ids is None:
        # todo : move prerequisite check into slurm script like in run_qsirecon.
        #  This ckeck must be done even if all previous jobs are finished. Because they could finish with errors
        if data_type == "fmriprep":
            if is_fmriprep_done(config, subject, session) is False:
                print(
                    f"[MRIQC] FMRIprep not yet completed for subject {subject}_{session}. Cannot proceed with MRIQC.\n")
                return None
        elif data_type == "xcp_d":
            if is_xcpd_done(config, subject, session) is False:
                print(f"[MRIQC] XCP-D not yet completed for subject {subject}_{session}. Cannot proceed with MRIQC.\n")
                return None
        elif data_type == "qsiprep":
            if is_qsiprep_done(config, subject, session) is False:
                print(
                    f"[MRIQC] QSIprep not yet completed for subject {subject}_{session}. Cannot proceed with MRIQC.\n")
                return None
        elif data_type == "qsirecon":
            if is_qsirecon_done(config, subject, session) is False:
                print(
                    f"[MRIQC] QSIrecon not yet completed for subject {subject}_{session}. Cannot proceed with MRIQC.\n")
                return None

        job_ids = []

    # Add dependency if this is not the first job in the chain
    path_to_script = f"{DERIVATIVES_DIR}/mriqc_{data_type}/scripts/{subject}_{session}_mriqc.slurm"
    generate_slurm_mriqc_script(config, subject, session, path_to_script, data_type=data_type, job_ids=job_ids)

    cmd = f"sbatch {path_to_script}"
    job_id = utils.submit_job(cmd)
    return job_id
