#!/usr/bin/env python3
import json, os
import numpy as np
import pandas as pd
from pathlib import Path
import nibabel as nb
import utils
# ------------------------------
# CONFIG / THRESHOLDS
# ------------------------------
MIN_RETAINED_VOLS = 100 # Minimum number of volumes to retain after censoring
MAX_CENSOR_PCT = 50.0 # Maximum percentage of censored volumes
MAX_MEAN_FD = 0.5 # Maximum mean framewise displacement

# ------------------------------
# HELPERS
# ------------------------------
def load_if_exists(path):
    """Load a file if it exists, otherwise return None."""
    return path if path.exists() else None

def compute_tsnr(nifti_file, mask_file=None):
    """
    Compute temporal signal-to-noise ratio (tSNR) for a given NIfTI file.

    Parameters
    ----------
    
    nifti_file: Path
        Path to the NIfTI file.
        
    mask_file: Path, optional
        Path to a mask NIfTI file to apply before computing tSNR.
    """
    img = nb.load(nifti_file)
    data = img.get_fdata()

    if mask_file:
        mask = nb.load(mask_file).get_fdata().astype(bool)
        data = data[mask]

    mean_img = np.mean(data, axis=-1)
    std_img = np.std(data, axis=-1)
    tsnr = np.nanmean(mean_img / std_img)
    return float(tsnr)

# ------------------------------
# MAIN QC FUNCTION
# ------------------------------
def extract_qc_metrics(config, xcpd_dir):
    """
    Run QC on XCP-D outputs.
    Parameters
    ----------
    config: simpleNamespace
        Configuration object.
    xcpd_dir: Path
        Path to the XCP-D output directory.
    """
    
    DERIVATIVES_DIR = config.config["common"]["derivatives"]
    os.makedirs(f"{DERIVATIVES_DIR}/qc/xcpd", exist_ok=True)
    xcpd_dir = Path(xcpd_dir)
    out_dir = Path(f"{DERIVATIVES_DIR}/qc/xcpd")

    rows = []
    for subject in utils.get_subjects(xcpd_dir):
        for session in utils.get_sessions(subject):

            subject_dir = Path(xcpd_dir / subject / session)
            try:
                # Extract status from log
                finished_status, runtime = utils.read_log(config, subject, session, run_type="fmriprep")
                dir_count = utils.count_dirs(f"{DERIVATIVES_DIR}/fmriprep/{subject}/{session}")
                file_count = utils.count_files(f"{DERIVATIVES_DIR}/fmriprep/{subject}/{session}")

                # Load XCP-D QC file
                qc_files = list(subject_dir.glob("**/*_qc.csv"))
                if not qc_files:
                    raise FileNotFoundError("No xcp-d QC file found.")

                qc_df = pd.read_csv(qc_files[0])

                # Basic metrics
                mean_fd = qc_df["mean_fd"].iloc[0]
                censor_pct = qc_df["fd_perc"].iloc[0]
                n_retained = qc_df["n_volumes_retained"].iloc[0]
                dvars = qc_df["dvars_mean"].iloc[0]

                # Denoised BOLD
                bold_files = list(subject_dir.glob("**/*desc-denoised_bold.nii.gz"))
                tsnr = None
                if bold_files:
                    tsnr = compute_tsnr(bold_files[0])

                # Functional connectivity sanity
                fc_files = list(subject_dir.glob("**/*connectivity*.tsv"))
                fc_ok = True
                for f in fc_files:
                    mat = pd.read_csv(f, sep="\t", header=None).values
                    if np.isnan(mat).any() or not np.allclose(mat, mat.T):
                        fc_ok = False

                # PASS / FAIL logic
                fail_reasons = []
                if n_retained < MIN_RETAINED_VOLS:
                    fail_reasons.append("Too few volumes")
                if censor_pct > MAX_CENSOR_PCT:
                    fail_reasons.append("Excessive censoring")
                if mean_fd > MAX_MEAN_FD:
                    fail_reasons.append("High motion")
                if not fc_ok:
                    fail_reasons.append("Invalid Functional Connectivity matrix")

                status = "FAIL" if fail_reasons else "CORRECT"

                row = dict(
                    subject = subject,
                    session = session,
                    Finished_without_error = finished_status,
                    Processing_time_hours = runtime,
                    Number_of_folders_generated = dir_count,
                    Number_of_files_generated = file_count,
                    mean_fd = mean_fd,
                    censor_pct = censor_pct,
                    n_volumes_retained = n_retained,
                    dvars_mean = dvars,
                    tsnr = tsnr,
                    fc_ok = fc_ok,
                    status = status,
                    fail_reasons = fail_reasons,
                )
                rows.append(row)

            except Exception as e:
                print(f"⚠️ Skipping {subject} {session}: {e}")
    
    # Save outputs
    qc = pd.DataFrame(rows)
    path_to_qc = f"{DERIVATIVES_DIR}/qc/xcpd/qc.csv"
    qc.to_csv(path_to_qc, index=False)

    print(f"QC saved in {path_to_qc}\n")
    print(f"XCP-D Quality Check terminated successfully.")
# ------------------------------

def generate_slurm_mriqc_script(config, path_to_script, job_ids=None):
    """
    Generate the SLURM script for MRIQC XCPD processing for the group.

    Parameters
    ----------
    args : Namespace
        Configuration arguments containing parameters for MRIQC.
    path_to_script : str
        Path where the SLURM script will be saved.
    job_ids : list, optional
        List of SLURM job IDs to set as dependencies (default is None).
    """

    common = config.config["common"]
    mriqc = config.config["mriqc"]
    DERIVATIVES_DIR = common["derivatives"]

    if job_ids is None:
        job_ids = []

    header = (
        f'#!/bin/bash\n'
        f'#SBATCH --job-name=qc_xcpd\n'
        f'#SBATCH --output={DERIVATIVES_DIR}/qc/xcpd/stdout/qc_xcpd_%j.out\n'
        f'#SBATCH --error={DERIVATIVES_DIR}/qc/xcpd/stdout/qc_xcpd_%j.err\n'
        f'#SBATCH --mem={mriqc["requested_mem"]}\n'
        f'#SBATCH --time={mriqc["requested_time"]}\n'
        f'#SBATCH --partition={mriqc["partition"]}\n'
    )

    if job_ids:
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
        f'module load python3/3.12.0\n'
    )

    tmp_dir_setup = (
        f'\nhostname\n'
        f'# Choose writable scratch directory\n'
        f'if [ -n "$SLURM_TMPDIR" ]; then\n'
        f'    TMP_WORK_DIR="$SLURM_TMPDIR"\n'
        f'elif [ -n "$TMPDIR" ]; then\n'
        f'    TMP_WORK_DIR="$TMPDIR"\n'
        f'else\n'
        f'    TMP_WORK_DIR=$(mktemp -d /tmp/group_mriqc_{data_type})\n'
        f'fi\n'

        f'mkdir -p $TMP_WORK_DIR\n'
        f'chmod -Rf 771 $TMP_WORK_DIR\n'
        f'echo "Using TMP_WORK_DIR = $TMP_WORK_DIR"\n'
        f'echo "Using OUT_MRIQC_DIR = {DERIVATIVES_DIR}/mriqc_group_{data_type}"\n'
    )

    # Define the Singularity command for running MRIQC
    # Note: Unlike fmriprep, no config file is used here, the option doesn't exist for mriqc
    # todo: input depends on data_type no ?
    singularity_cmd = (
        f'\napptainer run \\\n'
        f'    --cleanenv \\\n'
        f'    -B {DERIVATIVES_DIR}/xcpd/outputs:/data:ro \\\n'
        f'    -B {DERIVATIVES_DIR}/qc/xcpd:/out \\\n'
        f'    {mriqc["mriqc_container"]} /data /out group \\\n'
        f'    --mem {mriqc["requested_mem"]} \\\n'
        f'    -w $TMP_WORK_DIR \\\n'
        f'    --fd_thres 0.5 \\\n'
        f'    --verbose-reports \\\n'
        f'    --verbose \\\n'
        f'    --no-sub --notrack\n'
    )

    python_command = (
        f'\npython3 anat/qc_freesurfer.py '
        f"'{json.dumps(vars(config))}' {','.join(subjects_sessions)}"
    )
    save_work = (
        f'\necho "Cleaning up temporary work directory..."\n'
        f'\nchmod -Rf 771 {DERIVATIVES_DIR}/qc/xcpd\n'
        f'\ncp -r $TMP_WORK_DIR/* {DERIVATIVES_DIR}/qc/xcpd/work\n'
        f'echo "Finished MRIQC for XCP-D on the group processing"\n'
    )

    # Write the complete SLURM script to the specified file
    with open(path_to_script, 'w') as f:
        f.write(header + module_export + tmp_dir_setup + singularity_cmd + save_work)
    print(f"Created MRIQC SLURM job: {path_to_script} for group input directory: {input_dir}")
