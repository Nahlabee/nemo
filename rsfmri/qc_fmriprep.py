#!/usr/bin/env python3

from venv import logger
import numpy as np
import nibabel as nib
import pandas as pd
from pathlib import Path
from sklearn.metrics import mutual_info_score
from nilearn.image import mean_img
import warnings
import re
import os
import toml
import utils
from config import config

warnings.filterwarnings("ignore")
# -----------------------

def load_any_image(path: Path) -> np.ndarray:
    """
    Load an fMRIPrep/XCP-D output image, handling both NIfTI and GIFTI formats.
    
    Parameters
    ----------
    path : Path
        Path to the .nii(.gz) or .gii file.
    
    Returns
    -------
    data : np.ndarray
        Loaded numeric data array.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    img = nib.load(str(path)) # type: ignore

    if isinstance(img, nib.gifti.gifti.GiftiImage):
        logger.info(f"Detected GIFTI surface file: {path.name}")
        data = np.column_stack([d.data for d in img.darrays])
    elif isinstance(img, (nib.Nifti1Image, nib.Nifti2Image)): # type: ignore
        logger.info(f"Detected NIfTI volumetric file: {path.name}")
        data = img.get_fdata()
    else:
        raise TypeError(f"Unsupported file type: {type(img)}")

    return data, img.affine  # type: ignore

def voxel_count(mask):
    """
    Extract voxel count from a mask (binary or multiclass).
    
    :param mask: array data
    :return: voxel count per unique value
    """

    return np.unique(mask, return_counts=True)


def dice(a, b):
    """
    Compute dice similarity coefficient between two binary masks.
    
    :param a: array data
    :param b: array data

    :return: Dice similarity coefficient
    """
    a = a.astype(bool)
    b = b.astype(bool)
    inter = np.logical_and(a, b).sum()
    s = a.sum() + b.sum()
    return (2 * inter / s) if s > 0 else np.nan

def mutual_information(img1, img2, bins=64):
    """
    Compute mutual information between two images.
    
    :param img1: array data
    :param img2: array data
    :param bins: number of bins for histogram
    :return: Mutual information score
    """
    i1 = img1.flatten()
    i2 = img2.flatten()

    hgram, _, _ = np.histogram2d(i1, i2, bins=bins)
    return mutual_info_score(None, None, contingency=hgram)

# -----------------------
# Main extraction
# -----------------------
def extract_qc_metrics(config, fmriprep_dir):
    """
    Extract QC metrics from fMRIPrep outputs.

    Parameters
    ----------
    fmriprep_dir : Path
        Path to the fMRIPrep derivatives directory. 
    Returns
    -------
    pd.DataFrame
        DataFrame containing QC metrics for each subject and session.
    """
    
    DERIVATIVES_DIR = config.config["common"]["derivatives"]
    rows = []
    for subject in utils.get_subjects(fmriprep_dir):
        for session in utils.get_sessions(subject):

            try:

                finished_status, runtime = utils.read_log(config, subject, session, run_type="fmriprep")
                dir_count = utils.count_dirs(f"{DERIVATIVES_DIR}/fmriprep/{subject}/{session}")
                file_count = utils.count_files(f"{DERIVATIVES_DIR}/fmriprep/{subject}/{session}")

                anat = Path(fmriprep_dir / subject / session / "anat")
                func = Path(fmriprep_dir / subject / session / "func")

                t1w = next(anat.glob("*_desc-preproc_T1w.nii.gz"))
                t1w_mask = next(anat.glob("*_desc-brain_mask.nii.gz"))
                gm = next(anat.glob("*_label-GM_probseg.nii.gz"))
                wm = next(anat.glob("*_label-WM_probseg.nii.gz"))
                csf = next(anat.glob("*_label-CSF_probseg.nii.gz"))

                bold = next(func.glob("*_desc-preproc_bold.nii.gz"))
                bold_mask = next(func.glob("*_desc-brain_mask.nii.gz"))

                # Load data
                t1w_data, t1w_affine = load_any_image(t1w)
                bold_data, bold_affine = load_any_image(bold)

                mean_bold_img = mean_img(bold_data)
                mean_bold = mean_bold_img.get_fdata()

                brain_mask, _ = load_any_image(bold_mask)
                brain_mask = brain_mask > 0
                bg_mask = ~brain_mask

                gm_mask, _ = load_any_image(gm)
                gm_mask = gm_mask > 0.5
                wm_mask, _ = load_any_image(wm)
                wm_mask = wm_mask > 0.5
                csf_mask, _ = load_any_image(csf)
                csf_mask = csf_mask > 0.5

                row = dict(
                    subject=subject,
                    session=session,
                    Process_Run="fmriprep",
                    Finished_without_error=finished_status,
                    Processing_time_hours=runtime,
                    Number_of_folders_generated=dir_count,
                    Number_of_files_generated=file_count,
                    brain_voxels=voxel_count(brain_mask),
                    gm_voxels=voxel_count(gm_mask),
                    wm_voxels=voxel_count(wm_mask),
                    csf_voxels=voxel_count(csf_mask),
                    MI_T1w_BOLD=mutual_information(
                        t1w_data[t1w_mask > 0],
                        mean_bold[brain_mask > 0],
                    ),
                )

                rows.append(row)

            except Exception as e:
                print(f"⚠️ Skipping {subject} {session}: {e}")
    qc = pd.DataFrame(rows)
    path_to_qc = f"{DERIVATIVES_DIR}/qc/fmriprep/qc.csv"
    qc.to_csv(path_to_qc, index=False)

    print(f"QC saved in {path_to_qc}\n")
    print(f"Fmriprep Quality Check terminated successfully.")


# ------------------------
# Create SLURM job script for MRIQC 
# ------------------------
def generate_slurm_mriqc_script(config, subject, session, path_to_script, job_ids=None):
    """Generate the SLURM job script.
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
    BIDS_DIR = common["input_dir"]
    DERIVATIVES_DIR = common["derivatives"]

    header = (
        f'#!/bin/bash\n'
        f'#SBATCH --job-name=mriqc_fmriprep_{subject}_{session}\n'
        f'#SBATCH --output={DERIVATIVES_DIR}/mriqc_fmriprep/stdout/mriqc_fmriprep_{subject}_{session}_%j.out\n'
        f'#SBATCH --error={DERIVATIVES_DIR}/mriqc_fmriprep/stdout/mriqc_fmriprep_{subject}_{session}_%j.err\n'
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

    tmp_dir_setup = (
        f'\nhostname\n'
        f'# Choose writable scratch directory\n'
        f'if [ -n "$SLURM_TMPDIR" ]; then\n'
        f'    TMP_WORK_DIR="$SLURM_TMPDIR"\n'
        f'elif [ -n "$TMPDIR" ]; then\n'
        f'    TMP_WORK_DIR="$TMPDIR"\n'
        f'else\n'
        f'    TMP_WORK_DIR=$(mktemp -d /tmp/mriqc_{subject}_{session})\n'
        f'fi\n'

        f'mkdir -p $TMP_WORK_DIR\n'
        f'chmod -Rf 771 $TMP_WORK_DIR\n'
        f'echo "Using TMP_WORK_DIR = $TMP_WORK_DIR"\n'
        f'echo "Using OUT_MRIQC_DIR = {DERIVATIVES_DIR}/mriqc_fmriprep"\n'
    )

    prereq_check = (
        f'\n# Check that fmriprep finished without error\n'
        f'deriv_data_type_dir="{DERIVATIVES_DIR}/fmriprep/outputs/{subject}/{session}" \n'
        f'if [ ! -d "$deriv_data_type_dir" ]; then\n'
        f'    exit 1\n'
        f'fi\n'

        f'stdout_dir="{DERIVATIVES_DIR}/fmriprep/stdout"\n'
        f'prefix="fmriprep_{subject}_{session}"\n'
        f'success_string="fMRIPrep finished successfully"\n'
        f'found_success=false\n'
        f'for file in $(ls $prefix*.out 2>/dev/null); do\n'
        f'    if grep -q "$success_string" $file; then\n'
        f'        found_success=true\n'
        f'        break\n'
        f'    fi\n'
        f'done\n'
        f'if [ "$found_success" = false ]; then\n'
        f'    echo "[MRIQC] fmriprep did not terminate for {subject} {session}. Please run fmriprep command before."\n '
        f'    exit 1\n'
        f'fi\n'
    
    )

    # Define the Singularity command for running MRIQC
    # Note: Unlike fmriprep, no config file is used here, the option doesn't exist for mriqc
    input_dir = f"{DERIVATIVES_DIR}/fmriprep/outputs"

    singularity_cmd = (
            f'\napptainer run \\\n'
            f'    --cleanenv \\\n'
            f'    -B {input_dir}:/data:ro \\\n'
            f'    -B {DERIVATIVES_DIR}/mriqc_fmriprep/outputs:/out \\\n'
            f'    -B {mriqc["bids_filter_dir"]}:/bids_filter_dir \\\n'
            f'    {mriqc["mriqc_container"]} /data /out participant \\\n'
            f'    --participant_label {subject} \\\n'
            f'    --session-id {session} \\\n'
            f'    --bids-filter-file /bids_filter_dir/bids_filter_{session}.json \\\n'
            f'    --mem {mriqc["requested_mem"]} \\\n'
            f'    -w $TMP_WORK_DIR \\\n'
            f'    --fd_thres 0.5 \\\n'
            f'    --verbose-reports \\\n'
            f'    --verbose \\\n'
            f'    --no-sub --notrack\n'
        )
    
    # Call to python scripts for the rest of QC
    # todo: test toml config file
    python_command = (
        f'\npython3 rsfmri/qc_fmriprep.py {config} {','.join({subject}{session})}\n'
                )
    
    save_work = (
        f'\necho "Cleaning up temporary work directory..."\n'
        f'\nchmod -Rf 771 {DERIVATIVES_DIR}/mriqc_fmriprep\n'
        f'\ncp -r $TMP_WORK_DIR/* {DERIVATIVES_DIR}/mriqc_fmriprep/work\n'
        f'echo "Finished MRIQC-FMRIPREP for subject: {subject}, session: {session}"\n'
    )

    # Write the complete SLURM script to the specified file
    with open(path_to_script, 'w') as f:
            f.write(header + module_export + prereq_check + tmp_dir_setup + singularity_cmd + python_command + save_work)
    print(f"Created MRIQC-FMRIPREP SLURM job: {path_to_script} for subject {subject}, session {session}")


def run(config, subject, session, job_ids=None):
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

    # Run FreeSurfer QC
    # Note that FSQC must run on interactive mode to be able to display (and save) graphical outputs

    if job_ids is None:
        job_ids = []

    common = config["common"]
    mriqc = config["mriqc"]
    DERIVATIVES_DIR = common["derivatives"]

    # Create output (derivatives) directories
    os.makedirs(f"{DERIVATIVES_DIR}/qc/fmriprep", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/fmriprep/stdout", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/fmriprep/scripts", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/fmriprep/outliers", exist_ok=True)

    path_to_script = f"{DERIVATIVES_DIR}/qc/fmriprep/scripts/"
    generate_slurm_mriqc_script(config, subject, session, path_to_script)

    cmd = (f'\nsrun --job-name=mriqc --ntasks=1 '
           f'--partition={mriqc["partition"]} '
           f'--mem={mriqc["requested_mem"]}gb '
           f'--time={mriqc["requested_time"]} '
           f'--out={DERIVATIVES_DIR}/qc/fmriprep/stdout/mriqc.out '
           f'--err={DERIVATIVES_DIR}/qc/fmriprep/stdout/mriqc.err ')

    if job_ids:
        cmd += f'--dependency=afterok:{":".join(job_ids)} '

    cmd += f'sh {path_to_script} &'

    os.system(cmd)
    print(f"[FSQC] Submitting (background) task on interactive node")
    return