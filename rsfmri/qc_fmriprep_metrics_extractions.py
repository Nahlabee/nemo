#!/usr/bin/env python3
import json
import numpy as np
import pandas as pd
import warnings
import os
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
import utils

warnings.filterwarnings("ignore")


# -----------------------
# Main extraction
# -----------------------
def run(config, subject, session):
    """
    Extract QC metrics from fMRIPrep outputs.

    Parameters
    ----------
    config : dict
        Configuration dictionary.
    fmriprep_dir : Path
        Path to the fMRIPrep derivatives directory. 
    Returns
    -------
    pd.DataFrame
        DataFrame containing QC metrics for each subject and session.
    """

    DERIVATIVES_DIR = config["common"]["derivatives"]
    fmriprep_dir = f"{DERIVATIVES_DIR}/fmriprep/outputs/{subject}/{session}"

    # Read bids_filter file to get the list of tasks to consider
    bids_filter_path = Path(__file__).resolve().parent / "rsfmri" / "bids_filters" / f"bids_filter_{session}.json"
    if not bids_filter_path.is_file():
        raise FileNotFoundError(f"BIDS filter file {bids_filter_path} not found.")
    with open(bids_filter_path, 'r') as f:
        bids_filter_content = json.load(f)
    tasks = bids_filter_content["bold"]["task"]
    # Convert a single string into a list
    if isinstance(tasks, str):
        tasks = [tasks]

    for task in tasks:
        try:
            # Extract process status from log files
            finished_status, runtime = utils.read_log(config, subject, session, runtype="fmriprep")
            dir_count = utils.count_dirs(fmriprep_dir)
            file_count = utils.count_files(fmriprep_dir)

            # Load TSV file produced by FMRIprep
            fmriprep_metrics = f'{subject}_{session}_task-{task}_desc-confounds_timeseries.tsv'
            df = pd.read_csv(os.path.join(fmriprep_dir, 'func', fmriprep_metrics), sep='\t')

            max_framewise_displacement = df['framewise_displacement'].max()
            max_rot_x = df['rot_x'].max()
            max_rot_y = df['rot_y'].max()
            max_rot_z = df['rot_z'].max()
            max_trans_x = df['trans_x'].max()
            max_trans_y = df['trans_y'].max()
            max_trans_z = df['trans_z'].max()
            max_dvars = df['dvars'].max()
            max_rmsd = df['rmsd'].max()

            anat = Path(os.path.join(fmriprep_dir, "anat"))
            func = Path(os.path.join(fmriprep_dir, "func"))

            # Identify required files
            t1w = next(anat.glob("*_desc-preproc_T1w.nii.gz"))
            t1w_mask = next(anat.glob("*_desc-brain_mask.nii.gz"))
            gm = next(anat.glob("*_label-GM_probseg.nii.gz"))
            wm = next(anat.glob("*_label-WM_probseg.nii.gz"))
            csf = next(anat.glob("*_label-CSF_probseg.nii.gz"))
            bold = next(func.glob(f"*{task}_space-T1w_desc-preproc_bold.nii.gz"))
            bold_mask = next(func.glob(f"*{task}_space-T1w_desc-brain_mask.nii.gz"))

            # Load data
            t1w_img = utils.load_any_image(t1w)
            t1w_data = t1w_img.get_fdata()
            t1w_mask_img = utils.load_any_image(t1w_mask)
            t1w_mask_data = t1w_mask_img.get_fdata()
            bold_img = utils.load_any_image(bold)
            bold_data = bold_img.get_fdata()

            # Compute mean BOLD image
            mean_bold = np.mean(bold_data, axis=3)

            # Load masks for voxel counts
            bold_mask_img = utils.load_any_image(bold_mask)
            bold_mask_data = bold_mask_img.get_fdata()
            t1w_brain = t1w_data * t1w_mask_data
            bold_brain = mean_bold * bold_mask_data

            gm_img = utils.load_any_image(gm)
            gm_mask = gm_img.get_fdata() > 0.5
            wm_img = utils.load_any_image(wm)
            wm_mask = wm_img.get_fdata() > 0.5
            csf_img = utils.load_any_image(csf)
            csf_mask = csf_img.get_fdata() > 0.5

            # Resample bold into t1w space
            bold_brain_hr = utils.resample(bold_brain, t1w_data)
            bold_mask_data_hr = utils.resample(bold_mask_data, t1w_data)

            # Compute QC metrics
            row = dict(
                subject=subject,
                session=session,
                task=task,
                Process_Run="fmriprep",
                Finished_without_error=finished_status,
                Processing_time_hours=runtime,
                Number_of_folders_generated=dir_count,
                Number_of_files_generated=file_count,
                t1w_shape=t1w_data.shape,
                brain_voxels_t1w=np.sum(t1w_mask_data > 0),
                brain_voxels_bold=np.sum(bold_mask_data > 0),
                bold_shape=bold_img.shape,
                gm_voxels=np.sum(gm_mask > 0),
                wm_voxels=np.sum(wm_mask > 0),
                csf_voxels=np.sum(csf_mask > 0),
                DICE_t1w_bold=utils.dice(t1w_mask_data, bold_mask_data_hr),
                MI_t1w_bold=utils.mutual_information(t1w_brain, bold_brain_hr),
                max_framewise_displacement=max_framewise_displacement,
                max_rot_x=max_rot_x,
                max_rot_y=max_rot_y,
                max_rot_z=max_rot_z,
                max_trans_x=max_trans_x,
                max_trans_y=max_trans_y,
                max_trans_z=max_trans_z,
                max_dvars=max_dvars,
                max_rmsd=max_rmsd,
            )

            # Save outputs to csv file
            sub_ses_qc = pd.DataFrame([row])
            path_to_qc = f"{DERIVATIVES_DIR}/qc/fmriprep/outputs/{subject}/{session}/{subject}_{session}_task-{task}_qc.csv"
            sub_ses_qc.to_csv(path_to_qc, mode='w', header=True, index=False)
            print(f"QC saved in {path_to_qc}\n")

            print(f"Fmriprep Quality Check terminated successfully for {subject} {session} task-{task}.")

        except Exception as e:
            print(f"⚠️ ERROR: QC aborted for {subject} {session} task-{task}: \n{e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 4:
        raise RuntimeError(
            "Usage: python qc_fmriprep_metrics_extractions.py <config_path> <subject> <session>"
        )
    config = json.loads(sys.argv[1])
    subject = sys.argv[2]
    session = sys.argv[3]
    run(config, subject, session)
