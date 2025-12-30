#!/usr/bin/env python3
import json
import numpy as np
import pandas as pd
import os
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
import utils


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
    Returns
    -------
    pd.DataFrame
        DataFrame containing QC metrics for each subject and session.
    """

    DERIVATIVES_DIR = config["common"]["derivatives"]
    output_dir = f"{DERIVATIVES_DIR}/qsiprep/outputs/{subject}/{session}"
    anat = f"{DERIVATIVES_DIR}/qsiprep/outputs/{subject}/anat"  # todo: check if sessionwise
    dwi = f"{DERIVATIVES_DIR}/qsiprep/outputs/{subject}/{session}/dwi"

    try:
        # Extract process status from log files
        finished_status, runtime = utils.read_log(config, subject, session, runtype="qsiprep")
        dir_count = utils.count_dirs(output_dir)
        file_count = utils.count_files(output_dir)

        # Load TSV file produced by QSIprep
        qsiprep_confounds = f'{subject}_{session}_run-01_desc-confounds_timeseries.tsv'
        df = pd.read_csv(os.path.join(output_dir, 'dwi', qsiprep_confounds), sep='\t')

        max_framewise_displacement = df['framewise_displacement'].max()
        max_rot_x = df['rot_x'].max()
        max_rot_y = df['rot_y'].max()
        max_rot_z = df['rot_z'].max()
        max_trans_x = df['trans_x'].max()
        max_trans_y = df['trans_y'].max()
        max_trans_z = df['trans_z'].max()
        max_eddy_stdevs = df['eddy_stdevs'].max()
        max_denoising_change = df['DWIDenoise_change'].max() if 'DWIDenoise_change' in df.columns else 0
        max_unringing_change = df['MRDeGibbs_change'].max() if 'MRDeGibbs_change' in df.columns else 0

        # Identify required files
        t1w = next(anat.glob("*_desc-preproc_T1w.nii.gz"))
        t1w_mask = next(anat.glob("*_desc-brain_mask.nii.gz"))
        seg = next(anat.glob("*_dseg.nii.gz"))
        dwiref = next(dwi.glob("*_dwiref.nii.gz"))
        dwi_mask = next(dwi.glob("*_desc-brain_mask.nii.gz"))

        # Load data
        t1w_img = utils.load_any_image(t1w)
        t1w_data = t1w_img.get_fdata()
        t1w_mask_img = utils.load_any_image(t1w_mask)
        t1w_mask_data = t1w_mask_img.get_fdata()
        dwi_img = utils.load_any_image(dwiref)
        dwi_data = dwi_img.get_fdata()
        dwi_mask_img = utils.load_any_image(dwi_mask)
        dwi_mask_data = dwi_mask_img.get_fdata()
        seg_img = utils.load_any_image(seg)
        seg_data = seg_img.get_fdata()

        # Resample dwi into t1w space
        t1w_brain = t1w_data[t1w_mask_data > 0]
        dwi_brain = dwi_data[dwi_mask_data > 0]
        dwi_brain_hr = utils.resample(dwi_brain, t1w_data)
        dwi_mask_data_hr = utils.resample(dwi_mask_data, t1w_data)

        # Compute QC metrics
        row = dict(
            subject=subject,
            session=session,
            Process_Run="qsiprep",
            Finished_without_error=finished_status,
            Processing_time_hours=runtime,
            Number_of_folders_generated=dir_count,
            Number_of_files_generated=file_count,
            t1w_shape=t1w_data.shape,
            dwiref_shape=dwi_data.shape,
            brain_voxels_t1w=np.sum(t1w_mask_data > 0),
            brain_voxels_dwi=np.sum(dwi_mask_data > 0),
            gm_voxels=np.sum(seg_data == 2),
            wm_voxels=np.sum(seg_data == 3),
            csf_voxels=np.sum(seg_data == 1),
            DICE_t1w_dwi=utils.dice(t1w_mask_data, dwi_mask_data_hr),
            MI_t1w_dwi=utils.mutual_information(t1w_brain, dwi_brain_hr),
            max_framewise_displacement=max_framewise_displacement,
            max_rot_x=max_rot_x,
            max_rot_y=max_rot_y,
            max_rot_z=max_rot_z,
            max_trans_x=max_trans_x,
            max_trans_y=max_trans_y,
            max_trans_z=max_trans_z,
            max_eddy_stdevs=max_eddy_stdevs,
            max_denoising_change=max_denoising_change,
            max_unringing_change=max_unringing_change,
        )

        sub_ses = pd.DataFrame([row])
        # Save outputs to csv file
        path_to_qc = f"{DERIVATIVES_DIR}/qc/qsiprep/outputs/{subject}/{session}/{subject}_{session}_qc.csv"
        sub_ses.to_csv(path_to_qc, mode='w', header=True, index=False)
        print(f"QC saved in {path_to_qc}\n")

        print(f"QSIPrep Quality Check terminated successfully for {subject} {session}.")

    except Exception as e:
        print(f"⚠️ Skipping QC for {subject} {session}: \n{e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 4:
        raise RuntimeError(
            "Usage: python qc_qsiprep_metrics_extractions.py <config> <subject> <session>"
        )
    config = json.loads(sys.argv[1])
    subject = sys.argv[2]
    session = sys.argv[3]
    run(config, subject, session)
