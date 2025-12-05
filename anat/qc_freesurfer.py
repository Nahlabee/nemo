import os
import fsqc
import pandas as pd
from fsqc.outlierDetection import readAsegStats
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent))
sys.path.append(str(Path(__file__).resolve().parent.parent))
from outlierDetection import outlierDetection_normalized, outlierTable
import utils
import re
import csv
import numpy as np


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
        f.write(module_export + singularity_command + ownership_sharing)


def read_log(log_file):
    """
    Uses regex to extract information from the log file such as the runtime, the number of Euler number before and after topological correction

    """
    finished_pattern = re.compile(r"finished without error")
    runtime_pattern = re.compile(r"#@#%# recon-all-run-time-hours (\d+\.\d+)")
    topo_before_pattern_lh = re.compile(r"#@# Fix Topology lh.*?before topology correction, eno=([^\(]+)", re.DOTALL)
    topo_before_pattern_rh = re.compile(r"#@# Fix Topology rh.*?before topology correction, eno=([^\(]+)", re.DOTALL)
    topo_after_pattern_lh = re.compile(r"#@# Fix Topology lh.*?after topology correction, eno=([^\(]+)", re.DOTALL)
    topo_after_pattern_rh = re.compile(r"#@# Fix Topology rh.*?after topology correction, eno=([^\(]+)", re.DOTALL)

    # Read log file
    with open(log_file, 'r') as file:
        log_content = file.read()

    # Check if "finished without error" is present
    finished_status = "Success" if finished_pattern.search(log_content) else "Error"

    # Extract runtime
    runtime_match = runtime_pattern.search(log_content)
    runtime = runtime_match.group(1) if runtime_match else "Not found"

    # Extract Euler number before topological correction
    topo_match = topo_before_pattern_lh.search(log_content)
    eno_before_lh = topo_match.group(1) if topo_match else np.nan
    topo_match = topo_before_pattern_rh.search(log_content)
    eno_before_rh = topo_match.group(1) if topo_match else np.nan

    # Extract Euler number after topological correction
    topo_match = topo_after_pattern_lh.search(log_content)
    eno_after_lh = topo_match.group(1) if topo_match else np.nan
    topo_match = topo_after_pattern_rh.search(log_content)
    eno_after_rh = topo_match.group(1) if topo_match else np.nan

    return finished_status, runtime, eno_before_lh, eno_before_rh, eno_after_lh, eno_after_rh


def convert_radians_to_degrees(df):
    """
    Convert radians to degrees for rotation angles.
    Save results in new columns.
    """
    df["rot_tal_x_deg"] = df["rot_tal_x"].apply(lambda x: x * 180 / 3.14)
    df["rot_tal_y_deg"] = df["rot_tal_y"].apply(lambda x: x * 180 / 3.14)
    df["rot_tal_z_deg"] = df["rot_tal_z"].apply(lambda x: x * 180 / 3.14)
    return df


def normalize_aseg_volumes(freesurfer_dir, subjects_sessions, columns_to_extract, ETIV='aseg.EstimatedTotalIntraCranialVol'):
    """
    Extract ETIV value for each subject.
    Normalize ASEG volumes by EstimatedTotalIntraCranialVol and save it in a csv file.

    :param subjects:
    :param subjects_dir:
    :param columns_to_exclude: columns that are not volumes
    :param columns_to_skip: basically ETIV column and other columns to merge in the final dataframe
    :return:
    """

    df_qc = []
    for sub_sess in subjects_sessions:
        # Read aseg file
        aseg_stats_file = f"{freesurfer_dir}/{sub_sess}/stats/aseg.stats"
        aseg_stats = readAsegStats(aseg_stats_file)
        df_aseg = pd.DataFrame([aseg_stats])

        # Normalize volumes
        df_aseg_norm = df_aseg.drop(columns=columns_to_extract + ETIV)
        df_aseg_norm = df_aseg_norm.div(df_aseg['aseg.EstimatedTotalIntraCranialVol'], axis=0)
        df_aseg_norm.to_csv(f"{freesurfer_dir}/{sub_sess}/stats/aseg_stats_norm.csv", index=False)

        # Extract columns for QC
        df_sub = df_aseg[[columns_to_extract, ETIV]]
        df_sub['subject'] = [sub_sess]
        df_qc.append(df_sub)

    return pd.concat(df_qc, ignore_index=True)


def calculate_outliers(freesurfer_dir, subjects_sessions, outlier_dir, outlier_params):
    """
    Adapted from fsqc.fsqcMain.py (line 2726).
    Compute outliers for each subject compared to the sample for the following values :
    - aseg volumes normalized by ETIV
    - aparc cortical thickness
    - hypothalamus substructures volume (optional)
    - hippocampus and amygdala substructures volume (optional)

    The comparison against normative values is not used because default normative values are not normalized by brain
    size. However, it would be possible to use custom normative values to give as a dictionary to the function and
    uncomment the n_outlier_norms line The function outlierDetection_normalized is also adapted from the original
    function. This one reads the normalized aseg stats from the csv files (aseg_stats_norm.csv).

    :return:
    """
    print("---------------------------------------")
    print("Running outlier detection")
    print("")

    # determine outlier-table and get data
    if outlier_params['outlierDict'] is None:
        outlierDict = outlierTable()
    else:
        outlierDict = dict()
        with open(outlierDict, newline="") as csvfile:
            outlierCsv = csv.DictReader(csvfile, delimiter=",")
            for row in outlierCsv:
                outlierDict.update(
                    {
                        row["label"]: {
                            "lower": float(row["lower"]),
                            "upper": float(row["upper"]),
                        }
                    }
                )

    # process
    (
        df_group_stats,
        n_outlier_sample_nonpar,
        n_outlier_sample_param,
        n_outlier_norms,
    ) = outlierDetection_normalized(
        subjects_sessions,
        freesurfer_dir,
        outlier_dir,
        outlierDict,
        min_no_subjects=outlier_params['min_no_subjects'],
        hypothalamus=outlier_params['hypothalamus'],
        hippocampus=outlier_params['hippocampus'],
        hippocampus_label=outlier_params['hippocampus_label'],
        fastsurfer=outlier_params['fastsurfer'],
    )

    # create a dictionary from outlier module output
    outlierDict = dict()
    for sub_sess in subjects_sessions:
        outlierDict.update(
            {
                sub_sess: {
                    "n_outlier_sample_nonpar_normalized": n_outlier_sample_nonpar[sub_sess],
                    "n_outlier_sample_param_normalized": n_outlier_sample_param[sub_sess],
                    # "n_outlier_norms": n_outlier_norms[sub_sess],  # valeurs de référence non normalisées par ETIV
                }
            }
        )

    # Convert outlierDict into a dataframe
    df_outliers = pd.DataFrame(outlierDict).T.reset_index()
    df_outliers = df_outliers.rename(columns={'index': 'subject'})

    return df_group_stats, df_outliers


def qc_freesurfer(args, subjects_sessions, job_ids=None):
    """
    Note : Note that a minimum of 5 supplied subjects is required for running outlier analyses,
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

    freesurfer_dir = f"{args.derivatives}/freesurfer"
    fsqc_dir = f"{args.derivatives}/qc/fsqc"

    # Create output (derivatives) directories
    os.makedirs(fsqc_dir, exist_ok=True)
    os.makedirs(f"{fsqc_dir}/stdout", exist_ok=True)
    os.makedirs(f"{fsqc_dir}/scripts", exist_ok=True)

    # Run FSQC on a list of subjects
    fsqc.run_fsqc(subjects_dir=freesurfer_dir,
                  output_dir=fsqc_dir,
                  subjects=subjects_sessions,
                  screenshots=args.qc_screenshots,
                  surfaces=args.qc_surfaces,
                  skullstrip=args.qc_skullstrip,
                  fornix=args.qc_fornix,
                  hypothalamus=args.qc_hypothalamus,
                  hippocampus=args.qc_hippocampus,
                  # shape=args.qc_screenshots,  # Runs a specific freesurfer commands not compatible with container
                  outlier=args.qc_outlier,  # Outliers are recomputed later after normalization by ETIV
                  skip_existing=args.qc_skip_existing
                  )

    # path_to_script = f"{args.derivatives}/qc/fsqc/scripts/fsqc.sh"
    # generate_slurm_script(args, subjects_sessions, path_to_script, job_ids)
    #
    # cmd = f"nohup sh {path_to_script} > {args.derivatives}/qc/fsqc/stdout/fsqc.out 2>&1 &"
    # print(f"[FSQC] Submitting task in background: {cmd}")
    # os.system(cmd)

    fsqc_results = pd.read_csv(f"{fsqc_dir}/fsqc-results.csv")

    cols = ["subject",
            "Number of folders generated",
            "Number of files generated",
            "Finished without error",
            "Processing time (hours)",
            "Euler number before topo correction LH",
            "Euler number after topo correction RH",
            "Euler number before topo correction LH",
            "Euler number after topo correction RH"]
    frames = []
    for sub_sess in subjects_sessions:
        log_file = f"{freesurfer_dir}/{sub_sess}/scripts/recon-all.log"
        info = None
        dir_count = 0
        file_count = 0
        if os.path.exists(log_file):
            info = read_log(log_file)
            dir_count = utils.count_dirs(f"{freesurfer_dir}/{sub_sess}")
            file_count = utils.count_files(f"{freesurfer_dir}/{sub_sess}")
        frames.append([sub_sess, dir_count, file_count] + list(info))
    logs = pd.DataFrame(frames, columns=cols)
    qc = pd.merge(fsqc_results, logs, on="subject", how="left")

    # Convert radians to degrees
    qc = convert_radians_to_degrees(qc)

    # Normalize ASEG volumes by ETIV
    columns_to_extract = ['aseg.BrainSegVol_to_eTIV', 'aseg.MaskVol_to_eTIV', 'aseg.lhSurfaceHoles',
                          'aseg.rhSurfaceHoles', 'aseg.SurfaceHoles']
    vols = normalize_aseg_volumes(freesurfer_dir, subjects_sessions, columns_to_extract)
    qc = pd.merge(qc, vols, on="subject", how="left")

    # Calculate outliers and save new group aparc/aseg statistics
    outlier_dir = os.path.join(fsqc_dir, "outliers")
    outlier_params = {
        'min_no_subjects': 5,
        'hypothalamus': args.qc_hypothalamus,
        'hippocampus': args.qc_hippocampus,
        'hippocampus_label': args.qc_hippocampus_label,
        'fastsurfer': False,
        'outlierDict': None
    }
    df_group_stats, df_outliers = calculate_outliers(freesurfer_dir, subjects_sessions, outlier_dir, outlier_params)
    df_group_stats.reset_index(inplace=True)
    path_to_group_stats = f"{fsqc_dir}/group_stats.csv"
    df_group_stats.to_csv(path_to_group_stats, index=False)

    qc = pd.merge(qc, df_outliers, on="subject", how="left")
    path_to_final_fsqc = f"{fsqc_dir}/fsqc-results-final.csv"
    qc.to_csv(path_to_final_fsqc, index=False)

    return None


if __name__ == "__main__":
    import sys
    args = sys.argv[1]
    subjects_sessions = sys.argv[2].split(",")
    freesurfer_job_ids = sys.argv[3].split(",") if len(sys.argv) > 3 else []

    # Appeler la fonction qc_freesurfer
    qc_freesurfer(args, subjects_sessions, freesurfer_job_ids)