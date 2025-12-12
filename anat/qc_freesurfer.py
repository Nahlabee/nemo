import json
import os
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
import fsqc as fsqc_toolbox


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


def normalize_aseg_volumes(freesurfer_dir, subjects_sessions, columns_to_extract,
                           ETIV='aseg.EstimatedTotalIntraCranialVol'):
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
        df_aseg_norm = df_aseg.drop(columns=columns_to_extract)
        df_aseg_norm = df_aseg_norm.div(df_aseg[ETIV], axis=0)
        aseg_norm = f"{freesurfer_dir}/{sub_sess}/stats/aseg_stats_norm.csv"
        df_aseg_norm.to_csv(aseg_norm, index=False)
        print(f"Normalized aseg volumes saved in {aseg_norm}")

        # Extract columns for QC
        df_sub = df_aseg[columns_to_extract].copy()
        df_sub.loc[0, 'subject'] = sub_sess
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


def qc_freesurfer(config, subjects_sessions):
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

    common = config["common"]
    fsqc = config["fsqc"]
    DERIVATIVES_DIR = common["derivatives"]

    frames = []
    for ss in subjects_sessions:
        subject, session = ss.split("/")
        print(subject, session)

        # Run FSQC on a list of subjects
        print("Running FSQC on subject")
        os.makedirs(f"{DERIVATIVES_DIR}/qc/fsqc/{session}", exist_ok=True)
        fsqc_toolbox.run_fsqc(subjects_dir=f"{DERIVATIVES_DIR}/freesurfer/{session}",
                              output_dir=f"{DERIVATIVES_DIR}/qc/fsqc/{session}/{subject}",
                              subjects=subject,
                              screenshots=fsqc["qc_screenshots"],
                              surfaces=fsqc["qc_surfaces"],
                              skullstrip=fsqc["qc_skullstrip"],
                              fornix=fsqc["qc_fornix"],
                              hypothalamus=fsqc["qc_hypothalamus"],
                              hippocampus=fsqc["qc_hippocampus"],
                              shape=False,  # Runs a freesurfer commands not accessible with container
                              outlier=False,  # Outliers are recomputed later after normalization by ETIV
                              skip_existing=fsqc["qc_skip_existing"],
                              no_group=True
                              )

        print("\n---------------------------------------")
        print("Running log verification")
        subject_dir = f"{DERIVATIVES_DIR}/freesurfer/{session}/{subject}"
        log_file = f"{subject_dir}/scripts/recon-all.log"
        info = None
        dir_count = 0
        file_count = 0
        if os.path.exists(log_file):
            info = read_log(log_file)
            dir_count = utils.count_dirs(subject_dir)
            file_count = utils.count_files(subject_dir)
        frames.append([subject, session, dir_count, file_count] + list(info))

    # Run group-level statistics
    fsqc_toolbox.run_fsqc(subjects_dir=f"{DERIVATIVES_DIR}/freesurfer",
                          output_dir=f"{DERIVATIVES_DIR}/qc/fsqc",
                          group_only=True,
                          screenshots_html=fsqc["qc_screenshots"],
                          surfaces_html=fsqc["qc_surfaces"],
                          skullstrip_html=fsqc["qc_skullstrip"],
                          fornix_html=fsqc["qc_fornix"],
                          hypothalamus_html=fsqc["qc_hypothalamus"],
                          hippocampus_html=fsqc["qc_hippocampus"],
                          shape=False,
                          outlier=fsqc["qc_outlier"],  # Outliers are recomputed later after normalization by ETIV
                          skip_existing=fsqc["qc_skip_existing"]
                          )
    # Merge statistics
    cols = ["subject", "session",
            "Number of folders generated",
            "Number of files generated",
            "Finished without error",
            "Processing time (hours)",
            "Euler number before topo correction LH",
            "Euler number after topo correction RH",
            "Euler number before topo correction LH",
            "Euler number after topo correction RH"]
    fsqc_results = pd.read_csv(f"{DERIVATIVES_DIR}/qc/fsqc/fsqc-results.csv")
    logs = pd.DataFrame(frames, columns=cols)
    qc = pd.merge(logs, fsqc_results, on="subject", how="left")

    # Convert radians to degrees
    qc = convert_radians_to_degrees(qc)

    # # Normalize ASEG volumes by ETIV
    # print("\n---------------------------------------")
    # print("Running volume normalization")
    # columns_to_extract = ['aseg.EstimatedTotalIntraCranialVol',
    #                       'aseg.BrainSegVol_to_eTIV', 'aseg.MaskVol_to_eTIV', 'aseg.lhSurfaceHoles',
    #                       'aseg.rhSurfaceHoles', 'aseg.SurfaceHoles']
    # vols = normalize_aseg_volumes(f"{DERIVATIVES_DIR}/freesurfer", subjects_sessions, columns_to_extract)
    # qc = pd.merge(qc, vols, on="subject", how="left")
    #
    # # Calculate outliers and save new group aparc/aseg statistics
    # outlier_dir = f"{DERIVATIVES_DIR}/qc/fsqc/outliers"
    # outlier_params = {
    #     'min_no_subjects': 5,
    #     'hypothalamus': fsqc["qc_hypothalamus"],
    #     'hippocampus': fsqc["qc_hippocampus"],
    #     'hippocampus_label': fsqc["qc_hippocampus_label"],
    #     'fastsurfer': False,
    #     'outlierDict': None
    # }
    # df_group_stats, df_outliers = calculate_outliers(f"{DERIVATIVES_DIR}/freesurfer", subjects_sessions, outlier_dir,
    #                                                  outlier_params)
    # df_group_stats.reset_index(inplace=True)
    # path_to_group_stats = f"{DERIVATIVES_DIR}/qc/fsqc/group_aparc-aseg_values.csv"
    # df_group_stats.to_csv(path_to_group_stats, index=False)
    # qc = pd.merge(qc, df_outliers, on="subject", how="left")

    path_to_final_fsqc = f"{DERIVATIVES_DIR}/qc/fsqc/fsqc-results-final.csv"
    qc.to_csv(path_to_final_fsqc, index=False)

    print(f"QC saved in {path_to_final_fsqc}\n")

    print("FreeSurfer Quality Check terminated successfully.")

    return None


def generate_bash_script(config, subjects_sessions, path_to_script, job_ids=None):
    common = config["common"]
    fsqc = config["fsqc"]
    DERIVATIVES_DIR = common["derivatives"]

    module_export = (
        f'\nmodule purge\n'
        f'module load userspace/all\n'
        f'module load singularity\n'
        f'module load python3/3.12.0\n'
        f'source {common["python_env"]}/bin/activate\n'
        f'cd /scratch/lhashimoto/code/nemo'
    )

    # Call to python scripts for the rest of QC
    # todo: test json dump with dict
    subjects_sessions_str = " ".join([f"{sub}/{ses}" for (sub, ses) in subjects_sessions])
    python_command = (
        f'\npython3 anat/qc_freesurfer.py '
        f"'{json.dumps(config)}' {subjects_sessions_str}"
    )

    # Add permissions for shared ownership of the output directory
    ownership_sharing = f'\nchmod -Rf 771 {DERIVATIVES_DIR}/qc/fsqc\n'

    # Write the complete BASH script to the specified file
    with open(path_to_script, 'w') as f:
        # f.write(module_export + singularity_command + python_command + ownership_sharing)
        f.write(module_export + python_command + ownership_sharing)


def run(config, subjects_sessions, job_ids=None):
    """
    Run FreeSurfer QC
    Note that FSQC must run on interactive mode to be able to display (and save) graphical outputs
    """

    common = config["common"]
    fsqc = config["fsqc"]
    DERIVATIVES_DIR = common["derivatives"]

    # Create output (derivatives) directories
    os.makedirs(f"{DERIVATIVES_DIR}/qc/fsqc", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/fsqc/stdout", exist_ok=True)
    # os.makedirs(f"{DERIVATIVES_DIR}/qc/fsqc/scripts", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/fsqc/outliers", exist_ok=True)

    path_to_script = f"{DERIVATIVES_DIR}/qc/fsqc/scripts/fsqc.sh"
    generate_bash_script(config, subjects_sessions, path_to_script)

    cmd = (f'\nsrun --job-name=fsqc --ntasks=1 '
           f'--partition={fsqc["partition"]} '
           f'--mem={fsqc["requested_mem"]}gb '
           f'--time={fsqc["requested_time"]} '
           f'--out={DERIVATIVES_DIR}/qc/fsqc/stdout/fsqc.out '
           f'--err={DERIVATIVES_DIR}/qc/fsqc/stdout/fsqc.err ')

    if job_ids:
        cmd += f'--dependency=afterok:{":".join(job_ids)} '

    cmd += f"sh {path_to_script}"

    os.system(cmd)
    print(f"[FSQC] Submitting (background) task on interactive node")
    return


def main():
    # Convert arguments
    config = json.loads(sys.argv[1])  # Convert JSON into dict
    subjects_sessions = sys.argv[2].split()  # Split subjects list

    qc_freesurfer(config, subjects_sessions)
