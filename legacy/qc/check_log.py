# Auteur: Lucile Hashimoto (Adalab)
# Date: 2025

import os
import re
import csv
import glob
import numpy as np
from config import FREESURFER_FSQC, FREESURFER_OUTPUTS


def extract_info_from_log(log_file):
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


def count_dirs_in_directory(directory):
    """
    Count the number of directories in a given directory (non-recursively)

    """
    return sum([1 for item in os.listdir(directory) if os.path.isdir(os.path.join(directory, item))])


def count_files_in_directory(directory):
    """
    Count the number of files in a given directory

    """
    return sum([len(files) for _, _, files in os.walk(directory)])


def check_log_and_generate_csv(subj_list, output_csv):
    """
    Scan recon-all log files for each subject and save information in a CSV file

    """

    with open(output_csv, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Set header
        writer.writerow(["Subject",
                         "Number of folders generated",
                         "Number of files generated",
                         "Finished without error",
                         "Processing time (hours)",
                         "Euler number before topo correction LH",
                         "Euler number after topo correction RH",
                         "Euler number before topo correction LH",
                         "Euler number after topo correction RH"])

        # Scan log files for each subject
        for subj in subj_list:
            log_file = os.path.join(FREESURFER_OUTPUTS, subj, "scripts", "recon-all.log")
            print(log_file)
            if os.path.exists(log_file):
                info = extract_info_from_log(log_file)
                dir_count = count_dirs_in_directory(os.path.join(FREESURFER_OUTPUTS, subj))
                file_count = count_files_in_directory(os.path.join(FREESURFER_OUTPUTS, subj))
                # Write information to CSV
                writer.writerow([subj, dir_count, file_count] + list(info))
            else:
                print(f"Le fichier log pour le sujet {subj} n'existe pas.")


if __name__ == "__main__":
    # Get list of subjects with available freesurfer outputs
    subj_list = glob.glob(os.path.join(FREESURFER_OUTPUTS, "sub-*"))
    subj_list = [os.path.basename(x) for x in subj_list]

    # Set path to the output CSV file
    output_csv = os.path.join(FREESURFER_FSQC, "check_log.csv")  # Le fichier CSV de sortie

    # Extract information from log files and save to CSV
    check_log_and_generate_csv(subj_list, output_csv)

    print(f"Logs information have been saved in {output_csv}.")
