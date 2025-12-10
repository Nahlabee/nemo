import os
import re
from datetime import datetime
import pandas as pd
import utils


def extract_runtime(content):
    # Expression régulière pour capturer les timestamps
    timestamp_pattern = r"\d{6}-\d{2}:\d{2}:\d{2}"

    # Trouver tous les timestamps dans le fichier
    timestamps = re.findall(timestamp_pattern, content)

    if not timestamps:
        return 0

    # Convertir les timestamps en objets datetime
    first_timestamp = datetime.strptime(timestamps[0], "%y%m%d-%H:%M:%S")
    last_timestamp = datetime.strptime(timestamps[-1], "%y%m%d-%H:%M:%S")

    # Calculer le runtime
    runtime = last_timestamp - first_timestamp

    return runtime


def read_log(config, subject, session):

    finished_status = "Error"
    runtime = 0

    DERIVATIVES_DIR = config.config["common"]["derivatives"]
    stdout_dir = f"{DERIVATIVES_DIR}/qsiprep/stdout"

    # Check that QSIprep finished without error
    if not os.path.exists(stdout_dir):
        return finished_status, runtime

    prefix = f"qsiprep_{subject}_{session}"
    stdout_files = [f for f in os.listdir(stdout_dir) if (f.startswith(prefix) and f.endswith('.out'))]
    if not stdout_files:
        return finished_status, runtime

    for file in stdout_files:
        file_path = os.path.join(stdout_dir, file)
        with open(file_path, 'r') as f:
            content = f.read()
            if 'QSIPrep finished successfully!' in content:
                finished_status = "Success"
                try:
                    runtime = extract_runtime(content)
                except ValueError as e:
                    print(e)

    return finished_status, runtime


def run(config, subjects_sessions, job_ids=None):

    if job_ids is None:
        job_ids = []

    DERIVATIVES_DIR = config.config["common"]["derivatives"]

    # Create output (derivatives) directories
    os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep", exist_ok=True)
    # os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep/stdout", exist_ok=True)
    # os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep/scripts", exist_ok=True)
    # os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep/outliers", exist_ok=True)

    cols = ["subject",
            "session",
            "Finished without error",
            "Processing time (hours)",
            "Number of folders generated",
            "Number of files generated"]
    frames = []
    for sub_sess in subjects_sessions:
        subject = sub_sess.split('_')[0]
        session = sub_sess.split('_')[1]
        finished_status, runtime = read_log(config, subject, session)
        dir_count = utils.count_dirs(f"{DERIVATIVES_DIR}/qsiprep/{subject}/{session}")
        file_count = utils.count_files(f"{DERIVATIVES_DIR}/qsiprep/{subject}/{session}")
        frames.append([subject, session, finished_status, runtime, dir_count, file_count])
    qc = pd.DataFrame(frames, columns=cols)

    path_to_qc = f"{DERIVATIVES_DIR}/qc/qsiprep/qc.csv"
    qc.to_csv(path_to_qc, index=False)

    print(f"QC saved in {path_to_qc}\n")

    print("QSIprep Quality Check terminated successfully.")

