import os
import re
from datetime import datetime
import pandas as pd
import utils

def run(config, subjects_sessions,  job_ids=None):

    if job_ids is None:
        job_ids = []

    DERIVATIVES_DIR = config["common"]["derivatives"]

    # Create output (derivatives) directories
    os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep", exist_ok=True)
    # os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep/stdout", exist_ok=True)
    # os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep/scripts", exist_ok=True)
    # os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep/outliers", exist_ok=True)

    cols = ["subject",
            "session",
            "Process Run",
            "Finished without error",
            "Processing time (hours)",
            "Number of folders generated",
            "Number of files generated"]
    frames = []
    for sub_sess in subjects_sessions:
        subject = sub_sess.split('_')[0]
        session = sub_sess.split('_')[1]
        run_type = run_type
        finished_status, runtime = utils.read_log(config, subject, session, run_type)
        dir_count = utils.count_dirs(f"{DERIVATIVES_DIR}/{run_type}/{subject}/{session}")
        file_count = utils.count_files(f"{DERIVATIVES_DIR}/{run_type}/{subject}/{session}")
        frames.append([subject, session, run_type, finished_status, runtime, dir_count, file_count])
    qc = pd.DataFrame(frames, columns=cols)
    path_to_qc = f"{DERIVATIVES_DIR}/qc/{run_type}/qc.csv"
    qc.to_csv(path_to_qc, index=False)

    print(f"QC saved in {path_to_qc}\n")
    print(f"{run_type.capitalize()} Quality Check terminated successfully.")