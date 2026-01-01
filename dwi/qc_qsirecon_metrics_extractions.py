#!/usr/bin/env python3
import json
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
import utils


# -----------------------
# Main extraction
# -----------------------
def run(config, subject, session):
    """
    Extract QC metrics from QSIPrep outputs.

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
    qsirecon_dir = f"{DERIVATIVES_DIR}/qsirecon/outputs/{subject}/{session}"

    try:
        # Extract process status from log files
        finished_status, runtime = utils.read_log(config, subject, session, runtype="qsirecon")
        dir_count = utils.count_dirs(qsirecon_dir)
        file_count = utils.count_files(qsirecon_dir)

        # Compute QC metrics
        row = dict(
            subject=subject,
            session=session,
            Process_Run="qsiprep",
            Finished_without_error=finished_status,
            Processing_time_hours=runtime,
            Number_of_folders_generated=dir_count,
            Number_of_files_generated=file_count,
        )

        sub_ses_qc = pd.DataFrame([row])
        # Save outputs to csv file
        path_to_qc = f"{DERIVATIVES_DIR}/qc/qsirecon/outputs/{subject}/{session}/{subject}_{session}_qc.csv"
        sub_ses_qc.to_csv(path_to_qc, mode='w', header=True, index=False)
        print(f"QC saved in {path_to_qc}\n")

        print(f"QSIRecon Quality Check terminated successfully for {subject} {session}.")

    except Exception as e:
        print(f"⚠️ ERROR: QC aborted for {subject} {session}: \n{e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 4:
        raise RuntimeError(
            "Usage: python qc_qsirecon_metrics_extractions.py <config> <subject> <session>"
        )
    config = json.loads(sys.argv[1])
    subject = sys.argv[2]
    session = sys.argv[3]
    run(config, subject, session)
