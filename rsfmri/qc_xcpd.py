#!/usr/bin/env python3
import os
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from rsfmri.qc_xcpd_metrics_extractions import run as extract_qc_metrics
from rsfmri.run_xcpd import is_already_processed as is_xcpd_done


def run_qc_xcpd(config, subject, session, job_ids=None):
    """
    Run QC and MRIQC on XCP-D outputs for a given subject and session.

    Parameters
    ----------
    config: 
        Configuration object.
    subject: str
        Subject identifier.
    session: str
        Session identifier.
    job_ids: list, optional
        List of SLURM job IDs to set as dependencies (default is None).
    """

    common = config["common"]
    DERIVATIVES_DIR = common["derivatives"]

    # Create output (derivatives) directories
    os.makedirs(f"{DERIVATIVES_DIR}/qc/xcpd", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/xcpd/outputs", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/xcpd/stdout", exist_ok=True)

    if not is_xcpd_done(config, subject, session):
        print(f"[QC-XCPD] XCP-D did not terminate for {subject} {session}. Please run XCP-D command before QC.")
        return None

    print(f"[QC-XCPD] Performing QC metric extraction for {subject} {session}")
    try:
        extract_qc_metrics(config, subject, session)
    except Exception as e:
        print(f"[QC-XCPD] ERROR during QC extraction: {e}", file=sys.stderr)
        raise

    # todo: qc group récupérer les valeurs de sub-003_ses-01_task-rest_space-fsLR_den-91k_desc-linc_qc.tsv