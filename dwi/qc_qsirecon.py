import os
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from dwi.qc_qsirecon_metrics_extractions import run as extract_qc_metrics
from dwi.run_qsirecon import is_already_processed as is_qsirecon_done


def run(config, subject, session, job_ids=None):

    common = config["common"]
    DERIVATIVES_DIR = common["derivatives"]

    # Create output (derivatives) directories
    os.makedirs(f"{DERIVATIVES_DIR}/qc/qsirecon", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/qsirecon/outputs", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/qsirecon/stdout", exist_ok=True)

    if not is_qsirecon_done(config, subject, session):
        print(f"[QC-QSIRECON] QSIrecon did not terminate for {subject} {session}. Please run QSIrecon command before QC.")
        return None

    print(f"[QC-QSIRECON] Performing QC metric extraction for {subject} {session}")
    try:
        extract_qc_metrics(config, subject, session)
    except Exception as e:
        print(f"[QC-QSIRECON] ERROR during QC extraction: {e}", file=sys.stderr)
        raise
