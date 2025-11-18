#!/usr/bin/env python3
import os
import sys
import subprocess
import shutil
import config
import pandas as pd
from datetime import datetime
from qc_generator import generate_qc_pdf


# Create necessary directories
os.makedirs(config.LOG_DIR + "/freesurfer", exist_ok=True)
os.makedirs(config.LOG_DIR + "/xcp_d", exist_ok=True)
os.makedirs(os.path.dirname(config.QC_TABLE), exist_ok=True)

# Set required environment vars
os.environ["FREESURFER_HOME"] = "/usr/local/freesurfer"
os.environ["FS_LICENSE"] = "/usr/local/freesurfer/license.txt"
os.environ["SUBJECTS_DIR"] = config.FREESURFER_OUTPUTS

print("FREESURFER_HOME:", os.environ['FREESURFER_HOME'])
print("FS_LICENSE:", os.environ['FS_LICENSE'])
print("SUBJECTS_DIR:", os.environ['SUBJECTS_DIR'])

def log_path(tool, subject):
    """Generate log file path.
    """
    return f"{config.LOG_DIR}/{tool}/{subject}.log"


def run_cmd(cmd, logfile):
    """Run a command and log output to a file."""
    print(f"\n[RUNNING] {' '.join(cmd)}")
    with open(logfile, "w") as f:
        subprocess.run(cmd, stdout=f, stderr=f, text=True, check=True)


def run_freesurfer(subject):
    """Run Freesurfer's recon-all for a given subject.
    """

    # Cleanup previous recon (optional)
    subject_dir = f"{config.FREESURFER_OUTPUTS}/{subject}"
    if os.path.exists(subject_dir):
        shutil.rmtree(subject_dir)

    cmd = [
    "apptainer", "exec",
    "-B", f"{config.DIR_INPUTS}:/data",
    "-B", f"{config.FREESURFER_OUTPUTS}:/output",
    "-B", f"{config.FS_LICENSE}:/usr/local/freesurfer/license.txt",
    config.FREESURFER_SIF,
    "bash", "-c",
    "export FREESURFER_HOME=/usr/local/freesurfer && "
    "source $FREESURFER_HOME/SetUpFreeSurfer.sh && "
    f"recon-all -all -s {subject} "
    f"-i /data/{subject}/ses-01/anat/{subject}_ses-01_T1w.nii.gz "
    f"-sd /output"
]

    run_cmd(cmd, log_path("freesurfer", subject))

def extract_qc(subject):
    stats_file = f"{config.FREESURFER_OUTPUTS}/{subject}/stats/aseg.stats"
    if not os.path.exists(stats_file):
        return None

    cortex_size = 0
    for line in open(stats_file):
        if "CortexVolume" in line:
            cortex_size = int(line.split()[3])

    return {"subject": subject, "cortex_volume": cortex_size}

def main(subject):
    """Main processing function.
    """
    print(f"\n=== Processing {subject} ===")

    run_freesurfer(subject)
    qc = extract_qc(subject)

    if qc:
        df = pd.DataFrame([qc])
        if not os.path.exists(config.QC_TABLE):
            df.to_csv(config.QC_TABLE, index=False)
        else:
            df.to_csv(config.QC_TABLE, mode='a', header=False, index=False)

    generate_qc_pdf(subject, config.FREESURFER_OUTPUTS,
                    f"{config.LOG_DIR}/freesurfer/{subject}_qc.pdf")
    print(f"✅ Completed {subject}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ./pipeline.py <subject>")
        sys.exit(1)

    main(sys.argv[1])