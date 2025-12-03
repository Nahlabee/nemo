import os

DATA_BIDS_DIR = "/scratch/lhashimoto/nemo_database/imaging_data"
DERIVATIVES_BIDS_DIR = "/scratch/lhashimoto/nemo_database_derivatives"
# CODE_DIR="/scratch/lhashimoto/code/nemo"

FREESURFER_CONTAINER = "/scratch/lhashimoto/freesurfer-7.4.1.sif"
FREESURFER_LICENSE = "/scratch/lhashimoto/freesurfer/license"
# FREESURFER_DIR="/scratch/lhashimoto/freesurfer/outputs"
FREESURFER_DIR = DERIVATIVES_BIDS_DIR + "/freesurfer"
# FREESURFER_STDOUT = FREESURFER_DIR + "/stdout"
FREESURFER_QC = FREESURFER_DIR + "/qc"

QSIPREP_CONTAINER = "/scratch/lhashimoto/qsiprep-1.0.2.sif"
QSIPREP_DIR = DERIVATIVES_BIDS_DIR + "/qsiprep"

QSIRECON_CONTAINER = "/scratch/lhashimoto/qsirecon-1.1.1.sif"

CONFIG_EDDY = f"{os.path.dirname(__file__)}/dwi/eddy_params.json"
CONFIG_QSIPREP = f"{os.path.dirname(__file__)}/dwi/config_qsiprep.json"

SUBJECTS = ['1054001']
SESSIONS = ['01']

EMAIL=None
ACCOUNT=None

def print_paths():
    """
    Print paths to have access to them in a shell script
    """
    paths = {
        "DATA_BIDS_DIR": DATA_BIDS_DIR,
        "DERIVATIVES_BIDS_DIR": DERIVATIVES_BIDS_DIR,
        "FREESURFER_CONTAINER": FREESURFER_CONTAINER,
        "FREESURFER_LICENSE": FREESURFER_LICENSE,
        "FREESURFER_DIR": FREESURFER_DIR,
        "FREESURFER_STDOUT": FREESURFER_STDOUT,
        "QSIPREP_CONTAINER": QSIPREP_CONTAINER,
        "QSIRECON_CONTAINER": QSIRECON_CONTAINER,
        "CODE_DIR": CODE_DIR
    }
    for key, value in paths.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    print_paths()
