# Paths to container images
DIR_CONTAINER = "/home/henit/nemo/fmriprep/containers"

# Paths to BIDS input directory
DIR_INPUTS = "/home/henit/fmriprep_data/bids_dir"

# Outputs structures
DERIVATIVES = "/home/henit/fmriprep_data/derivatives"
FREESURFER_OUTPUTS = f"{DERIVATIVES}/freesurfer"

# Freesurfer license
FS_LICENSE = "/home/henit/fmriprep_data/license.txt"

# Freesurfer SIF
FREESURFER_SIF = f"{DIR_CONTAINER}/freesurfer_7.4.1.sif"

# Logging & QC
LOG_DIR = "/home/henit/fmriprep_data/logs"
QC_DIR = "/home/henit/fmriprep_data/qc"

QC_TABLE = "/home/henit/fmriprep_data/freesurfer_qc.csv"


DIR_FREESURFER = "/home/henit/freesurfer/freesurfer"

def print_paths():
    """
    Print paths to have access to them in a shell script
    """
    paths = {
        "DIR_CONTAINER": DIR_CONTAINER,
        "DIR_INPUTS": DIR_INPUTS,
        "FREESURFER_OUTPUTS": FREESURFER_OUTPUTS,
        "FS_LICENSE": FS_LICENSE,
        "DIR_FREESURFER": DIR_FREESURFER,
        "LOG_DIR": LOG_DIR,
        "QC_TABLE": QC_TABLE   
    }
    for key, value in paths.items():
        print(f"{key}={value}")

if __name__ == "__main__":
    print_paths()