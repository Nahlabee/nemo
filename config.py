DATA_BIDS_DIR = "/scratch/lhashimoto/nemo_database/imaging_data"
DERIVATIVES_BIDS_DIR = "/scratch/lhashimoto/nemo_database_derivatives"

FREESURFER_CONTAINER = "/scratch/lhashimoto/freesurfer-7.4.1.sif"
FREESURFER_LICENSE = "/scratch/lhashimoto/freesurfer/license"
FREESURFER_DIR = DERIVATIVES_BIDS_DIR + "/freesurfer"
FREESURFER_STDOUT = FREESURFER_DIR + "/stdout"
FREESURFER_OUTPUTS = FREESURFER_DIR + "/outputs"
FREESURFER_QC = FREESURFER_DIR + "/qc"


# def print_paths():
#     """
#     Print paths to have access to them in a shell script
#     """
#     paths = {
#         "DIR_CONTAINER": DIR_CONTAINER,
#         "DIR_INPUTS": DIR_INPUTS,
#         "DIR_FREESURFER": DIR_FREESURFER,
#         "FREESURFER_STDOUT": FREESURFER_STDOUT,
#         "FREESURFER_OUTPUTS": FREESURFER_OUTPUTS
#     }
#     for key, value in paths.items():
#         print(f"{key}={value}")
#
# if __name__ == "__main__":
#     print_paths()