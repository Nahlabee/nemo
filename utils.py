import json
import os
import subprocess
from pathlib import Path

def get_subjects(input_dir, specified_subjects=None):
    """
    Retrieve the list of subjects from the input directory or use the specified list.

    Parameters
    ----------
    input_dir : str
        Path to the input directory containing the dataset in BIDS format.
    specified_subjects : list or None
        List of subjects to process. If None, all subjects in the input directory are retrieved.

    Returns
    -------
    list
        List of subjects.
    """
    if specified_subjects:
        return [f"sub-{sub}" if not sub.startswith("sub-") else sub for sub in specified_subjects]

    return sorted(d for d in os.listdir(input_dir) if d.startswith("sub-") and os.path.isdir(os.path.join(input_dir, d)))
    


def get_sessions(input_dir, subject, specified_sessions=None):
    """
    Retrieve the list of sessions for a given subject or use the specified list.

    Parameters
    ----------
    input_dir : str
        Path to the input directory containing the dataset in BIDS format.
    subject : str
        Subject identifier (e.g., "sub-01").
    specified_sessions : list or None
        List of sessions to process. If None, all sessions in the subject directory are retrieved.

    Returns
    -------
    list
        List of sessions.
    """
    subject_path = os.path.join(input_dir, subject)
    if specified_sessions:
        return [f"ses-{ses}" if not ses.startswith("ses-") else ses for ses in specified_sessions]

    return sorted(d for d in os.listdir(subject_path) if d.startswith("ses-") and os.path.isdir(os.path.join(subject_path, d)))

def subject_exists(input_dir, subject):
    """
    Check if the subject directory exists in the input directory.
    
    :param input_dir: Description
    :param subject: Description
    :return: Description
    
    """

    return (Path(input_dir) / subject).exists()

def has_anat(input_dir, subject):
    """
    Check if the subject has anatomical data.
    
    :param input_dir: Description
    :param subject: Description
    :return: Description
    
    """
    return any((Path(input_dir)/subject).glob("**/anat/*T1w.nii*"))

def has_dwi(input_dir, subject):
    """
    Check if the subject has diffusion-weighted imaging (DWI) data.
    
    :param input_dir: Description
    :param subject: Description
    :return: Description
    
    """
    return any((Path(input_dir)/subject).glob("**/dwi/*dwi.nii*"))

def has_func_fmap(input_dir, subject):
    """
    Check if the subject has functional MRI data along with field maps.
    
    :param input_dir: Description
    :param subject: Description
    :return: Description
    
    """
    return any((Path(input_dir)/subject).glob("**/func/*bold.nii*")) and any((Path(input_dir)/subject).glob("**/fmap/*"))


def submit_job(cmd):
    """
    Submits a SLURM job using the provided command and returns the job ID.

    Parameters
    ----------
    cmd : str
        The command to submit the SLURM job, typically using `sbatch`.

    Returns
    -------
    str or None
        The SLURM job ID if the submission is successful, or None if the submission fails.

    Notes
    -----
    - The function executes the `sbatch` command using the `subprocess.run` method.
    - It captures the output of the command to extract the job ID.
    - If the command fails or the job ID cannot be extracted, the function returns None.
    - The function prints messages to indicate the success or failure of the job submission.
    """
    try:
        # Execute the sbatch command and capture the output
        result = subprocess.run(cmd, shell=True, check=True, text=True, capture_output=True)
        output = result.stdout.strip()

        # Parse the output to extract the job ID
        if output.startswith("Submitted batch job"):
            job_id = output.split()[-1]
            print(f"SLURM job successfully submitted: ID {job_id}")
            return job_id
        else:
            print("Unable to retrieve the SLURM job ID.")
            return None
    except subprocess.CalledProcessError as e:
        # Handle errors during the job submission process
        print(f"Error while submitting the SLURM job: {e}")
        return None


def count_dirs(directory):
    """
    Count the number of directories in a given directory (non-recursively)

    """
    if os.path.isdir(directory):
        return sum([1 for item in os.listdir(directory) if os.path.isdir(os.path.join(directory, item))])
    else:
        return 0


def count_files(directory):
    """
    Count the number of files in a given directory

    """
    if os.path.isdir(directory):
        return sum([len(files) for _, _, files in os.walk(directory)])
    else:
        return 0