import os
import sys
import pandas as pd
import utils


def generate_slurm_script(config, subject, session, path_to_script, job_ids=None):
    """
    Generate the SLURM job script for MRIQC FMRIPREP processing.

    Parameters
    ----------
    config : dict
        Configuration dictionary.
    subject : str
        Subject identifier.
    session : str
        Session identifier.
    path_to_script : str
        Path to save the generated SLURM script.
    job_ids : list, optional
        List of SLURM job IDs to set as dependencies (default is None).
    """

    common = config["common"]
    mriqc = config["mriqc"]
    DERIVATIVES_DIR = common["derivatives"]

    header = (
        f'#!/bin/bash\n'
        f'#SBATCH --job-name=qc_qsiprep_{subject}_{session}\n'
        f'#SBATCH --output={DERIVATIVES_DIR}/qc/qsiprep/stdout/qc_qsiprep_{subject}_{session}_%j.out\n'
        f'#SBATCH --error={DERIVATIVES_DIR}/qc/qsiprep/stdout/qc_qsiprep_{subject}_{session}_%j.err\n'
        f'#SBATCH --mem={mriqc["requested_mem"]}\n'
        f'#SBATCH --time={mriqc["requested_time"]}\n'
        f'#SBATCH --partition={mriqc["partition"]}\n'
    )

    if job_ids:
        header += f'#SBATCH --dependency=afterok:{":".join(job_ids)}\n'

    if common.get("email"):
        header += (
            f'#SBATCH --mail-type={common["email_frequency"]}\n'
            f'#SBATCH --mail-user={common["email"]}\n'
        )

    if common.get("account"):
        header += f'#SBATCH --account={common["account"]}\n'

    module_export = (
        f'\nmodule purge\n'
        f'module load userspace/all\n'
        f'module load singularity\n'
        f'module load python3/3.12.0\n'
        f'source {common["python_env"]}/bin/activate\n'
    )

    prereq_check = (
        f'\n# Check that QSIPREP outputs exists\n'
        f'if [ ! -d "{DERIVATIVES_DIR}/qsiprep/outputs/{subject}/{session}" ]; then\n'
        f'    echo "[QC-QSIPREP] Please run QSIprep command before QC."\n'
        f'    exit 1\n'
        f'fi\n'

        f'\n# Check that QSIPREP finished without error\n'
        f'prefix="{DERIVATIVES_DIR}/qsiprep/stdout/qsiprep_{subject}_{session}"\n'
        f'found_success=false\n'
        f'for file in $(ls $prefix*.out 2>/dev/null); do\n'
        f'    if grep -q "QSIPrep finished successfully" $file; then\n'
        f'        found_success=true\n'
        f'    fi\n'
        f'done\n'
        f'if [ "$found_success" = false ]; then\n'
        f'    echo "[QC-QSIPREP] QSIPrep did not terminate for {subject} {session}. Please run QSIPrep command before QC."\n'
        f'    exit 1\n'
        f'fi\n'
    )

    # Define the Singularity command for running MRIQC
    # Note: Unlike other BIDS apps, no config file is used here, the option doesn't exist for mriqc
    input_dir = f"{DERIVATIVES_DIR}/qsiprep/outputs"

    #todo: voir version Heni
    singularity_command = (
            f'\napptainer run \\\n'
            f'    --cleanenv \\\n'
            f'    -B {input_dir}:/data:ro \\\n'
            f'    -B {DERIVATIVES_DIR}/qc/qsiprep:/out \\\n'
            f'    -B {mriqc["bids_filter_dir"]}:/bids_filter_dir \\\n'
            f'    {mriqc["mriqc_container"]} /data /out/outputs participant \\\n'
            f'    --participant_label {subject} \\\n'
            f'    --session-id {session} \\\n'
            f'    --mem {mriqc["requested_mem"]} \\\n'
            f'    -w /out/work \\\n'
            f'    --fd_thres 0.5 \\\n'
            f'    --verbose-reports \\\n'
            f'    --verbose \\\n'
            f'    --no-sub --notrack\n'
        )

    # Call to python scripts for the rest of QC
    # todo : adapt to qsiprep
    python_command = (
        f'\necho "Running QC metrics extraction"\n'
        f'python3 dwi/qc_fmriprep_metrics_extractions.py {config} {subject} {session}\n'
    )
    # python_command = (
    #     f'\npython3 anat/qc_freesurfer.py '
    #     f"'{json.dumps(config)}' {','.join(subjects_sessions)}"
    # )

    # Add permissions for shared ownership of the output directory
    ownership_sharing = f'\nchmod -Rf 771 {DERIVATIVES_DIR}/qc/qsiprep\n'

    # Write the complete SLURM script to the specified file
    with open(path_to_script, 'w') as f:
        f.write(header + module_export + prereq_check + singularity_command + python_command + ownership_sharing)


def run(config, subject, session, job_ids=None):

    # todo: Check si QC + MRIQC processed (ligne dans le csv final ?)
    if is_already_processed(config, subject, session):
        return None

    common = config["common"]
    DERIVATIVES_DIR = common["derivatives"]

    # Create output (derivatives) directories
    os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep/stdout", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep/scripts", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep/outliers", exist_ok=True)
    os.makedirs(f"{DERIVATIVES_DIR}/qc/qsiprep/work", exist_ok=True)


    if not utils.is_mriqc_done(config, subject, session, runtype='fmriprep'):
        path_to_script = f"{DERIVATIVES_DIR}/qc/qsiprep/scripts/qc_qsiprep_{subject}_{session}.slurm"
        generate_slurm_script(config, subject, session, path_to_script, job_ids=job_ids)
        cmd = f"sbatch {path_to_script}"
        print(f"[QC-QSIPREP] Submitting job: {cmd}")
        job_id = utils.submit_job(cmd)
        return job_id

    else:
        print(f"[QC-QSIPREP] Skip already processed MRIQC")
        print(f"[QC-QSIPREP] Performing only python command extraction for {subject}_{session}")
        try:
            extract_qc_metrics(config, subject, session)
        except Exception as e:
            print(f"[QC-QSIPREP] ERROR during QC extraction: {e}", file=sys.stderr)
            raise

    # todo: Dans le script slurm :
    # - appeler MRIQC pour qsiprep individuel
    # - appeler python pour le calcul des autres métriques et combinaison des valeurs dans un tableau unique

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
        # todo: move read_log to utils
        finished_status, runtime = read_log(config, subject, session)
        dir_count = utils.count_dirs(f"{DERIVATIVES_DIR}/qsiprep/{subject}/{session}")
        file_count = utils.count_files(f"{DERIVATIVES_DIR}/qsiprep/{subject}/{session}")
        frames.append([subject, session, finished_status, runtime, dir_count, file_count])

        # Extract values, mean, max, std of metrics from qsiprep outputs :
        # - confounds_timeseries
        # - image_qc
        # put values in frames
    qc = pd.DataFrame(frames, columns=cols)

    path_to_qc = f"{DERIVATIVES_DIR}/qc/qsiprep/qc.csv"
    qc.to_csv(path_to_qc, index=False)

    print(f"QC saved in {path_to_qc}\n")

    print("QSIprep Quality Check terminated successfully.")

