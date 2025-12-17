# run_workflow.py
"""
This script orchestrates the execution of various neuroimaging workflows including
fMRIPrep, FreeSurfer, QSIprep, QSIrecon, XCP-D, and MRIQC. It reads configuration
settings from a TOML file, checks for the existence of required data, and submits
jobs for each workflow step based on the specified subjects and sessions.
It also handles group-level MRIQC jobs for the processed data.

It is designed to be run in a Slurm environment, where each step can be submitted as a job.
Usage:
    python run_workflow.py [--config <path_to_config_file>]
"""

import os
from datetime import datetime
from pathlib import Path
import sys
from dwi import qc_qsiprep
#from anat import qc_freesurfer
import toml
sys.path.append(str(Path(__file__).resolve().parent))
import utils
from anat.run_freesurfer import run_freesurfer
from dwi.run_qsiprep import run_qsiprep
from dwi.run_qsirecon import run_qsirecon
#from anat.qc_freesurfer import run as run_freesurfer_qc
from rsfmri.run_fmriprep import run_fmriprep
from rsfmri.run_mriqc_raw import run_mriqc as run_mriqc_raw
from rsfmri.qc_fmriprep import run_qc_fmriprep
from rsfmri.run_xcpd import run_xcpd
from rsfmri.qc_xcpd import run_qc_xcpd 
from rsfmri.run_mriqc_group import run_mriqc_group
from config import config

def main(config_file=None):
    """
    Main function to execute the workflow steps based on the configuration file.
    Parameters
    ----------
    config_file : str, optional
        Path to the configuration file. If None, a default path is used.
    """

    # -------------------------------
    # Load configuration
    # -------------------------------
    if not config_file:
        config_file = f"{Path(__file__).parent}/config/config.toml"
    config = utils.load_config(config_file)

    common = config["common"]
    workflow = config["workflow"]
    freesurfer = config["freesurfer"]
    qsiprep = config["qsiprep"]
    qsirecon = config["qsirecon"]
    fmriprep = config["fmriprep"]
    mriqc = config["mriqc"]
    xcpd = config["xcpd"]
    fsqc = config["fsqc"]
    BIDS_DIR = common["input_dir"]
    DERIVATIVES_DIR = common["derivatives"]

    # Save config with datetime
    filename = f"{DERIVATIVES_DIR}/config_{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    with open(filename, "w") as f:
        toml.dump(config, f)

    # -------------------------------------------------------
    # Sanity checks
    # -------------------------------------------------------
    if not os.path.exists(BIDS_DIR):
        print("Dataset directory does not exist.")
        return 0

    subjects_sessions = []
    freesurfer_job_ids = []
    qsiprep_job_ids = []
    mriqc_job_ids = []

    qsirecon_job_ids = []
    xcpd_job_ids = []
    # -------------------------------------------------------
    # Loop over subjects and sessions
    subjects = utils.get_subjects(BIDS_DIR, common.get('subjects'))

    print("\nThe following subjects will be processed :", subjects)

    # -------------------------------------------------------
    # Workflow per subject
    # -------------------------------------------------------
    for subject in subjects:
        # Check if subject exists
        if not utils.subject_exists(BIDS_DIR, subject):
            print(f"[WARNING] Subject {subject} does not exist in the input directory. Skipping.")
            continue

        print(f"\n================ {subject} ================")

        # Initialize job IDs list for the current subject
        job_ids = []
       
        # -------------------------------------------
        # Loop over sessions
        # -------------------------------------------
        sessions = utils.get_sessions(BIDS_DIR, subject, common.get('sessions'))

        fmriprep_sub_job_ids = []
        
        for session in sessions:
            print('\n', subject, ' - ', session, '\n')
            subjects_sessions.append(f"{subject}_{session}")

            # -------------------------------------------
            # 0. MRIQC on raw BIDS data
            # -------------------------------------------
            if workflow["run_mriqc_raw"]:
                print("🔹 Submitting MRIQC (raw data)")
                mriqc_raw_job_id = run_mriqc_raw(
                    config,
                    subject=subject,
                    session=session,
                    data_type="raw"
                    )
                mriqc_job_ids.append(mriqc_raw_job_id)
                print(f"[MRIQC-RAW] job IDs: {mriqc_raw_job_id}\n")
            else:
                mriqc_raw_job_id = None    
            # -------------------------------------------
            # 1. FREESURFER
            # -------------------------------------------
            if workflow["run_freesurfer"]:
                print("🔹 Submitting freesurfer")
                freesurfer_job_id = run_freesurfer(config, subject, session)
                freesurfer_job_ids.append(freesurfer_job_id)
                print(f"[FREESURFER] job IDs: {freesurfer_job_id}\n")
            else:
                freesurfer_job_id = None
            
            # -------------------------------------------
            # 2. QSIprep and QSIrecon
            # -------------------------------------------
            if workflow["run_qsiprep"]:
                print("🔹 Submitting QSIprep")
                qsiprep_job_id = run_qsiprep(config, subject, session)
                qsiprep_job_ids.append(qsiprep_job_id)
                print(f"[QSIPREP] job IDs: {qsiprep_job_id}\n")
            else:
                qsiprep_job_id = None

            if workflow["run_qsirecon"]:
                print("🔹 Submitting QSIrecon")
                dependencies = [job_id for job_id in [freesurfer_job_id, qsiprep_job_id] if job_id is not None]
                qsirecon_job_id = run_qsirecon(config, subject, session, dependencies)
                qsirecon_job_ids.append(qsirecon_job_id)
                print(f"[QSIRECON] job IDs: {qsirecon_job_id}\n")
            else:
                qsirecon_job_id = None
            
            # -------------------------------------------
            # 3.a fMRIPrep 
            # -------------------------------------------

            if workflow["run_fmriprep"]:
                print("🔹 Submitting fMRIPrep")
                dependencies = [job_id for job_id in [freesurfer_job_id] + fmriprep_sub_job_ids if job_id is not None]
                fmriprep_job_id = run_fmriprep(config, 
                                               subject=subject,
                                               session=session,
                                               job_ids=dependencies)
                print(f"[FMRIPREP] job IDs: {fmriprep_job_id}\n")
                fmriprep_sub_job_ids.append(fmriprep_job_id)
            else:
                fmriprep_job_id = None
            
            # -------------------------------------------
            # 3.b QC fMRIPrep 
            # -------------------------------------------

            if workflow["run_mriqc_derivatives"]:
                print("🔹 Submitting MRIQC (fMRIPrep)")
                dependencies_mriqc_fmriprep = [job_id for job_id in [freesurfer_job_id, fmriprep_job_id] if job_id is not None]
                mriqc_fmriprep_job_id = run_qc_fmriprep(
                    config,
                    subject=subject,
                    session=session,
                    job_ids=dependencies_mriqc_fmriprep
                )
                print(f"[MRIQC-FMRIPREP] job IDs: {mriqc_fmriprep_job_id}\n")
            else:
                fmriprep_job_id = None


            # -------------------------------------------
            # 4.a XCP-D
            # -------------------------------------------
            if workflow["run_xcpd"]:
                print("🔹 Submitting XCP-D")
                dependencies_xcpd = [job_id for job_id in [freesurfer_job_id, fmriprep_job_id] if job_id is not None]
                xcpd_job_id = run_xcpd(
                    config,
                    subject=subject,
                    session=session,
                    job_ids=dependencies_xcpd
                )
                print(f"[XCP-D] job IDs: {xcpd_job_id}\n")
                xcpd_job_ids.append(xcpd_job_id)
            else:
                xcpd_job_id = None
            
            # -------------------------------------------
            # 4.b QC on XCP-D 
            # -------------------------------------------

            if workflow["run_mriqc_derivatives"]:
                print("🔹 Submitting MRIQC (XCP-D)")
                dependencies_mriqc_xcpd = [job_id for job_id in [freesurfer_job_id, fmriprep_job_id, xcpd_job_id] if job_id is not None]
                mriqc_xcpd_job_id = run_qc_xcpd(
                    config,
                    subject=subject,
                    session=session,
                    job_ids=dependencies_mriqc_xcpd
                )
                print(f"[MRIQC-XCPD] job IDs: {mriqc_xcpd_job_id}\n")
            else:
                mriqc_xcpd_job_id = None

            # -------------------------------------------
            # 5. MRIQC on derivatives
            # -------------------------------------------
            # if workflow["run_mriqc_derivatives"]:
            #     mriqc_fprep_job_id = run_mriqc(
            #         config,
            #         subject=subject,
            #         session=session,
            #         data_type="fmriprep",
            #         job_ids=fmriprep_job_id
            #     )
            #     print(f"[MRIQC-FMRIPREP] job IDs: {mriqc_fprep_job_id}\n")

            #     mriqc_qsiprep_job_id = run_mriqc(
            #         config,
            #         subject=subject,
            #         session=session,
            #         data_type="qsiprep",
            #         job_ids=qsiprep_job_id
            #     )
            #     print(f"[MRIQC-QSIPREP] job IDs: {mriqc_qsiprep_job_id}\n")

            #     mriqc_qsirecon_job_id = run_mriqc(
            #         config,
            #         subject=subject,
            #         session=session,
            #         data_type="qsirecon",
            #         job_ids=qsirecon_job_id
            #     )
            #     print(f"[MRIQC-QSIRECON] job IDs: {mriqc_qsirecon_job_id}\n")

            #     dependencies = [job_id for job_id in [fmriprep_job_id, xcpd_job_id] if job_id is not None]
            #     mriqc_xcpd_job_id = run_mriqc(
            #         config,
            #         subject=subject,
            #         session=session,
            #         data_type="xcpd",
            #         job_ids=dependencies
            #     )
            #     print(f"[MRIQC-XCPD] job IDs: {mriqc_xcpd_job_id}\n")
            # else:
            #     print("⚠️  MRIQC on derivatives skipped")
        print("\n✅ Workflow submission complete")

    # -------------------------------------------
    # 6. QC FREESURFER
    # -------------------------------------------
#    if workflow["run_freesurfer_qc"] and subjects_sessions:
#        print("🔹 Submitting FreeSurfer QC")
#        dependencies = [job_id for job_id in freesurfer_job_ids if job_id is not None]
#        run_freesurfer_qc(config, subjects_sessions, dependencies)
#    else:
#        print("⚠️  FreeSurfer QC skipped")
    # -------------------------------------------
    # 7. QC QSIPREP
    # -------------------------------------------
#    if workflow["run_qsiprep_qc"] and subjects_sessions:
#        print("🔹 Submitting QSIprep QC")
#        dependencies = [job_id for job_id in qsiprep_job_ids if job_id is not None]
#        qc_qsiprep.run(config, subjects_sessions, dependencies)
#    else:
#        print("⚠️  QSIprep QC skipped")

    # -------------------------------------------------------
    # GROUP-LEVEL MRIQC JOBS
    # -------------------------------------------------------
    if workflow["run_mriqc_group"]:
        print("🔹 Submitting MRIQC group-level jobs")
        # MRIQC group-level for raw data
        jid_mriqc_raw_group = run_mriqc_group(
            config,
            data_type="raw",
            input_dir=BIDS_DIR
        )
        print(f"[MRIQC-RAW-GROUP] job IDs: {jid_mriqc_raw_group}\n")

        # MRIQC group-level for fmriprep data
        jid_mriqc_fmriprep_group = run_mriqc_group(
            config,
            data_type="fmriprep",
            input_dir=f"{DERIVATIVES_DIR}/fmriprep/outputs"
        )
        print(f"[MRIQC-FMRIPREP-GROUP] job IDs: {jid_mriqc_fmriprep_group}\n")

        # MRIQC group-level for qsiprep data
        jid_mriqc_qsiprep_group = run_mriqc_group(
            config,
            data_type="qsiprep",
            input_dir=f"{DERIVATIVES_DIR}/qsiprep/outputs"
        )
        print(f"[MRIQC-QSIPREP-GROUP] job IDs: {jid_mriqc_qsiprep_group}\n")

        # MRIQC group-level for qsirecon data
        jid_mriqc_qsirecon_group = run_mriqc_group(
            config,
            data_type="qsirecon",
            input_dir=f"{DERIVATIVES_DIR}/qsirecon/outputs"
        )
        print(f"[MRIQC-QSIRECON-GROUP] job IDs: {jid_mriqc_qsirecon_group}\n")

        # MRIQC group-level for xcpd data
        jid_mriqc_xcpd_group = run_mriqc_group(
            config,
            data_type="xcpd",
            input_dir=f"{DERIVATIVES_DIR}/xcpd/outputs"
        )
        print(f"[MRIQC-XCPD-GROUP] job IDs: {jid_mriqc_xcpd_group}\n")
    else:
        print("⚠️  Group-level MRIQC jobs skipped")


if __name__ == "__main__":
    main()
