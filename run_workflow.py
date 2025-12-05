import json
import os
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
import sys

sys.path.append(str(Path(__file__).resolve().parent))
import utils
from anat.run_freesurfer import run_freesurfer
from dwi.run_qsiprep import run_qsiprep
from dwi.run_qsirecon import run_qsirecon


def main(config_file=None):
    """
    Main function to execute the workflow steps based on the configuration file.
    """
    # Load configuration
    if not config_file:
        config_file = f"{Path(__file__).parent}/config/config.json"
    config = utils.load_config(config_file)
    args = SimpleNamespace(**config.get('common', {}))

    # Save config with datetime
    filename = f"{args.derivatives}/config_{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    with open(filename, "w") as f:
        json.dump(config, f, indent=4)

    # Check dataset directory
    if not os.path.exists(args.input_dir):
        print("Dataset directory does not exist.")
        return 0

    subjects_sessions = []
    freesurfer_job_ids = []
    # Loop over subjects and sessions
    subjects = utils.get_subjects(args.input_dir, args.subjects)
    for subject in subjects:
        sessions = utils.get_sessions(args.input_dir, subject, args.sessions)
        for session in sessions:

            print('\n', subject, ' - ', session, '\n')
            subjects_sessions.append(f"{subject}_{session}")

            # Run workflow steps based on configuration
            if args.run_freesurfer:
                step_config = config.get('freesurfer', {})
                for key, value in step_config.items():
                    setattr(args, key, value)
                freesurfer_job_id = run_freesurfer(args, subject, session)
                freesurfer_job_ids.append(freesurfer_job_id)
                print(f"[FREESURFER] job IDs: {freesurfer_job_id}\n")
            else:
                freesurfer_job_id = None

            if args.run_qsiprep:
                step_config = config.get('qsiprep', {})
                for key, value in step_config.items():
                    setattr(args, key, value)
                qsiprep_job_id = run_qsiprep(args, subject, session)
                print(f"[QSIPREP] job IDs: {qsiprep_job_id}\n")
            else:
                qsiprep_job_id = None

            if args.run_qsirecon:
                step_config = config.get('qsirecon', {})
                for key, value in step_config.items():
                    setattr(args, key, value)
                dependencies = [job_id for job_id in [freesurfer_job_id, qsiprep_job_id] if job_id is not None]
                qsirecon_job_id = run_qsirecon(args, subject, session, dependencies)
                print(f"[QSIRECON] job IDs: {qsirecon_job_id}\n")
            else:
                qsirecon_job_id = None

            # print("Workflow submitted.")

    if args.run_freesurfer_qc and subjects_sessions:
        # Run FreeSurfer QC
        # Note that FSQC must run on interactive mode to be able to display (and save) graphical outputs
        step_config = config.get('fsqc', {})
        for key, value in step_config.items():
            setattr(args, key, value)
        path_to_script = f"{args.derivatives}/qc/fsqc/scripts/fsqc.sh"
        cmd = (f'nohup python3 anat/qc_freesurfer.py '
               f'"{json.dumps(vars(args))}" "{",".join(subjects_sessions)}" '
               f'> {args.derivatives}/qc/fsqc/stdout/fsqc.out 2>&1 &')
        print(f"[FSQC] Submitting task in background: {cmd}")
        os.system(cmd)
        # fsqc_job_id = qc_freesurfer(args, sub_ses, freesurfer_job_ids)



if __name__ == "__main__":
    main()
