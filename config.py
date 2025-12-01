"""
config.py

Usage:
  - In Python: `import config; print(config.BIDS_DIR)`
  - In bash/SLURM before running containerized commands:
      eval "$(python3 -c 'import sys; sys.path.insert(0,"/project"); import config; config.print_paths()')"
    (replace /project with the path you bind into the container)
"""

from pathlib import Path
import json
import os

# Find config.json relative to this file (project root)
_THIS_DIR = Path(__file__).resolve().parent
_CONFIG_FILE = _THIS_DIR / "config.json"

# Defaults (safe fallback)
_defaults = {
"BIDS_DIR": "/scratch/hrasoanandrianina/braint_database",
"WORK_DIR": "/home/hrasoanandrianina/work_dir_bis",
"FS_LICENSE_FILE": "/scratch/hrasoanandrianina/containers/license.txt",

"MRIQC_SIF": "/scratch/hrasoanandrianina/containers/mriqc_24.0.2.sif",
"OUT_MRIQC_DIR": "/scratch/hrasoanandrianina/derivatives/mriqc_24.0.2",

"FMRIPREP_SIF": "/scratch/hrasoanandrianina/containers/fmriprep_25.2.0.sif",
"OUT_FMRIPREP_DIR": "/scratch/hrasoanandrianina/derivatives/fmriprep_25.2.0",

"XCP_D_SIF": "/scratch/hrasoanandrianina/containers/xcp_d_0.12.0.sif",
"OUT_XCP_D_DIR": "/scratch/hrasoanandrianina/derivatives/xcp_d_0.12.0",

"SLURM_DIR": "./slurm_jobs",
"SLURM_PARTITION": "skylake",
"SLURM_CPUS": "16",
"SLURM_MEM": "64G",
"SLURM_TIME": "12:00:00"
}

# load json if present
_config = {}
if _CONFIG_FILE.exists():
    try:
        with _CONFIG_FILE.open("r") as f:
            _config = json.load(f)
    except Exception as e:
        raise RuntimeError(f"Failed to parse {_CONFIG_FILE}: {e}")

# Merge defaults with config.json, env overrides win
for k, v in _defaults.items():
    val = _config.get(k, v)
    # allow environment to override
    val = os.environ.get(k, str(val))
    # cast numeric fields back to int if defaults are int
    if isinstance(_defaults[k], int):
        try:
            val = int(val)
        except Exception:
            val = _defaults[k]
    globals()[k] = val

def print_paths():
    """
    Print environment assignment lines for shell `eval`:

    Example usage from shell (project dir must be importable):
      eval $(python3 -c 'import sys; sys.path.insert(0,"/project"); import config; config.print_paths()')
    """
    for name, val in list(globals().items()):
        if name.isupper():
            # escape double quotes
            s = str(val).replace('"', '\\"')
            print(f'export {name}="{s}"')

def write_env_file(path):
    """
    Write a shell file that exports all config variables.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as f:
        for name, val in list(globals().items()):
            if name.isupper():
                s = str(val).replace('"', '\\"')
                f.write(f'export {name}="{s}"\n')
    return str(p)
