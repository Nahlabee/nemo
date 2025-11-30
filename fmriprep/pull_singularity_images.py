#!/usr/bin/env python3
"""
Pull fMRIPrep and XCP-D Apptainer/Singularity images for versions 23.2 and 25.2.

This script:
  • Detects whether 'apptainer' or 'singularity' is available
  • Pulls images from DockerHub using the detected tool
  • Stores them in /project_root/containers/
  • Skips downloads if images already exist
  • Optionally inspects images after pull

Author: HR
Date: 2025-10-24
"""

import subprocess
import shutil
from pathlib import Path
import sys

# ========================
# CONFIGURATION
# ========================

PROJECT_ROOT = Path("/scratch/hrasoanandrianina/code/nemo/fmriprep")  # absolute path to your project root
CONTAINER_DIR = Path("/scratch/hrasoanandrianina/containers")  

IMAGES = {
    "fmriprep": ["23.2.0", "25.2.0"],
    "xcp_d": ["0.12.0"],
    "freesurfer": ["7.4.1"]
}

DOCKER_REPOS = {
    "fmriprep": "nipreps/fmriprep",
    "xcp_d": "pennlinc/xcp_d",
    "freesurfer": "freesurfer/freesurfer",
}


# ========================
# HELPER FUNCTIONS
# ========================

def detect_container_tool() -> str:
    """Detect whether Apptainer or Singularity is installed."""
    if shutil.which("apptainer"):
        print("[INFO] Using Apptainer as container runtime.")
        return "apptainer"
    elif shutil.which("singularity"):
        print("[INFO] Using Singularity as container runtime.")
        return "singularity"
    else:
        sys.exit(
            "[ERROR] Neither Apptainer nor Singularity found.\n"
            "Please install Apptainer in WSL2 with:\n"
            "  sudo add-apt-repository -y ppa:apptainer/ppa && sudo apt update && sudo apt install -y apptainer"
        )


def run_command(cmd: list[str]):
    """Run a shell command and stream output."""
    print(f"\n[RUNNING] {' '.join(cmd)}")
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in process.stdout: # type: ignore
        print(f"[{cmd[0]}] {line.strip()}")
    process.wait()
    if process.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")


def pull_image(tool: str, name: str, version: str, repo: str, out_dir: Path):
    """Pull the image using Apptainer or Singularity."""
    sif_name = f"{name}_{version}.sif"
    sif_path = out_dir / sif_name
    docker_uri = f"docker://{repo}:{version}"

    if sif_path.exists():
        print(f"[SKIP] {sif_name} already exists at {sif_path}")
        return sif_path

    print(f"[INFO] Pulling {sif_name} from {docker_uri}")
    cmd = [tool, "pull", str(sif_path), docker_uri]
    run_command(cmd)

    # Inspect metadata (optional)
    print(f"\n[INFO] Inspecting {sif_name}:")
    inspect_cmd = [tool, "inspect", str(sif_path)]
    subprocess.run(inspect_cmd, check=True)
    return sif_path


def main():
    tool = detect_container_tool()
    CONTAINER_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[SETUP] Containers directory: {CONTAINER_DIR.resolve()}")

    for software, versions in IMAGES.items():
        repo = DOCKER_REPOS[software]
        for version in versions:
            pull_image(tool, software, version, repo, CONTAINER_DIR)

    print("\n✅ All requested container images are present and verified.")
    print(f"📁 Location: {CONTAINER_DIR.resolve()}")


if __name__ == "__main__":
    main()
