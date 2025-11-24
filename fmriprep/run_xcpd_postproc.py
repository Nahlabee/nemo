#!/usr/bin/env python3
"""
Run XCP-D post-processing and compare outputs across fMRIPrep versions.
Author: Henitsoa RASOANANDRIANINA
Date: 2025-10-22

Usage:
    python run_xcpd_postproc.py
"""

import argparse
import subprocess
from pathlib import Path
import pandas as pd
import json
from rich.console import Console
from jinja2 import Template
import shutil

console = Console()

# ========================
# CONFIGURATION
# ========================

THREADS = 8
OUTPUT_SPACES = ["MNI152NLin2009cAsym", "fsLR"]
SMOOTHING_FWHM = 6


# ========================
# CORE FUNCTIONALITY
# ========================

def run_xcpd(work_dir: Path, fmriprep_dir: Path, output_dir: Path, xcpd_sif: Path, subject: str):
    """Run XCP-D via Singularity."""
    console.rule(f"[bold blue]Running XCP-D for {subject} ({xcpd_sif.name})[/]")
    cmd = [
    "apptainer", "run", "--cleanenv",
    "-B", f"{str(fmriprep_dir)}:/data:ro",
    "-B", f"{str(output_dir)}:/out",
    "-B", f"{str(work_dir)}:/work",
    str(xcpd_sif),
    "/data", "/out", "participant",
    "--mode", "abcd",
    "--motion-filter-type", "none",
    "--input-type", "fmriprep",
    "--participant-label", subject,
    "--bids-filter-file", "/home/henit/fmriprep_data/bids_dir/bids_filter.json",
    "--nuisance-regressors", "36P",
    "--smoothing", "4",
    "--session-id", "ses-01",
    "--task-id", "rest",                # Correct for --task-id
    "--despike",
    "--dummy-scans", "auto",
    "--linc-qc",
    "--abcc-qc",

    "--nthreads", str(THREADS),
    "--omp-nthreads", str(THREADS),

    "-w", "/work",                      # must be writable!
    "--stop-on-first-crash"
]
    console.print(f"[cyan]Executing:[/]\n{' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    console.print(f"[green]✔ Completed XCP-D for {subject} using {xcpd_sif.name}[/]")


def compare_confounds(conf1: Path, conf2: Path, out_path: Path):
    """Compare XCP-D confound files."""
    df1 = pd.read_csv(conf1, sep="\t")
    df2 = pd.read_csv(conf2, sep="\t")
    shared_cols = sorted(set(df1.columns).intersection(df2.columns))
    diffs = (df2[shared_cols].mean() - df1[shared_cols].mean()).to_frame("mean_diff")
    diffs.to_csv(out_path, sep="\t")
    return diffs


def compare_qc_metrics(qc1: Path, qc2: Path, out_path: Path):
    """Compare XCP-D QC metrics JSON files."""
    with open(qc1) as f1, open(qc2) as f2:
        qc23, qc25 = json.load(f1), json.load(f2)
    keys = sorted(set(qc23.keys()) & set(qc25.keys()))
    diffs = {k: qc25[k] - qc23[k] if isinstance(qc25[k], (int, float)) else None for k in keys}
    with open(out_path, "w") as f:
        json.dump(diffs, f, indent=2)
    return diffs


def generate_html_report(conf_diffs: pd.DataFrame, qc_diffs: dict, out_html: Path):
    """Generate a compact HTML summary report."""
    template = Template("""
    <html>
    <head>
        <title>XCP-D Migration Report</title>
        <style>
            body { font-family: sans-serif; margin: 40px; }
            table { border-collapse: collapse; width: 70%; margin-top: 20px; }
            th, td { border: 1px solid #ccc; padding: 6px 10px; text-align: left; }
            th { background: #eee; }
            h2 { color: #2c5282; }
        </style>
    </head>
    <body>
        <h1>XCP-D Migration QA Report</h1>
        <h2>Confound Differences (Mean)</h2>
        {{ conf_html | safe }}

        <h2>QC Metric Differences</h2>
        <table>
        {% for key, val in qc_diffs.items() %}
          <tr><td>{{ key }}</td><td>{{ val }}</td></tr>
        {% endfor %}
        </table>
    </body>
    </html>
    """)
    conf_html = conf_diffs.to_html(float_format="%.4f") # type: ignore
    html = template.render(conf_html=conf_html, qc_diffs=qc_diffs)
    with open(out_html, "w") as f:
        f.write(html)


def main():
    # ========================
    # PATH CONFIGURATION
    # ========================
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--work_dir", required=True)
    parser.add_argument("--subject", required=True)
    parser.add_argument("--fs_license", required=True)
    parser.add_argument("--sif_dir", required=True)
    args = parser.parse_args()
    
    out_dir = Path(args.output_dir)
    work_dir = Path(args.work_dir)
    subject = args.subject
    fs_license = Path(args.fs_license)
    sif_dir = Path(args.sif_dir)

    versions = {
        # "23": {
        #     "fmriprep": out_dir / "fmriprep23",
        #     "xcpd": out_dir / "xcpd23",
        #     "sif": sif_dir / "xcp_d_0.12.0.sif"},
        "25": {
            "fmriprep": out_dir / "fmriprep25.2.0",
            "xcpd": out_dir / "xcpd25.2.0",
            "sif": sif_dir / "xcp_d_0.12.0.sif",
        },
    }

    # ========================
    # RUN XCP-D FOR BOTH VERSIONS
    # ========================
    for v, cfg in versions.items():
        cfg["xcpd"].mkdir(parents=True, exist_ok=True)
        run_xcpd(
            work_dir=work_dir,
            fmriprep_dir=cfg["fmriprep"],
            output_dir=cfg["xcpd"],
            xcpd_sif=cfg["sif"],
            subject=subject,
        )

    # # ========================
    # # COMPARE RESULTS
    # # ========================
    # diff_dir = out_dir / "diff_logs" / "xcpd"
    # diff_dir.mkdir(parents=True, exist_ok=True)

    # conf23 = versions["23"]["xcpd"] / subject / "func" / f"{subject}_task-rest_desc-confounds_timeseries.tsv"
    # conf25 = versions["25"]["xcpd"] / subject / "func" / f"{subject}_task-rest_desc-confounds_timeseries.tsv"
    # qc23 = versions["23"]["xcpd"] / subject / "func" / f"{subject}_desc-qc_metrics.json"
    # qc25 = versions["25"]["xcpd"] / subject / "func" / f"{subject}_desc-qc_metrics.json"

    # console.rule("[bold magenta]Comparing XCP-D outputs[/]")
    # conf_diffs = compare_confounds(conf23, conf25, diff_dir / "confounds_diff.tsv")
    # qc_diffs = compare_qc_metrics(qc23, qc25, diff_dir / "qc_metrics_diff.json")

    # # ========================
    # # GENERATE REPORT
    # # ========================
    # html_out = diff_dir / "xcpd_migration_report.html"
    # generate_html_report(conf_diffs, qc_diffs, html_out)
    # console.print(f"[bold green]✅ XCP-D migration report saved at:[/] {html_out}")


if __name__ == "__main__":
    main()
