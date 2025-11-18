#!/usr/bin/env python3
"""
Compare fMRIPrep outputs across FreeSurfer versions (e.g. 7.3.2 vs 7.4.1),
and produce visual QC plots (mask diffs, segmentation diffs, thickness scatterplots).

Author: Henitsoa R. (2025)

Usage example:
python compare_fmriprep_fs_versions_with_qc.py \
  --dirA /home/henit/fmriprep_data/derivatives/fmriprep_fs7.3.2 \
  --dirB /home/henit/fmriprep_data/derivatives/fmriprep_fs7.4.1 \
  --out /home/henit/fmriprep_data/fs_version_comparison.csv \
  --qc /home/henit/fmriprep_data/qc_images \
  --pdf /home/henit/fmriprep_data/fs_version_qc.pdf
  
"""

import argparse
import logging
import os
from pathlib import Path
import numpy as np
import nibabel as nib
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from scipy.stats import pearsonr

# Configure logging
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("xcpd_postproc")

# ---------- Utility Functions ----------

def ensure_dir(p):
    Path(p).mkdir(parents=True, exist_ok=True)

def find_subjects(base_dir):
    return [p.name for p in Path(base_dir).glob("sub-*") if p.is_dir()]

def load_data(path):
    img = nib.load(str(path)) # type: ignore
    return img.get_fdata(), img.affine # type: ignore

def summarize_data(data: np.ndarray, path: Path):
    """Compute and log basic statistics."""
    logger.info(f"Summary for {path.name}: shape={data.shape}")
    logger.info(f"Mean={np.mean(data):.4f}, Std={np.std(data):.4f}, Min={np.min(data):.4f}, Max={np.max(data):.4f}")


def dice(a, b):
    a = a.astype(bool)
    b = b.astype(bool)
    inter = np.logical_and(a, b).sum()
    s = a.sum() + b.sum()
    return (2 * inter / s) if s > 0 else np.nan

def diff_stats(a, b):
    diff_vox = np.sum(a != b)
    union = np.logical_or(a, b).sum()
    return diff_vox, 100 * diff_vox / union if union > 0 else np.nan

def find_file(subject_dir, pattern):
    files = list(Path(subject_dir).rglob(pattern))
    return files[0] if files else None

def find_gifti(subject_dir, desc):
    files = list(Path(subject_dir).rglob(f"*desc-{desc}*.gii"))
    return files[0] if files else None

def compare_masks(fileA, fileB):
    dataA, _ = load_data(fileA)
    dataB, _ = load_data(fileB)
    dataA = dataA > 0
    dataB = dataB > 0
    d = dice(dataA, dataB)
    diff_vox, diff_pct = diff_stats(dataA, dataB)
    return d, diff_vox, diff_pct, dataA.sum(), dataB.sum(), dataA, dataB

def load_gifti_thickness(path):
    """Load GIFTI thickness values as a numpy array."""
    gi = nib.load(path) # type: ignore
    summarize_data(np.column_stack([d.data for d in gi.darrays]), Path(path))
    return np.column_stack([d.data for d in gi.darrays])


def load_cortex_mask(path):
    """Load cortex mask (0/1). Returns boolean array."""
    gi = nib.load(path) # type: ignore
    summarize_data(np.column_stack([d.data for d in gi.darrays]), Path(path))
    data = np.column_stack([d.data for d in gi.darrays])

    return data.astype(bool)


def compute_metrics(a, b):
    """Compute correlation and error metrics."""
    corr = pearsonr(a, b)[0]
    mad = np.mean(np.abs(a - b))
    rms = np.sqrt(np.mean((a - b) ** 2))
    return corr, mad, rms


# ---------- Visualization Helpers ----------

def plot_brainmask_diff(sub, dataA, dataB, out_dir, pdf=None):
    diff = np.zeros_like(dataA)
    diff[(dataA == 1) & (dataB == 0)] = 1  # only in A
    diff[(dataA == 0) & (dataB == 1)] = 2  # only in B
    mid = dataA.shape[2] // 2
    fig, ax = plt.subplots(1, 3, figsize=(12, 4))
    ax[0].imshow(dataA[:, :, mid].T, cmap="gray", origin="lower")
    ax[0].set_title("FS 7.4.1 fmriprep 25.2 goodvoxels mask")
    ax[1].imshow(dataB[:, :, mid].T, cmap="gray", origin="lower")
    ax[1].set_title("FS 7.4.1 fmriprep 25.2 mask")
    ax[2].imshow(diff[:, :, mid].T, cmap="bwr", origin="lower")
    ax[2].set_title("Difference map (red=A only, blue=B only)")
    for a in ax: a.axis("off")
    plt.suptitle(f"{sub} – Brain Mask Comparison")
    save_path = Path(out_dir) / f"{sub}_mask_diff.png"
    plt.tight_layout(); plt.savefig(save_path, dpi=150)
    if pdf: pdf.savefig(fig)
    plt.close(fig)
    return save_path

def plot_dseg_diff(sub, dataA, dataB, out_dir, pdf=None):
    mid = dataA.shape[2] // 2
    diff = dataA != dataB
    fig, ax = plt.subplots(1, 3, figsize=(12, 4))
    ax[0].imshow(dataA[:, :, mid].T, cmap="nipy_spectral", origin="lower")
    ax[0].set_title("Dseg 7.4.1 fmriprep 25.2 goodvoxels")
    ax[1].imshow(dataB[:, :, mid].T, cmap="nipy_spectral", origin="lower")
    ax[1].set_title("Dseg 7.4.1 fmriprep 25.2")
    ax[2].imshow(diff[:, :, mid].T, cmap="gray", origin="lower")
    ax[2].set_title("Difference mask")
    for a in ax: a.axis("off")
    plt.suptitle(f"{sub} – Dseg Comparison")
    save_path = Path(out_dir) / f"{sub}_dseg_diff.png"
    plt.tight_layout(); plt.savefig(save_path, dpi=150)
    if pdf: pdf.savefig(fig)
    plt.close(fig)
    return save_path

def plot_thickness_scatter(sub, hemi, a, b, out_dir, pdf=None):
    fig, ax = plt.subplots(1, 2, figsize=(10, 4))
    mask = (~np.isnan(a)) & (~np.isnan(b))
    ax[0].scatter(a[mask], b[mask], s=3, alpha=0.4)
    ax[0].set_xlabel("FS 7.4.1 fmriprep 25.2 goodvoxels"); ax[0].set_ylabel("FS 7.4.1 fmriprep 25.2")
    ax[0].set_title(f"{sub} {hemi}-hemi thickness scatter")
    ax[1].hist(a[mask]-b[mask], bins=50, color="gray")
    ax[1].set_title("Thickness diff histogram (A - B)")
    plt.tight_layout()
    save_path = Path(out_dir) / f"{sub}_{hemi}_thickness_scatter.png"
    plt.savefig(save_path, dpi=150)
    if pdf: pdf.savefig(fig)
    plt.close(fig)
    return save_path

def compare_surfaces(dir1, dir2, subject, out_dir):
    print(f"Comparing surfaces for subject {subject}...")
    ensure_dir(out_dir)
    results = []

    for hemi in ["L", "R"]:

        print(f"Processing hemisphere: {hemi}")

        # File paths
        f732 = os.path.join(dir1, f"{subject}_hemi-{hemi}_thickness.shape.gii")
        f741 = os.path.join(dir2, f"{subject}_hemi-{hemi}_thickness.shape.gii")
        mask_path = os.path.join(dir2, f"{subject}_hemi-{hemi}_desc-cortex_mask.label.gii")

        # Load data
        t732 = load_gifti_thickness(f732)
        t741 = load_gifti_thickness(f741)
        mask = load_cortex_mask(mask_path)

        # Apply cortex mask (remove medial wall)
        t732 = t732[mask]
        t741 = t741[mask]

        # Compute metrics
        corr, mad, rms = compute_metrics(t732, t741)

        results.append({
            "hemisphere": hemi,
            "pearson_r": corr,
            "mean_abs_difference": mad,
            "rms_difference": rms
        })

        # -----------------------------
        # Plotting
        # -----------------------------
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))

        # Histogram of differences
        diff = t741 - t732
        axes[0].hist(diff, bins=50)
        axes[0].set_title(f"Hemisphere {hemi}: Thickness Difference (FS7.4.1 - FS7.3.2)")
        axes[0].set_xlabel("Difference (mm)")
        axes[0].set_ylabel("Vertex count")

        # Scatter plot
        axes[1].scatter(t732, t741, s=2, alpha=0.4)
        axes[1].set_title(f"Hemisphere {hemi}: Vertex-wise correlation")
        axes[1].set_xlabel("FS 7.3.2 thickness")
        axes[1].set_ylabel("FS 7.4.1 thickness")

        # Save figure
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, f"{subject}_hemi-{hemi}_comparison.png"))
        plt.close()

    # -----------------------------
    # Save summary table
    # -----------------------------
    df = pd.DataFrame(results)
    df.to_csv(os.path.join(out_dir, f"{subject}_thickness_comparison_summary.csv"), index=False)

    print("\nComparison complete!")
    print(df)
    print(f"\nResults saved to: {out_dir}")
# ---------- Main Comparison ----------

def compare_subject(sub, dirA, dirB, qc_dir, pdf=None):
    out = {"subject": sub}
    subA = Path(dirA) / sub / "anat"
    subB = Path(dirB) / sub / "anat"

    ensure_dir(qc_dir)

    # --- Brain mask
    maskA = find_file(subA, "*_desc-brain_mask.nii.gz")
    maskB = find_file(subB, "*_desc-brain_mask.nii.gz")
    if maskA and maskB:
        d, diff_vox, diff_pct, nA, nB, dataA, dataB = compare_masks(maskA, maskB)
        out.update({
            "brainmask_dice": d, "brainmask_diff_vox": diff_vox,
            "brainmask_diff_pct": diff_pct, "brainmask_voxA": nA, "brainmask_voxB": nB
        })
        plot_brainmask_diff(sub, dataA, dataB, qc_dir, pdf)
    else:
        out["brainmask_dice"] = np.nan

    # --- Dseg
    dsegA = find_file(subA, "*_dseg.nii.gz")
    dsegB = find_file(subB, "*_dseg.nii.gz")
    if dsegA and dsegB:
        dataA, _ = load_data(dsegA)
        dataB, _ = load_data(dsegB)
        same_vox = np.sum(dataA == dataB)
        total_vox = np.prod(dataA.shape)
        pct_equal = 100 * same_vox / total_vox
        out["dseg_equal_pct"] = pct_equal
        plot_dseg_diff(sub, dataA, dataB, qc_dir, pdf)
    else:
        out["dseg_equal_pct"] = np.nan

    # --- Surface thickness
    compare_surfaces(Path(dirA) / sub / "anat", Path(dirB) / sub / "anat", sub, qc_dir)
    print(f"Finished surfaces comparison for {sub}")

    return out

# ---------- CLI ----------

def main():
    parser = argparse.ArgumentParser(description="Compare fMRIPrep outputs and generate QC plots.")
    parser.add_argument("--dirA", required=True, help="Path to fMRIPrep outputs (FS 7.3.2)")
    parser.add_argument("--dirB", required=True, help="Path to fMRIPrep outputs (FS 7.4.1)")
    parser.add_argument("--out", default="fmriprep_fs_compare.csv", help="Output CSV file")
    parser.add_argument("--qc", default="qc_images", help="Directory to save QC images")
    parser.add_argument("--pdf", default=None, help="Optional PDF QC summary")
    args = parser.parse_args()

    subsA = find_subjects(args.dirA)
    subsB = find_subjects(args.dirB)
    common = sorted(set(subsA) & set(subsB))
    if not common:
        print("No common subjects found!")
        return

    ensure_dir(args.qc)
    results = []
    pdf = PdfPages(args.pdf) if args.pdf else None

    for sub in common:
        print(f"Comparing {sub}...")
        res = compare_subject(sub, args.dirA, args.dirB, args.qc, pdf)
        results.append(res)

    if pdf:
        pdf.close()
        print(f"QC PDF saved: {args.pdf}")

    df = pd.DataFrame(results)
    df.to_csv(args.out, index=False)
    print(f"Summary CSV saved: {args.out}")
    print(f"QC images in: {args.qc}")

if __name__ == "__main__":
    main()
