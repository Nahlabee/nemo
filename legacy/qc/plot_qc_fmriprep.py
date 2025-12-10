#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from scipy.stats import zscore

sns.set(style="whitegrid", context="talk")

METRICS = ["tSNR", "SNR", "CNR", "MI_T1w_BOLD"]

def ensure_dir(p):
    p.mkdir(parents=True, exist_ok=True)

def plot_distributions(df, outdir):
    for m in METRICS:
        plt.figure(figsize=(8,5))
        sns.histplot(df[m].dropna(), kde=True)
        plt.title(f"Distribution of {m}")
        plt.tight_layout()
        plt.savefig(outdir / f"{m}_distribution.png")
        plt.close()

def plot_boxplots(df, outdir):
    for m in METRICS:
        plt.figure(figsize=(10,5))
        sns.boxplot(x=df[m])
        plt.title(f"{m} Boxplot")
        plt.tight_layout()
        plt.savefig(outdir / f"{m}_boxplot.png")
        plt.close()

def plot_session_comparison(df, outdir):
    if df["session"].nunique() < 2:
        return

    for m in METRICS:
        plt.figure(figsize=(7,6))
        sns.pointplot(
            data=df,
            x="session",
            y=m,
            hue="subject",
            dodge=True,
            markers="o",
            linestyles="-",
            legend=False,
        )
        plt.title(f"{m} by Session")
        plt.tight_layout()
        plt.savefig(outdir / f"{m}_by_session.png")
        plt.close()

def plot_correlations(df, outdir):
    pairs = [
        ("MI_T1w_BOLD", "tSNR"),
        ("SNR", "gm_voxels"),
    ]

    for x, y in pairs:
        plt.figure(figsize=(7,6))
        sns.scatterplot(data=df, x=x, y=y, hue="session")
        sns.regplot(data=df, x=x, y=y, scatter=False, color="black")
        plt.title(f"{x} vs {y}")
        plt.tight_layout()
        plt.savefig(outdir / f"{x}_vs_{y}.png")
        plt.close()

def flag_outliers(df, z_thresh=3):
    df = df.copy()
    for m in METRICS:
        df[f"{m}_z"] = zscore(df[m], nan_policy="omit")
        df[f"{m}_outlier"] = df[f"{m}_z"].abs() > z_thresh
    return df

def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("metrics_csv", type=Path)
    parser.add_argument("--outdir", default="qc_plots")
    args = parser.parse_args()

    outdir = Path(args.outdir)
    ensure_dir(outdir)

    df = pd.read_csv(args.metrics_csv)

    plot_distributions(df, outdir)
    plot_boxplots(df, outdir)
    plot_correlations(df, outdir)
    plot_session_comparison(df, outdir)

    flagged = flag_outliers(df)
    flagged.to_csv(outdir / "qc_with_outliers.csv", index=False)

    print(f"✅ QC plots written to {outdir}")

if __name__ == "__main__":
    main()
