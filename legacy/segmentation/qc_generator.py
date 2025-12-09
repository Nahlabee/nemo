import os
import nibabel as nib
import numpy as np
import subprocess
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

def screenshot_surface(subject, hemi, out_png, subjects_dir):
    cmd = [
        "xvfb-run", "-a", "freeview",
        "-f", f"{subjects_dir}/{subject}/surf/{hemi}.white:edgecolor=red",
        "-f", f"{subjects_dir}/{subject}/surf/{hemi}.pial:edgecolor=yellow",
        "-viewport", "3d",
        "-ss", out_png
    ]
    subprocess.run(cmd, check=True)

def screenshot_thickness(subject, hemi, out_png, subjects_dir):
    thickness = f"{subjects_dir}/{subject}/surf/{hemi}.thickness"
    inflated = f"{subjects_dir}/{subject}/surf/{hemi}.inflated"

    cmd = [
        "xvfb-run", "-a", "freeview",
        "-f", f"{inflated}:overlay={thickness}:overlay_threshold=0.1,3",
        "-viewport", "3d",
        "-ss", out_png
    ]
    subprocess.run(cmd, check=True)

def screenshot_t1_mosaic(subject, subjects_dir, out_png):
    t1 = f"{subjects_dir}/{subject}/mri/T1.mgz"
    img = nib.load(t1).get_fdata()
    mids = np.linspace(0, img.shape[2]-1, 16).astype(int)
    
    fig, axs = plt.subplots(4, 4, figsize=(8, 8))
    for ax, z in zip(axs.flat, mids):
        ax.imshow(img[:,:,z].T, cmap='gray', origin='lower')
        ax.set_xticks([]); ax.set_yticks([])
    plt.tight_layout()
    fig.savefig(out_png)
    plt.close(fig)

def make_pdf(subject, subjects_dir, output_pdf):
    c = canvas.Canvas(output_pdf, pagesize=A4)
    w, h = A4

    t1_png = f"/tmp/{subject}_t1.png"
    lh_surf = f"/tmp/{subject}_lh_surf.png"
    rh_surf = f"/tmp/{subject}_rh_surf.png"
    lh_thick = f"/tmp/{subject}_lh_thick.png"
    rh_thick = f"/tmp/{subject}_rh_thick.png"

    screenshot_t1_mosaic(subject, subjects_dir, t1_png)
    screenshot_surface(subject, "lh", lh_surf, subjects_dir)
    screenshot_surface(subject, "rh", rh_surf, subjects_dir)
    screenshot_thickness(subject, "lh", lh_thick, subjects_dir)
    screenshot_thickness(subject, "rh", rh_thick, subjects_dir)

    c.setFont("Helvetica-Bold", 20)
    c.drawString(50, h-50, f"FreeSurfer QC — {subject}")

    img_y = h-200
    for img in [t1_png, lh_surf, rh_surf, lh_thick, rh_thick]:
        c.drawImage(img, 50, img_y, width=w-100, height=150)
        img_y -= 170
        if img_y < 50:
            c.showPage()
            img_y = h-200

    c.save()
    print(f"✅ QC PDF saved to: {output_pdf}")

def generate_qc_pdf(subject, subjects_dir, qc_dir):
    os.makedirs(qc_dir, exist_ok=True)
    output_pdf = f"{qc_dir}/{subject}_qc.pdf"
    make_pdf(subject, subjects_dir, output_pdf)
