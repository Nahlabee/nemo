#!/usr/bin/env python3

from pathlib import Path

def get_subjects(bids_dir):
    """Find subjects (folders starting with 'sub-')."""
    subs = sorted([p.name for p in Path(bids_dir).glob("sub-*") if p.is_dir()])
    if not subs:
        raise RuntimeError("No subjects found in BIDS directory.")
    return subs

def get_sessions(bids_dir, subject):
    """Find sessions for a given subject (folders starting with 'ses-')."""
    subj_path = Path(bids_dir) / subject
    sessions = sorted([p.name for p in subj_path.glob("ses-*") if p.is_dir()])
    if not sessions:
        return [None] # No sessions found
    return [s for s in sessions]
