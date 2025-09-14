# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
====================================================================================================
Script: code_to_migrate_fgdb.py
Author: Nathanael Sheean
Date: 2025-09-14
Version: 2.1 (Safe-abort, explicit user guidance)

Purpose:
  Safely copy a File Geodatabase (.gdb) to a new location without accidental overwrite.

Operator instructions:
  1) Replace the {place.holders} with real Windows paths (use raw strings r"..."):
     - SOURCE_GDB: full path to the source .gdb folder.
     - DEST_PARENT: destination parent folder that already exists.
     - DEST_GDB_NAME: new .gdb folder name (must end with ".gdb").
  2) Run. If the script ABORTS, read the printed instructions, fix the issue, and re-run.

Examples:
  SOURCE_GDB   = r"C:\Data\Site\Site.gdb"
  DEST_PARENT  = r"D:\Backups\Q3"
  DEST_GDB_NAME= r"Site_2025Q3.gdb"

Safety rules:
  • ABORT if *.lock files are present in source (lists each lock and how to resolve).
  • ABORT if destination exists (no overwrite by default).
  • ABORT if source and destination paths are identical.
  • No silent failure: every abort prints precise corrective actions.
====================================================================================================
"""

import os, shutil, time, sys

# --------------------- REQUIRED PLACEHOLDERS (edit these) ---------------------
SOURCE_GDB     = r"{FULL_PATH_TO_SOURCE_GDB}"           # e.g., r"C:\Data\Site\Site.gdb"
DEST_PARENT    = r"{FULL_PATH_TO_DEST_PARENT_FOLDER}"   # e.g., r"D:\Backups\Q3"
DEST_GDB_NAME  = r"{NEW_GDB_NAME}.gdb"                  # e.g., r"Site_2025Q3.gdb"
# ------------------------------------------------------------------------------

# --------------------- OPTIONS (keep defaults for maximum safety) ------------
# Set to True ONLY if you intend to replace an existing destination after you confirm a backup.
OVERWRITE      = False
# Do NOT bypass lock checks unless you fully control all editors/services on the source.
IGNORE_LOCKS   = False
# ------------------------------------------------------------------------------

# Messaging compatible with and without arcpy
try:
    import arcpy
    def _msg(s): arcpy.AddMessage(s)
    def _warn(s): arcpy.AddWarning(s)
    def _err(s): arcpy.AddError(s)
except Exception:
    def _msg(s): print(s)
    def _warn(s): print("WARNING:", s)
    def _err(s): print("ERROR:", s)

def _ts():
    return time.strftime("%Y%m%d_%H%M%S")

def abort_with_instructions(title: str, instructions: list, exit_code=2):
    _err("=== ABORTED === " + title)
    for step in instructions:
        _err("• " + step)
    _err("===============")
    sys.exit(exit_code)

def validate_inputs(src, dst_parent, dst_name):
    issues = []
    if not src or not os.path.isdir(src) or not src.lower().endswith(".gdb"):
        issues.append("SOURCE_GDB must be an existing folder ending with .gdb.")
    if not dst_parent or not os.path.isdir(dst_parent):
        issues.append("DEST_PARENT must be an existing folder.")
    if not dst_name or not dst_name.lower().endswith(".gdb"):
        issues.append('DEST_GDB_NAME must end with ".gdb".')
    return issues

def find_locks(src):
    locks = []
    for root, dirs, files in os.walk(src):
        for f in files:
            if f.lower().endswith(".lock"):
                locks.append(os.path.join(root, f))
    return locks

def ensure_destination(dst_parent, dst_name, src_path):
    dst = os.path.abspath(os.path.join(dst_parent, dst_name))
    src_abs = os.path.abspath(src_path)

    # Identical path guard
    if os.path.normcase(dst) == os.path.normcase(src_abs):
        abort_with_instructions(
            "Source and destination paths are identical.",
            [
                f"Change DEST_PARENT or DEST_GDB_NAME so it differs from SOURCE_GDB.",
                f"Current SOURCE_GDB: {src_abs}",
                f"Current DEST path:  {dst}"
            ],
            exit_code=3
        )

    # Destination exists guard
    if os.path.exists(dst):
        if not OVERWRITE:
            abort_with_instructions(
                "Destination already exists; copy would overwrite.",
                [
                    f"Existing destination: {dst}",
                    "Choose ONE of the following and re-run:",
                    "  - Change DEST_GDB_NAME to a new name; or",
                    "  - Move/rename the existing destination; or",
                    "  - Set OVERWRITE=True *only if* you intend to replace it (make a backup first)."
                ],
                exit_code=4
            )
        else:
            # With OVERWRITE=True, rename existing destination to a timestamped backup.
            backup = dst.rstrip("\\/") + f".bak_{_ts()}"
            os.rename(dst, backup)
            _warn(f"Existing destination renamed to: {backup}")
    return dst

def copy_fgdb(src, dst):
    # Ignore any lingering lock files on copy
    ignore_fn = shutil.ignore_patterns("*.lock")
    shutil.copytree(src, dst, ignore=ignore_fn)

def summarize(dst):
    file_count = 0
    byte_count = 0
    for root, dirs, files in os.walk(dst):
        file_count += len(files)
        for f in files:
            try:
                byte_count += os.path.getsize(os.path.join(root, f))
            except Exception:
                pass
    _msg(f"Summary: files={file_count}, size_bytes={byte_count}")

def main():
    _msg("=== FGDB Migration Start ===")
    _msg(f"SOURCE_GDB   = {SOURCE_GDB}")
    _msg(f"DEST_PARENT  = {DEST_PARENT}")
    _msg(f"DEST_GDB_NAME= {DEST_GDB_NAME}")
    _msg(f"OVERWRITE    = {OVERWRITE}")
    _msg(f"IGNORE_LOCKS = {IGNORE_LOCKS}")

    # Basic input validation
    issues = validate_inputs(SOURCE_GDB, DEST_PARENT, DEST_GDB_NAME)
    if issues:
        abort_with_instructions("Invalid input configuration.", issues, exit_code=1)

    # Lock check
    locks = find_locks(SOURCE_GDB)
    if locks and not IGNORE_LOCKS:
        abort_with_instructions(
            "Lock files detected in source geodatabase.",
            [
                f"{len(locks)} lock file(s) found under: {SOURCE_GDB}",
                *[f"  LOCK: {p}" for p in locks],
                "Close ArcGIS Pro sessions or services that reference the source.",
                "Wait a few seconds for locks to release and verify *.lock files disappear.",
                "Re-run this script after locks are cleared."
            ],
            exit_code=5
        )
    elif locks and IGNORE_LOCKS:
        _warn(f"{len(locks)} lock file(s) detected but ignored due to IGNORE_LOCKS=True.")

    # Prepare destination (includes identical-path and exists checks)
    dest_path = ensure_destination(DEST_PARENT, DEST_GDB_NAME, SOURCE_GDB)

    _msg(f"Copying:\n  From: {SOURCE_GDB}\n  To:   {dest_path}")
    try:
        copy_fgdb(SOURCE_GDB, dest_path)
    except Exception as ex:
        abort_with_instructions(
            "Copy failed.",
            [
                f"Error: {ex}",
                "Common causes:",
                "  • Insufficient permissions on destination.",
                "  • Path too long or invalid characters.",
                "  • Destination created partially—remove the partial folder and re-run."
            ],
            exit_code=6
        )

    _msg("Copy complete.")
    summarize(dest_path)
    _msg("=== FGDB Migration Done ===")

if __name__ == "__main__":
    main()
