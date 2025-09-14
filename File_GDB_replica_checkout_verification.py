# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
=============================================================================
Script: File_GDB_replica_checkout_verification.py
Author: Nathanael Sheean
Date: 2025-09-14
Version: 2.1 (Active-map aware, no hardcoding, explicit messaging)

Purpose:
  Report replica information for one or more File Geodatabases (.gdb).
  Runs inside ArcGIS Pro on the CURRENT project. No hardcoded paths.

How to use (choose any combination):
  1) Paste parent folders to scan recursively for *.gdb (SEARCH_FOLDERS).
  2) Paste explicit .gdb paths (TARGET_GDBS).
  3) Leave both empty to auto-discover the project's default GDB and any .gdb
     referenced by layers in the ACTIVE MAP.

Outputs:
  • Replica_Check_<timestamp>.csv  → written to project home.

Behavior guarantees (no silent failure):
  • Prints each candidate GDB path before checking.
  • Prints one line per replica with full path and properties.
  • Prints an explicit "<no replicas>" line for empty GDBs.
  • Prints "ERROR:" lines when a GDB cannot be read.
  • Prints a final summary with totals.
=============================================================================
"""

import arcpy, os, csv
from datetime import datetime
from collections import OrderedDict, deque

# --------- PASTE HERE (optional) --------------------------------------------
# Option A: Folders to scan recursively for *.gdb
SEARCH_FOLDERS = [
    # r"C:\Data\Sites",
    # r"D:\Projects\{BASE_CODE}"
]

# Option B: Explicit .gdb paths to include
TARGET_GDBS = [
    # r"C:\Data\MyProject\Default.gdb",
    # r"D:\Replica\FieldEdits.gdb"
]
# ---------------------------------------------------------------------------

def _ts(): return datetime.now().strftime("%Y%m%d_%H%M%S")

# Project context
APRX = arcpy.mp.ArcGISProject("CURRENT")
ACTIVE = APRX.activeMap
HOME = APRX.homeFolder or os.path.dirname(APRX.defaultGeodatabase or "") or os.path.expanduser("~")
os.makedirs(HOME, exist_ok=True)
CSV_OUT = os.path.join(HOME, f"Replica_Check_{_ts()}.csv")

def msg(s): arcpy.AddMessage(s)
def warn(s): arcpy.AddWarning(s)
def err(s): arcpy.AddError(s)

def discover_gdbs_from_active_map():
    """Find .gdb paths from active map layer dataSources."""
    results = OrderedDict()
    if ACTIVE is None:
        return results
    stack = deque(ACTIVE.listLayers())
    while stack:
        lyr = stack.pop()
        # descend into groups/composites
        try:
            subs = lyr.listLayers()
            if subs:
                for sub in subs: stack.append(sub)
                continue
        except Exception:
            pass
        # leaf
        try:
            if hasattr(lyr, "supports") and lyr.supports("DATASOURCE"):
                ds = lyr.dataSource
            else:
                continue
        except Exception:
            continue
        if not ds:
            continue
        p = str(ds).replace("/", os.sep)
        lower = p.lower()
        if ".gdb" in lower:
            gdb = p[: lower.rfind(".gdb") + 4]
            if os.path.isdir(gdb):
                results[gdb] = True
    return results

def discover_gdbs_from_project_default():
    """Include the project's default geodatabase, if present."""
    out = OrderedDict()
    gdb = APRX.defaultGeodatabase
    if gdb and os.path.isdir(gdb) and gdb.lower().endswith(".gdb"):
        out[gdb] = True
    return out

def scan_parent_folders_for_gdbs(folders):
    """Recursively find *.gdb directories under the given parent folders."""
    out = OrderedDict()
    for root in folders:
        if not root or not os.path.isdir(root):
            warn(f"Folder not found or not a directory: {root}")
            continue
        for dirpath, dirnames, _ in os.walk(root):
            for d in list(dirnames):
                if d.lower().endswith(".gdb"):
                    gdb_path = os.path.join(dirpath, d)
                    out[gdb_path] = True
    return out

def validate_explicit_gdbs(paths):
    out = OrderedDict()
    for p in paths:
        if p and os.path.isdir(p) and p.lower().endswith(".gdb"):
            out[p] = True
        else:
            warn(f"Not a valid File GDB: {p}")
    return out

def list_replicas_safe(workspace):
    """Call arcpy.da.ListReplicas with guardrails; raise informative errors."""
    try:
        reps = arcpy.da.ListReplicas(workspace)
        return reps or []
    except Exception as ex:
        raise RuntimeError(str(ex))

def replica_props(rep):
    """Safely extract common replica properties."""
    def g(attr):
        try:
            return getattr(rep, attr, None)
        except Exception:
            return None
    return {
        "name": g("name"),
        "date": g("replicaDate"),
        "id": g("replicaID"),
        "version": g("replicaVersion"),
        "type": g("replicaType"),
        "role": g("role")
    }

def run():
    # Build candidate set
    candidates = OrderedDict()
    candidates.update(discover_gdbs_from_project_default())
    candidates.update(discover_gdbs_from_active_map())
    candidates.update(scan_parent_folders_for_gdbs(SEARCH_FOLDERS))
    candidates.update(validate_explicit_gdbs(TARGET_GDBS))

    if not candidates:
        err("No candidate File Geodatabases (.gdb) resolved. Paste paths in SEARCH_FOLDERS or TARGET_GDBS and run again.")
        # Still write an empty CSV with header for clarity
        fields = ["workspace","status","replica_name","replica_date","replica_id","replica_version","replica_type","replica_role","note"]
        with open(CSV_OUT, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fields).writeheader()
        msg(f"Empty report written to: {CSV_OUT}")
        return

    msg(f"Checking {len(candidates)} File Geodatabase(s):")
    for g in candidates.keys():
        msg(f"  • {g}")

    rows = []
    gdb_with_reps = 0
    gdb_no_reps = 0
    gdb_errors = 0
    total_reps = 0

    for gdb in candidates.keys():
        try:
            reps = list_replicas_safe(gdb)
            if not reps:
                msg(f"{gdb} | <no replicas>")
                rows.append({
                    "workspace": gdb, "status": "No replicas",
                    "replica_name": "", "replica_date": "", "replica_id": "",
                    "replica_version": "", "replica_type": "", "replica_role": "",
                    "note": "No replicas"
                })
                gdb_no_reps += 1
                continue

            gdb_with_reps += 1
            for r in reps:
                rp = replica_props(r)
                total_reps += 1
                # Print one line per replica with full path + key props
                msg(
                    f"{gdb} | name={rp['name'] or ''} | id={rp['id'] or ''} | version={rp['version'] or ''} "
                    f"| type={rp['type'] or ''} | role={rp['role'] or ''} | date={rp['date'] or ''}"
                )
                rows.append({
                    "workspace": gdb, "status": "OK",
                    "replica_name": rp["name"] or "", "replica_date": rp["date"] or "",
                    "replica_id": rp["id"] or "", "replica_version": rp["version"] or "",
                    "replica_type": rp["type"] or "", "replica_role": rp["role"] or "",
                    "note": "OK"
                })

        except Exception as ex:
            gdb_errors += 1
            err(f"{gdb} | ERROR: {ex}")
            rows.append({
                "workspace": gdb, "status": "Error",
                "replica_name": "", "replica_date": "", "replica_id": "",
                "replica_version": "", "replica_type": "", "replica_role": "",
                "note": f"Error: {ex}"
            })

    # Write CSV
    fields = ["workspace","status","replica_name","replica_date","replica_id","replica_version","replica_type","replica_role","note"]
    with open(CSV_OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(rows)

    # Final summary (no ambiguity)
    msg("----- Summary -----")
    msg(f"GDBs checked:      {len(candidates)}")
    msg(f"GDBs with replicas:{gdb_with_reps}")
    msg(f"GDBs with none:    {gdb_no_reps}")
    msg(f"GDBs with errors:  {gdb_errors}")
    msg(f"Replicas total:    {total_reps}")
    msg(f"Report CSV:        {CSV_OUT}")

if __name__ == "__main__":
    run()
