# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
================================================================================
Script: Recalculate_Spatial_Index_ALL.py
Author: Nathanael Sheean
Date: 2025-09-14
Version: 2.0.0 (Cross-user, safe, deep-group aware)

Purpose:
  Rebuild spatial indexes and recalc extents for all concrete feature classes
  referenced anywhere in the CURRENT ArcGIS Pro project. Traverses nested
  GroupLayers and composite layers. Deduplicates by catalogPath. Produces a
  detailed log and two CSVs for QA/QC.

Safety:
  Non-destructive. Updates spatial index metadata and (optionally) feature
  class extent only. Geometry and attributes are unchanged.

Outputs (written to project home or next to the default GDB):
  • RebuildSI_Deep_<timestamp>.log
  • RebuildSI_Processed_<timestamp>.csv
  • RebuildSI_Skipped_<timestamp>.csv
================================================================================
"""

import arcpy, os, sys, csv, logging, time
from collections import OrderedDict, deque
from datetime import datetime

# ---------------- Configuration (adjust only if needed) ----------------------
RECALC_EXTENT  = True   # also recalc FC extent header with RecalculateFeatureClassExtent
REMOVE_FIRST   = True   # try RemoveSpatialIndex before AddSpatialIndex when possible

# ---------------- Resolve project paths (cross-user) -------------------------
_aprx = arcpy.mp.ArcGISProject("CURRENT")
_HOME = _aprx.homeFolder or os.path.dirname(_aprx.defaultGeodatabase or "") or arcpy.env.scratchFolder or os.getcwd()
os.makedirs(_HOME, exist_ok=True)

ts = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_PATH   = os.path.join(_HOME, f"RebuildSI_Deep_{ts}.log")
CSV_PROC   = os.path.join(_HOME, f"RebuildSI_Processed_{ts}.csv")
CSV_SKIP   = os.path.join(_HOME, f"RebuildSI_Skipped_{ts}.csv")

# ---------------- Logger ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger("RebuildSI.Deep")

def _msg(s): arcpy.AddMessage(s); log.info(s)
def _warn(s): arcpy.AddWarning(s); log.warning(s)
def _err(s): arcpy.AddError(s); log.error(s)

# ---------------- Helpers ----------------
def describe_safe(path):
    try:
        return arcpy.Describe(path)
    except Exception:
        return None

def is_concrete_fc(path):
    """
    Accept local or enterprise feature classes and shapefiles with geometry.
    Reject services, query layers, joins, tables, and annotation/dimension.
    """
    if not path or not arcpy.Exists(path):
        return False
    d = describe_safe(path)
    if not d:
        return False
    st = getattr(d, "shapeType", None)
    return st in ("Point", "Polyline", "Polygon", "Multipoint")

def has_spatial_index(path):
    d = describe_safe(path)
    return bool(getattr(d, "hasSpatialIndex", False)) if d else False

def test_schema_lock(path):
    try:
        return arcpy.TestSchemaLock(path)
    except Exception:
        return False

def iter_all_feature_layers(map_obj):
    """
    Depth-first traversal across:
      - GroupLayers (nested)
      - Composite layers exposing sublayers (listLayers)
    Yields leaf layers that behave like feature layers and expose dataSource.
    """
    stack = deque(map_obj.listLayers())
    while stack:
        lyr = stack.pop()
        # Descend into any layer that can contain children
        try:
            subs = lyr.listLayers()
            if subs:
                for sub in subs:
                    stack.append(sub)
        except Exception:
            pass
        # Yield feature-like leaves
        try:
            if getattr(lyr, "isFeatureLayer", False) and hasattr(lyr, "dataSource"):
                yield lyr
        except Exception:
            continue

def infer_workspace(path):
    """Infer a workspace from a dataset path for fallback walking."""
    try:
        d = describe_safe(path)
        if d and getattr(d, "catalogPath", None):
            path = d.catalogPath
    except Exception:
        pass
    if not path:
        return None
    p = path.replace("/", os.sep).lower()
    if ".gdb" in p:
        return path[: p.rfind(".gdb") + 4]
    if ".sde" in p:
        return path[: p.rfind(".sde") + 4]
    if p.endswith(".shp"):
        return os.path.dirname(path)
    return os.path.dirname(path)

def walk_workspace_collect_fc(workspace):
    """Collect concrete feature classes by walking a workspace."""
    collected = OrderedDict()
    try:
        for dirpath, dirnames, filenames in arcpy.da.Walk(workspace, datatype="FeatureClass"):
            for name in filenames:
                fc = os.path.join(dirpath, name)
                if is_concrete_fc(fc):
                    cat = describe_safe(fc).catalogPath
                    collected[cat] = True
    except Exception as ex:
        log.info(f"Workspace walk skipped for {workspace}: {ex}")
    return collected

def rebuild_fc(fc_path):
    """
    Attempt to remove and add spatial index; optionally recalc extent.
    Returns tuple(status, details_dict)
      status: 'Processed' or 'Skipped'
      details_dict: for CSV row
    """
    d = describe_safe(fc_path)
    ds_name = getattr(d, "name", os.path.basename(fc_path)) if d else os.path.basename(fc_path)
    ws = getattr(d, "path", os.path.dirname(fc_path)) if d else os.path.dirname(fc_path)
    shp = getattr(d, "shapeType", None) if d else None
    before_idx = has_spatial_index(fc_path)

    row = {
        "dataset": ds_name,
        "catalogPath": getattr(d, "catalogPath", fc_path) if d else fc_path,
        "workspace": ws,
        "shapeType": shp or "",
        "hadSpatialIndex": before_idx,
        "removedFirst": False,
        "addedIndex": False,
        "recalcExtent": False,
        "result": "",
        "note": ""
    }

    log.info(f"PROCESSING: {row['catalogPath']}")
    if not test_schema_lock(fc_path):
        row["result"] = "Skipped"
        row["note"] = "Schema lock in use (versioned edit, open session, or service)."
        log.info("  Skipped: schema lock.")
        return "Skipped", row

    try:
        if REMOVE_FIRST and before_idx:
            try:
                arcpy.management.RemoveSpatialIndex(fc_path)
                row["removedFirst"] = True
                log.info("  Removed spatial index.")
            except Exception as ex:
                log.info(f"  Remove skipped: {ex}")

        arcpy.management.AddSpatialIndex(fc_path)
        row["addedIndex"] = True
        log.info("  Added spatial index.")

        if RECALC_EXTENT:
            try:
                arcpy.management.RecalculateFeatureClassExtent(fc_path)
                row["recalcExtent"] = True
                log.info("  Recalculated extent.")
            except Exception as ex:
                log.info(f"  Extent recalc skipped: {ex}")

        row["result"] = "Processed"
        return "Processed", row

    except Exception as ex:
        row["result"] = "Skipped"
        row["note"] = f"Error: {ex}"
        log.error(f"  ERROR: {ex}")
        return "Skipped", row

# ---------------- Main ----------------
def run():
    maps = _aprx.listMaps()
    if not maps:
        raise RuntimeError("No maps found in the current project.")

    _msg(f"Project: {_aprx.filePath}")
    _msg("Maps detected: " + ", ".join(m.name for m in maps))

    # Collect all concrete FCs referenced by maps (deep)
    seen_fc = OrderedDict()
    skips_layers = []

    for m in maps:
        for lyr in iter_all_feature_layers(m):
            name = lyr.name
            try:
                ds = lyr.dataSource
            except Exception as ex:
                skips_layers.append((name, "No dataSource", str(ex)))
                continue

            if is_concrete_fc(ds):
                cat = describe_safe(ds).catalogPath
                seen_fc[cat] = True
            else:
                if not arcpy.Exists(ds):
                    skips_layers.append((name, "Path does not exist", ds))
                else:
                    d = describe_safe(ds)
                    dt = getattr(d, "dataType", None) if d else None
                    st = getattr(d, "shapeType", None) if d else None
                    reason = "Not a concrete, writable feature class"
                    if st is None:
                        reason = f"{dt} exposes no shapeType (likely service/query/join)"
                    skips_layers.append((name, reason, ds))

    _msg(f"Eligible feature classes via deep traversal: {len(seen_fc)}")

    processed_rows, skipped_rows = [], []
    processed = 0

    # Process found FCs
    for fc in seen_fc.keys():
        status, row = rebuild_fc(fc)
        (processed_rows if status == "Processed" else skipped_rows).append(row)
        if status == "Processed":
            processed += 1

    # Fallback discovery if nothing processed
    if processed == 0:
        workspaces = OrderedDict()
        for _, _, detail in skips_layers:
            ws = infer_workspace(detail) if isinstance(detail, str) else None
            if ws and arcpy.Exists(ws):
                workspaces[ws] = True

        # Always include default GDB for good measure
        if _aprx.defaultGeodatabase and arcpy.Exists(_aprx.defaultGeodatabase):
            workspaces[_aprx.defaultGeodatabase] = True

        if workspaces:
            _msg("Fallback workspaces: " + ", ".join(workspaces.keys()))
            walked = OrderedDict()
            for ws in workspaces.keys():
                walked.update(walk_workspace_collect_fc(ws))
            _msg(f"Feature classes discovered via workspace walk: {len(walked)}")
            for fc in walked.keys():
                status, row = rebuild_fc(fc)
                (processed_rows if status == "Processed" else skipped_rows).append(row)
                if status == "Processed":
                    processed += 1
        else:
            _msg("No valid local workspaces inferred for fallback.")

    # ---------------- Write CSVs ----------------
    proc_fields = ["dataset","catalogPath","workspace","shapeType",
                   "hadSpatialIndex","removedFirst","addedIndex","recalcExtent","result","note"]
    skip_fields = ["layerName","reason","detail"]

    with open(CSV_PROC, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=proc_fields)
        w.writeheader()
        for r in processed_rows + skipped_rows:  # include both with result flags
            # Only rows from rebuild_fc have these keys
            if "dataset" in r:
                w.writerow(r)

    with open(CSV_SKIP, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=skip_fields)
        w.writeheader()
        for n, reason, detail in skips_layers:
            w.writerow({"layerName": n, "reason": reason, "detail": detail})

    # ---------------- Summary ----------------
    _msg(f"Processed feature classes: {sum(1 for r in processed_rows if r.get('result')=='Processed')}")
    _msg(f"Skipped during rebuild (from processing phase): {sum(1 for r in skipped_rows if r.get('result')=='Skipped')}")
    if skips_layers:
        _msg(f"Skipped layers during discovery: {len(skips_layers)}")

    _msg(f"Log written to: {LOG_PATH}")
    _msg(f"Processed CSV: {CSV_PROC}")
    _msg(f"Skipped CSV:   {CSV_SKIP}")

if __name__ == "__main__":
    run()
