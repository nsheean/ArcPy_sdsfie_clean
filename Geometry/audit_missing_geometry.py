# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
================================================================================
Script: audit_bad_geometry.py
Author: Nathanael Sheean
Date: 2025-09-14
Version: 3.1 (Active-map only; cross-user; deep traversal; explicit reporting)

Purpose:
  Audit feature classes referenced by the ACTIVE MAP for bad geometry.

What is flagged:
  • CheckGeometry problems (self-intersections, null parts, etc.).
  • Empty shapes (SHAPE@ is None or isEmpty).
  • Zero-area polygons.
  • Zero-length polylines.

Scope:
  • ACTIVE MAP ONLY. Traverses nested group layers and composites to leaf layers.
  • Service/virtual/joined layers are recorded as skipped with a clear reason.

Outputs (project home):
  • bad_geometry_findings_<ts>.csv  → one row per issue
  • bad_geometry_scanned_<ts>.csv   → feature classes scanned (counts)
  • bad_geometry_skipped_<ts>.csv   → datasets skipped with reasons
  • bad_geometry_log_<ts>.log       → verbose log
================================================================================
"""

import arcpy, os, csv, logging, datetime
from collections import OrderedDict, deque

# -------------------- Project context & outputs ------------------------------
APRX = arcpy.mp.ArcGISProject("CURRENT")
HOME = APRX.homeFolder or os.path.dirname(APRX.defaultGeodatabase or "") or arcpy.env.scratchFolder or os.getcwd()
os.makedirs(HOME, exist_ok=True)
TS = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

CSV_FINDINGS = os.path.join(HOME, f"bad_geometry_findings_{TS}.csv")
CSV_SCANNED  = os.path.join(HOME, f"bad_geometry_scanned_{TS}.csv")
CSV_SKIPPED  = os.path.join(HOME, f"bad_geometry_skipped_{TS}.csv")
LOG_PATH     = os.path.join(HOME, f"bad_geometry_log_{TS}.log")

logging.basicConfig(filename=LOG_PATH, level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("BadGeom")

def msg(s): arcpy.AddMessage(s);  log.info(s)
def warn(s): arcpy.AddWarning(s); log.warning(s)
def err(s): arcpy.AddError(s);    log.error(s)

# -------------------- Layer & dataset utilities ------------------------------

def is_service_or_virtual(lyr) -> bool:
    """Skip map/feature services, WMS/WMTS, or layers with joins."""
    try:
        d = arcpy.Describe(lyr)
        src = (getattr(d, "dataSource", "") or "").lower()
        if any(s in src for s in (".mapserver", ".featureserver", "/wms", "/wmts")):
            return True
        return bool(getattr(d, "hasJoin", False))
    except Exception:
        return False

def resolve_dataset_path(lyr) -> str:
    """Prefer Describe.catalogPath; fall back to lyr.dataSource."""
    try:
        d = arcpy.Describe(lyr)
        cp = getattr(d, "catalogPath", "") or ""
        if cp: return cp
        return getattr(lyr, "dataSource", "") or ""
    except Exception:
        try:
            return getattr(lyr, "dataSource", "") or ""
        except Exception:
            return ""

def is_feature_class(path: str) -> bool:
    try:
        if not path or not arcpy.Exists(path): return False
        dt = getattr(arcpy.Describe(path), "dataType", "").lower()
        return dt in {"featureclass", "featureclassshapefile"}
    except Exception:
        return False

def shape_type(path: str) -> str:
    try:
        return arcpy.Describe(path).shapeType
    except Exception:
        return ""

def iter_leaf_layers_active_map():
    """Depth-first traversal of ACTIVE MAP; yield only leaf layers."""
    m = APRX.activeMap
    if not m:
        return
    stack = deque(m.listLayers())
    while stack:
        lyr = stack.pop()
        try:
            subs = lyr.listLayers()
            if subs:
                for s in subs: stack.append(s)
                continue
        except Exception:
            pass
        yield lyr

def gather_active_map_featureclasses():
    """
    Return dict[fc_catalog_path] -> {'layer_refs': set(layer names in map)}.
    Deduplicates multiple references to the same dataset.
    """
    cands = OrderedDict()
    for lyr in iter_leaf_layers_active_map():
        # Skip non-feature layers up front
        if not getattr(lyr, "isFeatureLayer", False):
            continue
        # Skip services/virtual/joined
        if is_service_or_virtual(lyr):
            # will be recorded as skipped later when evaluated
            pass
        path = resolve_dataset_path(lyr)
        if not path:
            continue
        try:
            cp = arcpy.Describe(path).catalogPath
        except Exception:
            cp = path
        entry = cands.setdefault(cp, {"layer_refs": set(), "lyr_objs": []})
        entry["layer_refs"].add(getattr(lyr, "name", ""))
        entry["lyr_objs"].append(lyr)
    return cands

# -------------------- CheckGeometry + direct checks --------------------------

def check_geometry(fc_path):
    """
    Run CheckGeometry and return a list of dict rows describing problems.
    The output schema varies by version; we select common fields and fallback.
    """
    rows = []
    try:
        scratch = arcpy.env.scratchGDB or arcpy.CreateFileGDB_management(HOME, f"_scratch_{TS}.gdb").getOutput(0)
        out_table = os.path.join(scratch, f"chk_{abs(hash(fc_path)) % 1000000}")
        if arcpy.Exists(out_table):
            arcpy.Delete_management(out_table)
        arcpy.management.CheckGeometry(fc_path, out_table)  # read-only

        fields = [f.name for f in arcpy.ListFields(out_table)]
        pick = lambda *cands: next((c for c in cands if c in fields), None)

        f_oid  = pick("OID", "OBJECTID", "FEATURE_ID", "FID", "ORIG_FID", "SOURCE_OID")
        f_prob = pick("PROBLEM", "Problem", "PROBLEM_TYPE", "ProblemType")
        f_desc = pick("DESCRIPTION", "Problem_Description", "PROBLEM_DESC", "ProblemDescript")
        f_x    = pick("X", "POINT_X")
        f_y    = pick("Y", "POINT_Y")

        use_fields = [c for c in (f_oid, f_prob, f_desc, f_x, f_y) if c] or fields
        with arcpy.da.SearchCursor(out_table, use_fields) as cur:
            for r in cur:
                rec = {"issue_source": "CheckGeometry"}
                for i, col in enumerate(use_fields):
                    rec[col] = r[i]
                rows.append(rec)

        try: arcpy.Delete_management(out_table)
        except Exception: pass

    except Exception as ex:
        err(f"{fc_path} | CheckGeometry error: {ex}")
        rows.append({"issue_source": "CheckGeometry", "error": str(ex)})
    return rows

def scan_empty_zero(fc_path, shp_type):
    """Direct cursor checks for empty shapes, zero-area polygons, zero-length polylines."""
    issues = []
    try:
        tokens = ["OID@", "SHAPE@", "SHAPE@AREA", "SHAPE@LENGTH"]
        with arcpy.da.SearchCursor(fc_path, tokens) as cur:
            for oid, geom, area, length in cur:
                if geom is None or geom.isEmpty:
                    issues.append({"issue_source": "Direct", "issue_code": "EMPTY_SHAPE", "OID": oid})
                    continue
                if shp_type == "Polygon":
                    if area is None or float(area) == 0.0:
                        issues.append({"issue_source": "Direct", "issue_code": "ZERO_AREA_POLYGON", "OID": oid})
                elif shp_type == "Polyline":
                    if length is None or float(length) == 0.0:
                        issues.append({"issue_source": "Direct", "issue_code": "ZERO_LENGTH_POLYLINE", "OID": oid})
    except Exception as ex:
        issues.append({"issue_source": "Direct", "error": str(ex)})
    return issues

# -------------------- Main ---------------------------------------------------

def run():
    arcpy.env.addOutputsToMap = False

    # Prepare ledgers
    with open(CSV_FINDINGS, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            "DatasetPath","LayerRefs","ShapeType","IssueSource","IssueCodeOrType",
            "IssueDescription","FeatureOID","X","Y"
        ])
    with open(CSV_SCANNED, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(["DatasetPath","ShapeType","Features","Issues"])
    with open(CSV_SKIPPED, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(["DatasetPath","Reason"])

    if APRX.activeMap is None:
        err("No active map. Open a map and re-run.")
        msg(f"Findings CSV: {CSV_FINDINGS}")
        msg(f"Scanned CSV:  {CSV_SCANNED}")
        msg(f"Skipped CSV:  {CSV_SKIPPED}")
        msg(f"Log:          {LOG_PATH}")
        return

    cands = gather_active_map_featureclasses()
    if not cands:
        err("Active map contains no feature layers with resolvable datasets.")
        msg(f"Findings CSV: {CSV_FINDINGS}")
        msg(f"Scanned CSV:  {CSV_SCANNED}")
        msg(f"Skipped CSV:  {CSV_SKIPPED}")
        msg(f"Log:          {LOG_PATH}")
        return

    msg(f"Feature classes to scan (active map): {len(cands)}")

    total_fc = 0
    total_issues = 0

    for fc_path, meta in cands.items():
        total_fc += 1
        layer_refs = ";".join(sorted(meta.get("layer_refs", []))) if meta.get("layer_refs") else ""

        # Validate dataset path
        if not arcpy.Exists(fc_path):
            with open(CSV_SKIPPED, "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([fc_path, "Dataset not found"])
            msg(f"{fc_path} | <skipped: not found>")
            continue

        # Skip service/virtual/joined datasets (cannot run CheckGeometry reliably)
        try:
            # If any source layer for this dataset is service/virtual/joined, treat as not eligible
            for lyr in meta.get("lyr_objs", []):
                if is_service_or_virtual(lyr):
                    with open(CSV_SKIPPED, "a", newline="", encoding="utf-8") as f:
                        csv.writer(f).writerow([fc_path, "Service/virtual/joined layer not eligible"])
                    msg(f"{fc_path} | <skipped: service/virtual/joined>")
                    raise StopIteration
        except StopIteration:
            continue

        shp = shape_type(fc_path)
        if shp not in {"Point","Polyline","Polygon","Multipoint"}:
            with open(CSV_SKIPPED, "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([fc_path, f"Unsupported shape type: {shp}"])
            msg(f"{fc_path} | <skipped: {shp}>")
            continue

        # Run checks
        try:
            cg_rows = check_geometry(fc_path)
            direct_rows = scan_empty_zero(fc_path, shp)

            # Write findings
            issues_here = 0
            with open(CSV_FINDINGS, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)

                # CheckGeometry results
                for rec in cg_rows:
                    if "error" in rec:
                        with open(CSV_SKIPPED, "a", newline="", encoding="utf-8") as fs:
                            csv.writer(fs).writerow([fc_path, f"CheckGeometry error: {rec['error']}"])
                        continue
                    issue_type = rec.get("PROBLEM") or rec.get("Problem") or rec.get("PROBLEM_TYPE") or rec.get("ProblemType") or ""
                    issue_desc = rec.get("DESCRIPTION") or rec.get("Problem_Description") or rec.get("PROBLEM_DESC") or ""
                    foid = rec.get("OID") or rec.get("OBJECTID") or rec.get("FEATURE_ID") or rec.get("FID") or rec.get("ORIG_FID") or rec.get("SOURCE_OID") or ""
                    x = rec.get("X") or rec.get("POINT_X") or ""
                    y = rec.get("Y") or rec.get("POINT_Y") or ""
                    w.writerow([fc_path, layer_refs, shp, "CheckGeometry", issue_type, issue_desc, foid, x, y])
                    issues_here += 1

                # Direct rows
                for rec in direct_rows:
                    if "error" in rec:
                        with open(CSV_SKIPPED, "a", newline="", encoding="utf-8") as fs:
                            csv.writer(fs).writerow([fc_path, f"Direct scan error: {rec['error']}"])
                        continue
                    w.writerow([
                        fc_path, layer_refs, shp, "Direct",
                        rec.get("issue_code",""), "",
                        rec.get("OID",""), "", ""
                    ])
                    issues_here += 1

            total_issues += issues_here

            # Feature count
            try:
                feat_ct = int(arcpy.management.GetCount(fc_path)[0])
            except Exception:
                feat_ct = ""

            with open(CSV_SCANNED, "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([fc_path, shp, feat_ct, issues_here])

            msg(f"{fc_path} | shape={shp} | features={feat_ct} | issues={issues_here}")

        except Exception as ex:
            with open(CSV_SKIPPED, "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow([fc_path, f"Error: {ex}"])
            err(f"{fc_path} | ERROR: {ex}")

    # Summary
    msg("----- Summary -----")
    msg(f"Feature classes scanned: {total_fc}")
    msg(f"Issues found:           {total_issues}")
    msg(f"Findings CSV:           {CSV_FINDINGS}")
    msg(f"Scanned CSV:            {CSV_SCANNED}")
    msg(f"Skipped CSV:            {CSV_SKIPPED}")
    msg(f"Log:                    {LOG_PATH}")

if __name__ == "__main__":
    run()
