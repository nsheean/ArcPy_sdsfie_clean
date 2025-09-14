# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
===================================================================================
Script: Alias_Based_Field_Selection_and_Calculation.py
Author: Nathanael Sheean
Date: 2025-09-14
Version: 3.0 (Active-map only, cross-user safe, deep traversal, full audit logging)

Purpose:
  Find a field by its Alias across feature classes referenced by the ACTIVE MAP,
  select features where that field is NULL or equals any configured placeholder,
  and populate the field with a configured text value. Non-text fields are skipped.

Scope:
  • Active map only. Deep traversal through group layers and composites.
  • Deduplicates datasets referenced by multiple layers; processes each dataset once.
  • Skips service/virtual/joined layers and datasets without an exclusive schema lock.

Outputs (to project home):
  • alias_calc_audit_<timestamp>.csv   → all attempts with action and reason
  • alias_calc_updated_<timestamp>.csv → only datasets that were updated
  • alias_calc_skipped_<timestamp>.csv → skipped/non-compliant/error with reason
  • alias_calc_<timestamp>.log         → verbose log and mirrored Messages

Operator parameters (edit below):
  • TARGET_ALIAS          → field Alias to match (case-insensitive)
  • NULL_MATCH_VALUES     → list of placeholder strings considered “empty” (case-insensitive)
  • FILL_VALUE_TEXT       → text to write into the matched field (expression auto-quoted)
  • CALC_CODE_BLOCK       → optional Python code block for CalculateField (rarely needed)
===================================================================================
"""

import arcpy, os, csv, logging
from datetime import datetime
from collections import OrderedDict, deque

# ------------------- OPERATOR PARAMETERS -------------------------------------
TARGET_ALIAS        = "This is the Field Alias"     # Field Alias to match
NULL_MATCH_VALUES   = ["TBD", "UNKNOWN", ""]        # Values treated as empty (case-insensitive)
FILL_VALUE_TEXT     = "This is the text to populate the field"  # Auto-quoted for PYTHON3 expression
CALC_CODE_BLOCK     = ""                            # Optional; leave empty if unused
# -----------------------------------------------------------------------------


# ------------------- Project context & outputs --------------------------------
APRX = arcpy.mp.ArcGISProject("CURRENT")
HOME = APRX.homeFolder or os.path.dirname(APRX.defaultGeodatabase or "") or arcpy.env.scratchFolder or os.getcwd()
os.makedirs(HOME, exist_ok=True)
TS   = datetime.now().strftime("%Y%m%d_%H%M%S")

LOG_PATH     = os.path.join(HOME, f"alias_calc_{TS}.log")
AUDIT_CSV    = os.path.join(HOME, f"alias_calc_audit_{TS}.csv")
UPDATED_CSV  = os.path.join(HOME, f"alias_calc_updated_{TS}.csv")
SKIPPED_CSV  = os.path.join(HOME, f"alias_calc_skipped_{TS}.csv")

logging.basicConfig(filename=LOG_PATH, level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("AliasCalc")
def msg(s):  arcpy.AddMessage(s);  log.info(s)
def warn(s): arcpy.AddWarning(s);  log.warning(s)
def err(s):  arcpy.AddError(s);    log.error(s)

# ------------------- CSV schemas ---------------------------------------------
AUDIT_HDR   = ["timestamp_utc","map","dataset","layer_refs","matched_field","field_type",
               "selected_count","updated_count","action","reason","workspace"]
UPDATED_HDR = ["timestamp_utc","map","dataset","layer_refs","matched_field","field_type",
               "selected_count","updated_count","calc_expression","workspace"]
SKIPPED_HDR = ["timestamp_utc","map","dataset","layer_refs","action","reason","workspace"]

def _ensure_header(path, header):
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(header)

def _ts_utc():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

# ------------------- Layer & dataset utilities --------------------------------
def _iter_leaf_layers_active_map():
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

def _is_service_or_virtual(lyr) -> bool:
    try:
        d = arcpy.Describe(lyr)
        src = (getattr(d, "dataSource", "") or "").lower()
        if any(s in src for s in (".mapserver",".featureserver","/wms","/wmts")):
            return True
        return bool(getattr(d, "hasJoin", False))
    except Exception:
        return False

def _catalog_path_or_empty(thing) -> str:
    try:
        return arcpy.Describe(thing).catalogPath
    except Exception:
        try:
            return getattr(thing, "dataSource", "") or ""
        except Exception:
            return ""

def _workspace_of(path: str) -> str:
    try:
        return arcpy.Describe(path).path
    except Exception:
        try:
            return arcpy.da.Describe(path).get("path", "")
        except Exception:
            return ""

def _schema_lock_ok(path: str) -> bool:
    try:
        return arcpy.TestSchemaLock(path)
    except Exception:
        return False

def _is_feature_class(path: str) -> bool:
    try:
        if not path or not arcpy.Exists(path): return False
        dt = getattr(arcpy.Describe(path), "dataType", "").lower()
        return dt in {"featureclass","featureclassshapefile"}
    except Exception:
        return False

def _gather_active_map_datasets():
    """
    Return dict[dataset_path] = {"layer_refs": set(), "layer_obj": representative layer, "map_name": map}
    Processes each dataset once, even if referenced by multiple layers.
    """
    cands = OrderedDict()
    m = APRX.activeMap
    if not m:
        return cands
    for lyr in _iter_leaf_layers_active_map():
        if not getattr(lyr, "isFeatureLayer", False):
            continue
        path = _catalog_path_or_empty(lyr)
        if not path:
            continue
        try:
            ds_path = arcpy.Describe(path).catalogPath
        except Exception:
            ds_path = path
        entry = cands.setdefault(ds_path, {"layer_refs": set(), "layer_obj": None, "map_name": m.name})
        entry["layer_refs"].add(getattr(lyr, "name", ""))
        if entry["layer_obj"] is None:
            entry["layer_obj"] = lyr  # first usable reference
    return cands

# ------------------- Field alias matching -------------------------------------
def _find_field_by_alias(dataset_path: str, target_alias: str):
    """
    Search dataset fields for a matching Alias (case-insensitive). Return (field_name, field_type).
    If multiple match, return the first and note that ambiguity in the reason.
    """
    try:
        fields = arcpy.ListFields(dataset_path)
    except Exception:
        return None, None, "No fields list available"
    matches = [f for f in fields if str(getattr(f, "aliasName", "")).strip().lower() == target_alias.strip().lower()]
    if not matches:
        return None, None, "Field Alias not found"
    if len(matches) > 1:
        chosen = matches[0]
        return chosen.name, chosen.type, "Multiple fields share the Alias; first selected"
    chosen = matches[0]
    return chosen.name, chosen.type, ""

# ------------------- Selection query builder ----------------------------------
def _build_text_null_query(layer, field_name: str, values_ci: list):
    """
    Build a SQL where clause selecting records where field is NULL or equals any value in values_ci
    (case-insensitive). Uses AddFieldDelimiters and UPPER(..) for portability.
    """
    fld = arcpy.AddFieldDelimiters(layer, field_name)
    parts = [f"{fld} IS NULL"]
    alts  = []
    for v in values_ci:
        if v is None:
            continue
        v = str(v)
        # Treat empty string explicitly
        if v == "":
            alts.append(f"{fld} = ''")
        # Case-insensitive equality via UPPER
        esc = v.replace("'", "''")
        alts.append(f"UPPER({fld}) = '{esc.upper()}'")
    if alts:
        parts.append("(" + " OR ".join(alts) + ")")
    return " OR ".join(parts)

# ------------------- Main processing for one dataset --------------------------
def _process_dataset(ds_path: str, layer_obj, layer_refs: set, map_name: str, writers, counters):
    audit_w, updated_w, skipped_w = writers
    ts = _ts_utc()
    lrefs = ";".join(sorted(layer_refs)) if layer_refs else ""
    ws = _workspace_of(ds_path)

    # Eligibility checks
    if not _is_feature_class(ds_path):
        msg(f"{ds_path} | <skipped: not a feature class>")
        skipped_w.writerow([ts, map_name, ds_path, lrefs, "skipped", "Not a feature class", ws])
        audit_w.writerow([ts, map_name, ds_path, lrefs, "", "", 0, 0, "skipped", "Not a feature class", ws])
        counters["skipped"] += 1
        return

    if _is_service_or_virtual(layer_obj):
        msg(f"{ds_path} | <skipped: service/virtual/joined>")
        skipped_w.writerow([ts, map_name, ds_path, lrefs, "skipped", "Service/virtual/joined layer not eligible", ws])
        audit_w.writerow([ts, map_name, ds_path, lrefs, "", "", 0, 0, "skipped", "Service/virtual/joined", ws])
        counters["skipped"] += 1
        return

    if not _schema_lock_ok(ds_path):
        msg(f"{ds_path} | <skipped: schema lock>")
        skipped_w.writerow([ts, map_name, ds_path, lrefs, "skipped", "Schema lock (dataset in use)", ws])
        audit_w.writerow([ts, map_name, ds_path, lrefs, "", "", 0, 0, "skipped", "Schema lock", ws])
        counters["skipped"] += 1
        return

    # Locate field by Alias
    field_name, field_type, alias_note = _find_field_by_alias(ds_path, TARGET_ALIAS)
    if not field_name:
        msg(f"{ds_path} | <skipped: alias not found>")
        skipped_w.writerow([ts, map_name, ds_path, lrefs, "skipped", "Field Alias not found", ws])
        audit_w.writerow([ts, map_name, ds_path, lrefs, "", "", 0, 0, "skipped", "Field Alias not found", ws])
        counters["skipped"] += 1
        return

    # Only text fields are updated; others are logged as non-compliant
    if str(field_type).lower() != "string":
        reason = f"Field '{field_name}' is {field_type}, only text supported"
        msg(f"{ds_path} | <non_compliant: {reason}>")
        skipped_w.writerow([ts, map_name, ds_path, lrefs, "non_compliant", reason, ws])
        audit_w.writerow([ts, map_name, ds_path, lrefs, field_name, field_type, 0, 0, "non_compliant", reason, ws])
        counters["non_compliant"] += 1
        return

    # Build selection and count candidates
    where = _build_text_null_query(layer_obj, field_name, NULL_MATCH_VALUES)
    arcpy.management.SelectLayerByAttribute(layer_obj, "NEW_SELECTION", where)
    try:
        selected = int(arcpy.management.GetCount(layer_obj)[0])
    except Exception:
        selected = 0

    if selected <= 0:
        msg(f"{ds_path} | field={field_name} | selected=0 | <no updates needed>")
        audit_w.writerow([ts, map_name, ds_path, lrefs, field_name, field_type, 0, 0, "skipped", "No NULL/placeholder matches", ws])
        counters["skipped"] += 1
        # clear selection
        try: arcpy.management.SelectLayerByAttribute(layer_obj, "CLEAR_SELECTION")
        except Exception: pass
        return

    # Calculate for selected features
    expr = repr(FILL_VALUE_TEXT)  # safe python string literal for PYTHON3 expression
    try:
        arcpy.management.CalculateField(
            in_table=layer_obj,
            field=field_name,
            expression=expr,
            expression_type="PYTHON3",
            code_block=CALC_CODE_BLOCK or ""
        )
        updated = selected
        msg(f"{ds_path} | field={field_name} | updated={updated}")
        updated_w.writerow([ts, map_name, ds_path, lrefs, field_name, field_type, selected, updated, expr, ws])
        audit_w.writerow([ts, map_name, ds_path, lrefs, field_name, field_type, selected, updated, "processed", alias_note or "OK", ws])
        counters["processed"] += 1
    except arcpy.ExecuteError as ee:
        reason = arcpy.GetMessages(2) or str(ee)
        err(f"{ds_path} | CalculateField ExecuteError: {reason}")
        skipped_w.writerow([ts, map_name, ds_path, lrefs, "error", f"ExecuteError: {reason}", ws])
        audit_w.writerow([ts, map_name, ds_path, lrefs, field_name, field_type, selected, 0, "error", f"ExecuteError: {reason}", ws])
        counters["errors"] += 1
    except Exception as ex:
        err(f"{ds_path} | CalculateField error: {ex}")
        skipped_w.writerow([ts, map_name, ds_path, lrefs, "error", str(ex), ws])
        audit_w.writerow([ts, map_name, ds_path, lrefs, field_name, field_type, selected, 0, "error", str(ex), ws])
        counters["errors"] += 1
    finally:
        try: arcpy.management.SelectLayerByAttribute(layer_obj, "CLEAR_SELECTION")
        except Exception: pass

# ------------------- Main -----------------------------------------------------
def run():
    _ensure_header(AUDIT_CSV, AUDIT_HDR)
    _ensure_header(UPDATED_CSV, UPDATED_HDR)
    _ensure_header(SKIPPED_CSV, SKIPPED_HDR)

    if APRX.activeMap is None:
        err("No active map. Open a map and re-run.")
        msg(f"Audit CSV:   {AUDIT_CSV}")
        msg(f"Updated CSV: {UPDATED_CSV}")
        msg(f"Skipped CSV: {SKIPPED_CSV}")
        msg(f"Log file:    {LOG_PATH}")
        return

    msg(f"Alias-based calculation start | Map: {APRX.activeMap.name} | Alias='{TARGET_ALIAS}'")

    cands = _gather_active_map_datasets()
    if not cands:
        err("Active map contains no feature layers with resolvable datasets.")
        msg(f"Audit CSV:   {AUDIT_CSV}")
        msg(f"Updated CSV: {UPDATED_CSV}")
        msg(f"Skipped CSV: {SKIPPED_CSV}")
        msg(f"Log file:    {LOG_PATH}")
        return

    with open(AUDIT_CSV, "a", newline="", encoding="utf-8") as fa, \
         open(UPDATED_CSV, "a", newline="", encoding="utf-8") as fu, \
         open(SKIPPED_CSV, "a", newline="", encoding="utf-8") as fs:

        audit_w   = csv.writer(fa)
        updated_w = csv.writer(fu)
        skipped_w = csv.writer(fs)

        counters = {"processed":0, "skipped":0, "non_compliant":0, "errors":0}

        for ds_path, meta in cands.items():
            _process_dataset(
                ds_path=ds_path,
                layer_obj=meta["layer_obj"],
                layer_refs=meta["layer_refs"],
                map_name=meta["map_name"],
                writers=(audit_w, updated_w, skipped_w),
                counters=counters
            )

    # Summary
    msg("----- Summary -----")
    msg(f"Processed (updated): {counters['processed']}  | Updated CSV: {UPDATED_CSV}")
    msg(f"Skipped:            {counters['skipped']}    | Skipped CSV: {SKIPPED_CSV}")
    msg(f"Non-compliant:      {counters['non_compliant']}")
    msg(f"Errors:             {counters['errors']}")
    msg(f"Audit CSV:          {AUDIT_CSV}")
    msg(f"Log file:           {LOG_PATH}")

if __name__ == "__main__":
    run()
