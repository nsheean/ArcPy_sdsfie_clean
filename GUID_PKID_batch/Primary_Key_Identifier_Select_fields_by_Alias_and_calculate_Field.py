# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
=====================================================================================
Script: Primary_Key_Identifier_Select_fields_by_Alias_and_calculate_Field.py
Author: Nathanael Sheean
Date: 2025-09-14
Version: 2.3.0 (Active-map only, deep nested group aware)

Purpose:
  In the ACTIVE MAP only, traverse all group and nested sublayers to find concrete
  feature classes whose field ALIAS equals "Primary Key Identifier". For each target,
  select NULL/placeholder values and populate with GUIDs. Writes a QA/QC CSV.

Scope:
  - Active map only (no other maps).
  - Deep traversal of GroupLayers and composite layers.
  - Deduplicates by catalogPath to avoid double edits.

Safety:
  - Non-destructive attribute fill only.
  - Skips schema-locked datasets.
  - Updates only selected rows.

Output:
  - PKID_Update_<timestamp>.csv in the project home.
=====================================================================================
"""

import arcpy, os, csv, uuid
from collections import OrderedDict, deque
from datetime import datetime

# ---------------- Configuration ----------------
TARGET_ALIAS = "Primary Key Identifier"  # case-insensitive alias match
PLACEHOLDERS = {"", " ", "TBD", "NULL", "N/A", "NA", "NONE", "UNKNOWN"}  # for TEXT PK fields

# ---------------- Project + paths ----------------
aprx = arcpy.mp.ArcGISProject("CURRENT")
active_map = aprx.activeMap
if active_map is None:
    raise RuntimeError("No active map. Activate a map and run again.")

PROJECT_HOME = aprx.homeFolder or os.path.dirname(aprx.defaultGeodatabase or "") or os.path.expanduser("~")
os.makedirs(PROJECT_HOME, exist_ok=True)
TS = datetime.now().strftime("%Y%m%d_%H%M%S")
CSV_PATH = os.path.join(PROJECT_HOME, f"PKID_Update_{TS}.csv")

def msg(s): arcpy.AddMessage(s)
def warn(s): arcpy.AddWarning(s)
def err(s): arcpy.AddError(s)

# ---------------- Traversal: deep groups + composites (ACTIVE MAP ONLY) -----
def iter_layers_deep(layer_or_map):
    """Depth-first traversal over the active map, GroupLayers, and composite layers; yields leaf layers."""
    stack = deque(layer_or_map.listLayers())
    while stack:
        lyr = stack.pop()
        # Descend into any layer that can contain children
        descended = False
        try:
            subs = lyr.listLayers()
            if subs:
                descended = True
                for sub in subs:
                    stack.append(sub)
        except Exception:
            pass
        if descended:
            continue
        yield lyr  # leaf layer

def is_concrete_feature_class(path):
    """Accept file/SDE/shapefile feature classes; reject services, joins, or tables."""
    if not path or not arcpy.Exists(path):
        return False
    try:
        d = arcpy.Describe(path)
        st = getattr(d, "shapeType", None)
        return st in ("Point", "Polyline", "Polygon", "Multipoint")
    except Exception:
        return False

def collect_unique_datasources_from_active_map():
    """
    Scan the ACTIVE MAP only. Traverse all groups and nested groups. Gather concrete FCs.
    Returns OrderedDict: catalogPath -> {'workspace':..., 'layer_names':set()}
    """
    collected = OrderedDict()
    for lyr in iter_layers_deep(active_map):
        try:
            if not hasattr(lyr, "supports") or not lyr.supports("DATASOURCE"):
                continue
            ds = lyr.dataSource
        except Exception:
            continue
        if not is_concrete_feature_class(ds):
            continue
        d = arcpy.Describe(ds)
        cat = d.catalogPath
        entry = collected.setdefault(cat, {"workspace": d.path, "layer_names": set()})
        entry["layer_names"].add(lyr.name)
    return collected

# ---------------- Field + selection helpers ---------------------------------
def find_pk_field_by_alias(dataset_path):
    """Return (field_name, field_type, field_length) for the field with alias == TARGET_ALIAS (case-insensitive)."""
    try:
        for f in arcpy.ListFields(dataset_path):
            if (f.aliasName or "").strip().lower() == TARGET_ALIAS.strip().lower():
                return f.name, f.type, getattr(f, "length", None)
    except Exception:
        pass
    return None, None, None

def build_missing_query(temp_layer, field_name, field_type):
    fld = arcpy.AddFieldDelimiters(temp_layer, field_name)
    if field_type == "GUID":
        return f"{fld} IS NULL"
    if field_type == "String":
        uppers = ", ".join(f"'{p.upper()}'" for p in PLACEHOLDERS if p is not None)
        return f"{fld} IS NULL OR {fld} = '' OR {fld} = ' ' OR UPPER({fld}) IN ({uppers})"
    return None  # unsupported type

def test_schema_lock(dataset_path):
    try:
        return arcpy.TestSchemaLock(dataset_path)
    except Exception:
        return False

# ---------------- GUID calculators ------------------------------------------
def calculate_guid_guidfield(temp_layer, field_name):
    # Prefer Arcade GUID(); fallback to Python for engines that do not support it
    try:
        arcpy.management.CalculateField(temp_layer, field_name, "GUID()", "ARCADE")
        return "Success - GUIDs calculated (Arcade GUID())"
    except Exception as ex_arcade:
        try:
            arcpy.management.CalculateField(
                temp_layer, field_name, "make_guid()", "PYTHON3",
                code_block="import uuid\ndef make_guid():\n    return str(uuid.uuid4())"
            )
            return "Success - GUIDs calculated (Python fallback)"
        except Exception as ex_py:
            return f"Failed - CalculateField error (Arcade: {ex_arcade}; Python: {ex_py})"

def calculate_guid_textfield(temp_layer, field_name, field_length):
    if field_length is not None and field_length < 36:
        return f"Skipped - Text field too short for GUID (len={field_length})"
    try:
        arcpy.management.CalculateField(
            temp_layer, field_name, "make_guid()", "PYTHON3",
            code_block="import uuid\ndef make_guid():\n    return str(uuid.uuid4())"
        )
        return "Success - GUIDs calculated (Python)"
    except Exception as ex:
        return f"Failed - CalculateField error: {ex}"

# ---------------- Main ------------------------------------------------------
def run():
    datasets = collect_unique_datasources_from_active_map()
    if not datasets:
        msg("Active map contains no eligible feature classes.")
        return

    rows = []
    msg(f"Eligible datasets in active map: {len(datasets)}")

    for cat_path, meta in datasets.items():
        d = arcpy.Describe(cat_path)
        ds_name = getattr(d, "name", os.path.basename(cat_path))
        ws = meta["workspace"]
        lyr_names = ";".join(sorted(meta["layer_names"]))

        # Schema-lock gate
        if not test_schema_lock(cat_path):
            rows.append({
                "dataset": ds_name, "catalogPath": cat_path, "workspace": ws, "layers": lyr_names,
                "fieldName": "", "fieldType": "", "fieldLength": "", "editsMade": 0,
                "status": "Skipped", "reason": "Schema lock (in edit or in use)"
            })
            warn(f"Skipped (lock): {cat_path}")
            continue

        # Resolve PK field by alias
        pk_name, pk_type, pk_len = find_pk_field_by_alias(cat_path)
        if not pk_name:
            rows.append({
                "dataset": ds_name, "catalogPath": cat_path, "workspace": ws, "layers": lyr_names,
                "fieldName": "", "fieldType": "", "fieldLength": "", "editsMade": 0,
                "status": "Skipped", "reason": f"Alias '{TARGET_ALIAS}' not found"
            })
            continue

        # Temp feature layer and selection
        tmp_lyr = arcpy.management.MakeFeatureLayer(cat_path, f"lyr_pkid_{uuid.uuid4().hex[:8]}").getOutput(0)
        where = build_missing_query(tmp_lyr, pk_name, pk_type)
        if not where:
            rows.append({
                "dataset": ds_name, "catalogPath": cat_path, "workspace": ws, "layers": lyr_names,
                "fieldName": pk_name, "fieldType": pk_type, "fieldLength": pk_len or "",
                "editsMade": 0, "status": "Skipped", "reason": f"Unsupported field type: {pk_type}"
            })
            try: arcpy.management.Delete(tmp_lyr)
            except Exception: pass
            continue

        arcpy.management.SelectLayerByAttribute(tmp_lyr, "NEW_SELECTION", where)
        try:
            count = int(arcpy.management.GetCount(tmp_lyr).getOutput(0))
        except Exception:
            count = 0

        if count <= 0:
            rows.append({
                "dataset": ds_name, "catalogPath": cat_path, "workspace": ws, "layers": lyr_names,
                "fieldName": pk_name, "fieldType": pk_type, "fieldLength": pk_len or "",
                "editsMade": 0, "status": "Skipped", "reason": "No NULL or placeholder values"
            })
            try:
                arcpy.management.SelectLayerByAttribute(tmp_lyr, "CLEAR_SELECTION")
                arcpy.management.Delete(tmp_lyr)
            except Exception:
                pass
            continue

        # Calculate GUIDs
        if pk_type == "GUID":
            status = calculate_guid_guidfield(tmp_lyr, pk_name)
        elif pk_type == "String":
            status = calculate_guid_textfield(tmp_lyr, pk_name, pk_len)
        else:
            status = f"Skipped - Unsupported field type: {pk_type}"

        rows.append({
            "dataset": ds_name, "catalogPath": cat_path, "workspace": ws, "layers": lyr_names,
            "fieldName": pk_name, "fieldType": pk_type, "fieldLength": pk_len or "",
            "editsMade": count, "status": "Success" if status.startswith("Success") else "Skipped",
            "reason": status
        })

        # Cleanup
        try:
            arcpy.management.SelectLayerByAttribute(tmp_lyr, "CLEAR_SELECTION")
            arcpy.management.Delete(tmp_lyr)
        except Exception:
            pass

    # ---------------- Write CSV summary ----------------
    fields = ["dataset","catalogPath","workspace","layers","fieldName","fieldType",
              "fieldLength","editsMade","status","reason"]
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader(); w.writerows(rows)
    msg(f"GUID assignment complete. Summary CSV: {CSV_PATH}")

if __name__ == "__main__":
    run()
