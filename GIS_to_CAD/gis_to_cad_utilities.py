# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
============================================================
Script: gis_to_cad_utilities.py
Author: Nathanael Sheean
Date: 2025-09-14
Purpose: Export Utilities layers to CAD. Only dataset names are fixed.

Behavior:
  - Finds target layers by simple dataset name anywhere in the active map.
  - Matches by TOC name or dataset basename, case-insensitive.
  - Prepares 'Layer' field; for *_L only, appends '_abandoned' when operationalStatus == 'abandoned'.
  - (Optional) For *_L only, populate up to two annotation fields (Anno1, Anno2) from chosen attributes.
  - Exports DWG to the project home folder with timestamp.

Annotation (disabled by default):
  1) Uncomment ANNO_FIELDS and list up to two source field names to annotate.
     Example: ANNO_FIELDS = ["pipeDiameter"]            # one field
              ANNO_FIELDS = ["pipeDiameter", "material"]  # two fields
  2) Only *_L feature classes are annotated.
  3) Any data type is accepted; values are stringified; nulls become empty text.
============================================================
"""

import arcpy
import os
from datetime import datetime

# ---------- Rules: simple dataset names (only hardcoded content) ----------
UTIL_TARGETS = [
    "CAccessPoint_P", "CAntenna_P", "CDuct_L",
    "EAccessPoint_P", "EAirfieldLight_P", "EExteriorLight_P", "EGenerator_P",
    "EGroundingPoint_P", "EMeter_P", "ESubstation_P", "ESurfaceStructure_P",
    "ESwitch_P", "ESwitchingStation_P", "ETransformer_P",
    "EUGPrimary_L", "EUGSecondary_L", "EAbandonedElectricLine_L",
    "OControlValve_P", "OInstallationPipeline_L", "OPump_P", "OStorageTank_P",
    "StorageTankFarm_A",
    "SCleanOut_P", "SFitting_P", "SGravityMain_L", "SGreaseTrap_P",
    "SLateralLine_L", "SManhole_P", "SNetworkStructure_A", "SNetworkStructure_P",
    "SPressurizedMain_L", "SPump_P", "SSepticTank_A", "SSepticTank_P", "SValve_P",
    "SwCulvert_L", "SwDetention_A", "SwDischargePoint_P", "SwForceMain_L",
    "SwGravityLine_L", "SwInlet_P", "SwManhole_P", "SwOpenDrainage_L",
    "SwPump_P", "SwPumpStation_A", "SwStorageReservoir_A", "SwValve_P",
    "WControlValve_P", "Well_P", "WFireHydrant_P", "WFitting_P", "WMainLine_L",
    "WMeterPoint_P", "WServiceLine_L", "WSource_P", "WStorageTank_P",
]

# ---------------- OPTIONAL ANNOTATION CONFIG (leave commented to disable) ----------------
# Enter up to two source field names to copy into text annotation fields (Anno1/Anno2)
# ANNO_FIELDS = ["pipeDiameter"]                    # example: one field
# ANNO_FIELDS = ["pipeDiameter", "material"]        # example: two fields
ANNO_FIELDS = []  # keep empty to disable annotation

# ------------------------ Helpers -------------------------
def _project_context():
    aprx = arcpy.mp.ArcGISProject("CURRENT")
    home = aprx.homeFolder or os.getcwd()
    return aprx, home

def _iter_layers_depth_first(layer):
    if getattr(layer, "isGroupLayer", False):
        for child in layer.listLayers():
            yield from _iter_layers_depth_first(child)
        return
    try:
        subs = layer.listLayers()
        if subs:
            for child in subs:
                yield from _iter_layers_depth_first(child)
            return
    except Exception:
        pass
    if getattr(layer, "isFeatureLayer", False):
        yield layer

def _all_feature_layers(map_obj):
    for top in map_obj.listLayers():
        yield from _iter_layers_depth_first(top)

def _catalog_path(lyr):
    try:
        if hasattr(lyr, "dataSource") and lyr.dataSource:
            return lyr.dataSource
    except Exception:
        pass
    try:
        return arcpy.Describe(lyr).catalogPath
    except Exception:
        return None

def _basename_no_ext(path):
    if not path:
        return ""
    base = os.path.basename(path)
    name, _ext = os.path.splitext(base)
    if "." in name:
        name = name.split(".")[-1]
    return name

def _resolve_by_names(map_obj, simple_names):
    wanted = {n.strip().lower() for n in simple_names if n and n.strip()}
    found_paths, seen = [], set()
    for lyr in _all_feature_layers(map_obj):
        toc = (lyr.name or "").strip().lower()
        cat = _catalog_path(lyr)
        base = _basename_no_ext(cat).lower()
        if not cat:
            continue
        if toc in wanted or base in wanted:
            key = cat.lower()
            if key not in seen and arcpy.Exists(cat):
                seen.add(key)
                found_paths.append(cat)
    return found_paths

def _ensure_text_field(fc, name, length=255):
    existing = {f.name.lower(): f for f in arcpy.ListFields(fc)}
    if name.lower() not in existing:
        arcpy.management.AddField(fc, name, "TEXT", field_length=length)

def _prepare_layer_field(feature_class, status_field="operationalStatus"):
    """
    Populate 'Layer' with dataset name.
    For *_L datasets only, append '_abandoned' when operationalStatus == 'abandoned'.
    """
    desc = arcpy.Describe(feature_class)
    dataset_name = getattr(desc, "name", os.path.basename(feature_class))
    fields = {f.name for f in arcpy.ListFields(feature_class)}
    if "Layer" not in fields:
        arcpy.management.AddField(feature_class, "Layer", "TEXT", field_length=255)

    use_status = dataset_name.endswith("_L") and (status_field in fields)
    field_list = ["Layer", status_field] if use_status else ["Layer"]

    with arcpy.da.UpdateCursor(feature_class, field_list) as cursor:
        for row in cursor:
            if use_status:
                val = row[1]
                if val is not None and str(val).strip().lower() == "abandoned":
                    row[0] = f"{dataset_name}_abandoned"
                else:
                    row[0] = dataset_name
            else:
                row[0] = dataset_name
            cursor.updateRow(row)

def _populate_annotation_fields(feature_class, anno_sources):
    """
    For *_L only: populate Anno1/Anno2 from anno_sources (max 2).
    - Creates Anno1/Anno2 TEXT fields if missing.
    - Reads source fields if present; writes '' when missing or null.
    - Stringifies any data type safely.
    """
    desc = arcpy.Describe(feature_class)
    dataset_name = getattr(desc, "name", os.path.basename(feature_class))
    if not dataset_name.endswith("_L"):
        return  # annotation restricted to lines only

    # Enforce max 2 and normalize list
    anno_sources = [s for s in (anno_sources or []) if s and s.strip()]
    if not anno_sources:
        return
    anno_sources = anno_sources[:2]

    # Ensure destination fields
    _ensure_text_field(feature_class, "Anno1", 255)
    if len(anno_sources) > 1:
        _ensure_text_field(feature_class, "Anno2", 255)

    # Build cursor schema based on available source fields
    fields_in_fc = {f.name.lower(): f.name for f in arcpy.ListFields(feature_class)}
    src1 = fields_in_fc.get(anno_sources[0].lower())
    src2 = fields_in_fc.get(anno_sources[1].lower()) if len(anno_sources) > 1 else None

    # Compose update field list
    update_fields = ["Anno1"] + (["Anno2"] if src2 else [])
    read_fields = ([src1] if src1 else [None]) + ([src2] if src2 else [])
    # If a source field is missing, we will just write empty strings.

    # Build cursor: include only existing sources
    cursor_fields = []
    if src1: cursor_fields.append(src1)
    if src2: cursor_fields.append(src2)
    cursor_fields = update_fields + cursor_fields  # dest first, then sources

    # If no sources exist at all, still clear Anno1/Anno2 to ''
    if not src1 and not src2:
        with arcpy.da.UpdateCursor(feature_class, update_fields) as uc:
            for row in uc:
                # Clear to empty text
                row[0] = ""
                if len(update_fields) > 1:
                    row[1] = ""
                uc.updateRow(row)
        return

    # Mixed case: handle present/missing sources uniformly
    with arcpy.da.UpdateCursor(feature_class, cursor_fields) as uc:
        for row in uc:
            # row indices: 0->Anno1, 1->Anno2 (if present), then sources...
            dest_anno1_idx = 0
            dest_anno2_idx = 1 if len(update_fields) > 1 else None
            src_vals_start = len(update_fields)

            # src1 value
            if src1:
                v1 = row[src_vals_start]
                row[dest_anno1_idx] = "" if v1 is None else str(v1)
            else:
                row[dest_anno1_idx] = ""

            # src2 value
            if dest_anno2_idx is not None:
                if src2:
                    v2 = row[src_vals_start + (1 if src1 else 0)]
                    row[dest_anno2_idx] = "" if v2 is None else str(v2)
                else:
                    row[dest_anno2_idx] = ""

            uc.updateRow(row)

# ------------------------ Main ----------------------------
def run():
    aprx, home = _project_context()
    m = aprx.activeMap
    if m is None:
        arcpy.AddWarning("No active map is open.")
        return

    inputs = _resolve_by_names(m, UTIL_TARGETS)
    if not inputs:
        arcpy.AddWarning("No target datasets were found in the active map.")
        return

    # Prepare fields and optional annotation for each dataset
    for fc in inputs:
        _prepare_layer_field(fc, status_field="operationalStatus")
        # Optional annotation for *_L only; enable by setting ANNO_FIELDS above
        if ANNO_FIELDS:
            _populate_annotation_fields(fc, ANNO_FIELDS)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dwg = os.path.join(home, f"{BASE_TAG}_Utility_Map_{timestamp}.dwg")
    arcpy.conversion.ExportCAD(
        in_features=";".join(inputs),
        Output_Type="DWG_R2018",
        Output_File=out_dwg,
        Ignore_FileNames="Ignore_Filenames_in_Tables",
        Append_To_Existing="Overwrite_Existing_Files",
        Seed_File=None
    )
    arcpy.AddMessage(f"Utilities export complete: {out_dwg}")

if __name__ == "__main__":
    run()
