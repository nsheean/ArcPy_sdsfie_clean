# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
============================================================
Script: gis_to_cad_cip.py
Author: Nathanael Sheean
Date: 2025-09-14
Purpose: Export CIP layers to CAD. Only dataset names are fixed.
Behavior:
  - Finds target layers by simple dataset name anywhere in the active map.
  - Matches by TOC name or dataset basename, case-insensitive.
  - Prepares 'Layer' field; appends '_abandoned' when operationalStatus == 'abandoned'.
  - Exports DWG to the project home folder with timestamp.
============================================================
"""

import arcpy
import os
from datetime import datetime

# ---------- Rules: simple dataset names (only hardcoded content) ----------
CIP_TARGETS = [
    "Wall_L", "Berm_A", "PavementSlab_A", "Building_A", "CAnchorGuy_L",
    "CAnchorGuy_P", "FlagPole_P", "Structure_A", "Tower_A", "Tower_P",
    "RecreationFeature_A", "AccessControl_L", "AccessControl_P", "Barricade_L",
    "Barricade_P", "Fence_L", "Airfield_A", "Airfield_L", "ArrestingGear_L",
    "Bridge_A", "Curb_L", "Guardrail_L", "Gutter_A", "PavementMarking_A",
    "PavementMarking_L", "RailTrack_L", "RoadCenterline_L", "Roadway_A",
    "Sidewalk_A", "VehicleParking_A", "WindSock_P", "WPumpStation_A",
    "OPumpStation_A",
]

def _project_context():
    aprx = arcpy.mp.ArcGISProject("CURRENT")
    home = aprx.homeFolder or os.getcwd()
    return aprx, home

def _iter_layers_depth_first(layer):
    if getattr(layer, "isGroupLayer", False):
        for child in layer.listLayers():
            yield from _iter_layers_depth_first(child); return
    try:
        subs = layer.listLayers()
        if subs:
            for child in subs:
                yield from _iter_layers_depth_first(child); return
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
    name, _ = os.path.splitext(base)
    if "." in name:
        name = name.split(".")[-1]
    return name

def _resolve_by_names(map_obj, simple_names):
    wanted = {n.strip().lower() for n in simple_names if n and n.strip()}
    found, seen = [], set()
    for lyr in _all_feature_layers(map_obj):
        toc = (lyr.name or "").strip().lower()
        cat = _catalog_path(lyr)
        base = _basename_no_ext(cat).lower()
        if not cat:
            continue
        if toc in wanted or base in wanted:
            key = cat.lower()
            if key not in seen and arcpy.Exists(cat):
                seen.add(key); found.append(cat)
    return found

def _prepare_layer_field(feature_class, status_field="operationalStatus"):
    desc = arcpy.Describe(feature_class)
    dataset_name = getattr(desc, "name", os.path.basename(feature_class))
    fields = {f.name for f in arcpy.ListFields(feature_class)}
    if "Layer" not in fields:
        arcpy.management.AddField(feature_class, "Layer", "TEXT", field_length=255)
    use_status = status_field in fields
    field_list = ["Layer", status_field] if use_status else ["Layer"]
    with arcpy.da.UpdateCursor(feature_class, field_list) as cur:
        for row in cur:
            if use_status and row[1] is not None and str(row[1]).strip().lower() == "abandoned":
                row[0] = f"{dataset_name}_abandoned"
            else:
                row[0] = dataset_name
            cur.updateRow(row)

def run():
    aprx, home = _project_context()
    m = aprx.activeMap
    if m is None:
        arcpy.AddWarning("No active map is open."); return
    inputs = _resolve_by_names(m, CIP_TARGETS)
    if not inputs:
        arcpy.AddWarning("No target datasets were found in the active map."); return
    for fc in inputs:
        _prepare_layer_field(fc, status_field="operationalStatus")
    out_dwg = os.path.join(home, f"{BASE_TAG}_Base_Map_{datetime.now().strftime('%Y%m%d_%H%M%S')}.dwg")
    arcpy.conversion.ExportCAD(";".join(inputs), "DWG_R2018", out_dwg,
                               "Ignore_Filenames_in_Tables", "Overwrite_Existing_Files", None)
    arcpy.AddMessage(f"CIP export complete: {out_dwg}")

if __name__ == "__main__":
    run()
