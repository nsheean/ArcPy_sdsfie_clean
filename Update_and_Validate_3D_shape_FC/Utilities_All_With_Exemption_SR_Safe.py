# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
===================================================================================
Script: Utilities_All_With_Exemption_SR_Safe.py
Author: Nathanael Sheean
Date: 2025-09-14
Version: 2.0.1 (Cross-user QC)

Purpose:
  Update Z-values for UTILITY INFRASTRUCTURE using configured surface rasters.
  Apply an explicit exemption for a defined utility line.
  Produce audit-grade logs for QA/QC across users and projects.

Scope (group-scoped, nested-group aware):
  - Processes ONLY layers contained in the group '{BASE_CODE}_UTM40N_Utilities' and all subgroups.
  - Ignores layers outside this group.

Required rasters (LiDAR-derived DEMs; naming in centimeters for clarity, data in meters):
  - {BASE_CODE}_DEM_10cm
    Meaning: base DEM built from LiDAR. The name encodes 10 cm cell size for re{BASE_CODE}ility.
    Units: the dataset and all calculations use meters.
  - {BASE_CODE}_DEM_10cm_PLUS_60cm
    Meaning: base DEM with a vertical offset of +0.60 meters applied to Z values.
    Units: meters. The “PLUS_60cm” suffix is a label only.
  - {BASE_CODE}_DEM_10cm_MINUS_60cm
    Meaning: base DEM with a vertical offset of −0.60 meters applied to Z values.
    Units: meters. The “MINUS_60cm” suffix is a label only.

Raster naming convention and renaming rules:
  - Use centimeters in names for human re{BASE_CODE}ility. Keep all data and math in meters.
  - If you generate different products, rename the layers to match the real values.
    Do not change code units.
    Examples:
      • Base at 50 cm cell size:           {BASE_CODE}_DEM_50cm          (data still in meters)
      • Raise Z by 0.60 m:                 {BASE_CODE}_DEM_50cm_PLUS_60cm
      • Lower Z by 1.00 m:                 {BASE_CODE}_DEM_50cm_MINUS_100cm
  - Update these constants to match your chosen names:
      RASTER_BASE_NAME, RASTER_PLUS60_NAME, RASTER_MINUS60_NAME

Routing rules:
  - Feature class suffix *_P or *_A → use BASE DEM (e.g., {BASE_CODE}_DEM_10cm)
  - Feature class suffix *_L       → use MINUS_60cm DEM (e.g., {BASE_CODE}_DEM_10cm_MINUS_60cm)
  - EXEMPTION (applies ONLY to OInstallationPipeline_L):
      facilityNumber = '23325'     → use PLUS_60cm DEM (e.g., {BASE_CODE}_DEM_10cm_PLUS_60cm)
      else (or NULL)               → use MINUS_60cm DEM

Safety:
  - Requires 3D Analyst. Checks out/in license.
  - Honors nested groups; validates Z-awareness before updates.
  - Geographic transformation is scoped per operation; no global env side effects.
  - No deletes, no appends, no temp writes to sources.

QC Logging (written to project home folder; fallback to arcpy.env.scratchFolder):
  1) <stamp>_layers_resolved.csv     — inventory of resolved layers
  2) <stamp>_updates_applied.csv     — each update attempt, route, raster, counts, status
  3) <stamp>_exemptions_applied.csv  — OIDs updated via exemption route
  4) <stamp>_Utilities_All_With_Exemption_SR_Safe.log — human-re{BASE_CODE}le log

Separation of concerns:
  - This script is for UTILITY INFRASTRUCTURE only under '{BASE_CODE}_UTM40N_Utilities'.
  - CIP layers run in a separate file: 'Main_PA_To_10cm_SR_Safe.py'.
  - Do not co-mingle CIP layers in the Utilities group.

Usage:
  - Ensure the group contains the three DEM rasters with names matching the convention above.
  - Open the project in ArcGIS Pro; run this script in the CURRENT context.
  - Review the CSVs and log for QA/QC.
===================================================================================
"""

import arcpy
import os
import csv
import datetime
import traceback
from contextlib import contextmanager

# -------------------------- Configuration --------------------------
GROUP_NAME            = "{BASE_CODE}_UTM40N_Utilities"

# Required raster display names (exact or prefix match; case-insensitive)
RASTER_BASE_NAME      = "{BASE_CODE}_DEM_10cm"
RASTER_MINUS60_NAME   = "{BASE_CODE}_DEM_10cm_MINUS_60cm"
RASTER_PLUS60_NAME    = "{BASE_CODE}_DEM_10cm_PLUS_60cm"

# Exemption details (applies only to OInstallationPipeline_L)
EXEMPT_LAYER_NAME     = "OInstallationPipeline_L"
EXEMPT_FIELD_NAME     = "facilityNumber"
EXEMPT_FIELD_VALUE    = "23325"

# Geographic transformation to use when projecting on-the-fly (string or empty).
GEOGRAPHIC_TRANSFORM  = ""

# -------------------------- Utilities ------------------------------
def _now_stamp():
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

def _project_context():
    aprx = arcpy.mp.ArcGISProject("CURRENT")
    home = aprx.homeFolder
    if not home or not os.path.isdir(home):
        home = arcpy.env.scratchFolder or os.getcwd()
    return aprx, home

def _log_paths(home, stamp):
    base = os.path.join(home, stamp + "_Utilities_All_With_Exemption_SR_Safe")
    return {
        "layers":        base + "_layers_resolved.csv",
        "updates":       base + "_updates_applied.csv",
        "exemptions":    base + "_exemptions_applied.csv",
        "textlog":       base + ".log",
    }

def _open_csv(path, header):
    fh = open(path, "w", newline="", encoding="utf-8")
    w = csv.DictWriter(fh, fieldnames=header)
    w.writeheader()
    return fh, w

def _add_msg(msg):  arcpy.AddMessage(msg)
def _add_warn(msg): arcpy.AddWarning(msg)
def _add_err(msg):  arcpy.AddError(msg)

def _find_group(map_obj, name_ci):
    key = (name_ci or "").strip().lower()
    for lyr in map_obj.listLayers():
        found = _find_group_recursive(lyr, key)
        if found:
            return found
    raise RuntimeError(f"Group '{GROUP_NAME}' not found in active map.")

def _find_group_recursive(layer, key_lower):
    if getattr(layer, "isGroupLayer", False):
        if (layer.name or "").strip().lower() == key_lower:
            return layer
        for child in layer.listLayers():
            found = _find_group_recursive(child, key_lower)
            if found:
                return found
    return None

def _iter_feature_layers(group_layer):
    # Depth-first feature layer enumeration under a group
    for child in group_layer.listLayers():
        if getattr(child, "isGroupLayer", False):
            for sub in _iter_feature_layers(child):
                yield sub
        else:
            # Some composite layers expose listLayers()
            try:
                subs = child.listLayers()
                if subs:
                    for sub in subs:
                        for x in _iter_feature_layers(sub):
                            yield x
                    continue
            except Exception:
                pass
            if getattr(child, "isFeatureLayer", False):
                yield child

def _catalog_path(layer_or_path):
    try:
        if hasattr(layer_or_path, "dataSource") and layer_or_path.dataSource:
            return layer_or_path.dataSource
    except Exception:
        pass
    try:
        return arcpy.Describe(layer_or_path).catalogPath
    except Exception:
        return None

def _basename_no_ext(path_or_name):
    if not path_or_name:
        return ""
    base = os.path.basename(path_or_name)
    name, _ext = os.path.splitext(base)
    if "." in name:  # strip schema owner
        name = name.split(".")[-1]
    return name

def _shape_type(ds):
    try:
        return (arcpy.Describe(ds).shapeType or "").lower()  # 'point','polyline','polygon','multipoint'
    except Exception:
        return ""

def _has_z(ds):
    try:
        return bool(getattr(arcpy.Describe(ds), "hasZ", False))
    except Exception:
        return False

def _resolve_field(ds, field_name):
    fields = {f.name.lower(): f.name for f in arcpy.ListFields(ds)}
    return fields.get(field_name.lower())

def _make_feature_layer(src, name, where=None):
    lyr = arcpy.management.MakeFeatureLayer(src, name, where).getOutput(0)
    return lyr

def _collect_rasters_recursive(layer, bag):
    try:
        d = arcpy.Describe(layer)
        dt = getattr(d, "dataType", "")
        if dt in ("RasterLayer", "MosaicLayer"):
            bag.append(arcpy.Raster(d.catalogPath))
        if getattr(layer, "isGroupLayer", False):
            for c in layer.listLayers():
                _collect_rasters_recursive(c, bag)
        else:
            subs = layer.listLayers()
            if subs:
                for c in subs:
                    _collect_rasters_recursive(c, bag)
    except Exception:
        pass

def _resolve_raster_in_group(group_layer, display_name):
    """
    Resolve a raster under the given group by:
      1) exact case-insensitive name match
      2) prefix match
    Returns an arcpy.Raster (ready to use).
    """
    key = display_name.strip().lower()
    rasters = []
    for top in group_layer.listLayers():
        _collect_rasters_recursive(top, rasters)

    # Exact match
    for r in rasters:
        nm = (r.name or "").strip()
        if nm.lower() == key:
            return r
    # Prefix match
    for r in rasters:
        nm = (r.name or "").strip().lower()
        if nm.startswith(key):
            return r
    raise RuntimeError(f"Required raster '{display_name}' not found in group '{GROUP_NAME}'.")

@contextmanager
def _scoped_env(geo_transform):
    saved_gt = arcpy.env.geographicTransformations
    try:
        if geo_transform:
            arcpy.env.geographicTransformations = geo_transform
        yield
    finally:
        arcpy.env.geographicTransformations = saved_gt

def _suffix_route(layer_name):
    nm = (layer_name or "").strip().lower()
    if nm.endswith("_l"): return "LINE"
    if nm.endswith("_p"): return "POINT"
    if nm.endswith("_a"): return "AREA"
    return "UNKNOWN"

# -------------------------- Main ------------------------------
def run():
    # Context
    aprx, home = _project_context()
    m = aprx.activeMap
    if m is None:
        _add_err("No active map detected. Open a map and try again.")
        return

    stamp = _now_stamp()
    paths = _log_paths(home, stamp)

    # Log files
    layers_fh, layers_csv = _open_csv(paths["layers"], [
        "layer_name", "dataset_basename", "catalog_path", "geometry", "has_z"
    ])
    updates_fh, updates_csv = _open_csv(paths["updates"], [
        "layer_name", "route", "where_clause", "raster_used",
        "attempted_count", "status", "message"
    ])
    exempt_fh, exempt_csv = _open_csv(paths["exemptions"], [
        "layer_name", "oid_field", "OBJECTID"
    ])
    log_fh = open(paths["textlog"], "w", encoding="utf-8")
    def _wlog(line): log_fh.write(line + "\n"); log_fh.flush()

    # License
    try:
        status = arcpy.CheckExtension("3D")
        if status != "Available":
            raise RuntimeError(f"3D Analyst extension not available: {status}")
        arcpy.CheckOutExtension("3D")
    except Exception as e:
        _add_err(str(e)); _wlog(f"[ERROR] License: {e}")
        layers_fh.close(); updates_fh.close(); exempt_fh.close(); log_fh.close()
        return

    try:
        # Resolve group
        group = _find_group(m, GROUP_NAME)

        # Resolve rasters
        base_ras   = _resolve_raster_in_group(group, RASTER_BASE_NAME)
        minus_ras  = _resolve_raster_in_group(group, RASTER_MINUS60_NAME)
        plus_ras   = _resolve_raster_in_group(group, RASTER_PLUS60_NAME)

        _wlog(f"[INFO] Rasters -> BASE='{base_ras.name}', MINUS='{minus_ras.name}', PLUS='{plus_ras.name}'")
        if GEOGRAPHIC_TRANSFORM:
            _wlog(f"[INFO] Geographic Transformation: '{GEOGRAPHIC_TRANSFORM}'")
        else:
            _wlog("[INFO] Geographic Transformation: <default/none>")

        # Enumerate feature layers under group
        for lyr in _iter_feature_layers(group):
            cat = _catalog_path(lyr)
            if not cat or not arcpy.Exists(cat):
                _wlog(f"[WARN] Skipping (no catalog): {lyr.name}"); continue

            base = _basename_no_ext(cat)
            geom = _shape_type(lyr)
            hasz = _has_z(lyr)

            # Record discovery
            layers_csv.writerow({
                "layer_name": lyr.name, "dataset_basename": base,
                "catalog_path": cat, "geometry": geom, "has_z": hasz
            })

            # Route decision
            route = _suffix_route(lyr.name)
            if route == "UNKNOWN":
                _wlog(f"[INFO] Skip (unsupported suffix): {lyr.name}")
                continue

            # Z-awareness gate
            if not hasz:
                msg = "not_z_aware; no update attempted"
                updates_csv.writerow({
                    "layer_name": lyr.name, "route": route, "where_clause": "",
                    "raster_used": "", "attempted_count": 0,
                    "status": "skipped", "message": msg
                })
                _wlog(f"[INFO] {lyr.name}: {msg}")
                continue

            # Pick raster by route
            def _raster_for(route_key):
                return {"POINT": base_ras, "AREA": base_ras, "LINE": minus_ras}[route_key]

            # Special exemption for OInstallationPipeline_L
            if base.lower() == EXEMPT_LAYER_NAME.lower() and route == "LINE":
                fld = _resolve_field(lyr, EXEMPT_FIELD_NAME)
                if not fld:
                    _wlog(f"[WARN] Exemption field '{EXEMPT_FIELD_NAME}' missing in {lyr.name}; treating all as non-exempt.")
                    _apply_update(src=lyr, route="LINE", raster=minus_ras, where_clause="",
                                  updates_csv=updates_csv, log=_wlog)
                else:
                    wc_exempt = f"{arcpy.AddFieldDelimiters(lyr, fld)} = '{EXEMPT_FIELD_VALUE}'"
                    wc_non    = f"({arcpy.AddFieldDelimiters(lyr, fld)} <> '{EXEMPT_FIELD_VALUE}' OR {arcpy.AddFieldDelimiters(lyr, fld)} IS NULL)"

                    # Apply to exempt subset
                    count_exempt = _apply_update(src=lyr, route="LINE", raster=plus_ras, where_clause=wc_exempt,
                                                 updates_csv=updates_csv, log=_wlog)
                    # Log OIDs for exempted features (if any)
                    if count_exempt > 0:
                        oid_name = arcpy.Describe(lyr).OIDFieldName
                        tmp = _make_feature_layer(lyr, f"exempt_{_now_stamp()}", wc_exempt)
                        with arcpy.da.SearchCursor(tmp, [oid_name]) as sc:
                            for (oid,) in sc:
                                exempt_csv.writerow({"layer_name": lyr.name, "oid_field": oid_name, "OBJECTID": oid})
                        arcpy.management.Delete(tmp)

                    # Apply to non-exempt subset
                    _apply_update(src=lyr, route="LINE", raster=minus_ras, where_clause=wc_non,
                                  updates_csv=updates_csv, log=_wlog)
                continue  # done with exemption layer

            # Standard routing (non-exempt)
            raster = _raster_for(route)
            _apply_update(src=lyr, route=route, raster=raster, where_clause="",
                          updates_csv=updates_csv, log=_wlog)

        _add_msg("Utilities Z-update processing complete.")
        _add_msg(f"Logs written:\n  {paths['layers']}\n  {paths['updates']}\n  {paths['exemptions']}\n  {paths['textlog']}")

    except Exception as e:
        tb = traceback.format_exc()
        _add_err(f"Fatal error: {e}")
        _wlog("[ERROR] " + str(e))
        _wlog(tb)
    finally:
        try:
            arcpy.CheckInExtension("3D")
        except Exception:
            pass
        layers_fh.close(); updates_fh.close(); exempt_fh.close(); log_fh.close()


def _apply_update(src, route, raster, where_clause, updates_csv, log):
    """
    Apply UpdateFeatureZ_3d to 'src' (layer object), with an optional where clause.
    Returns attempted record count for logging.
    """
    # Build a clean layer instance honoring optional where clause
    lyr = _make_feature_layer(src, f"u_{_now_stamp()}", where_clause if where_clause else None)

    # Count attempted features
    try:
        attempted = int(arcpy.management.GetCount(lyr).getOutput(0))
    except Exception:
        attempted = 0

    raster_name = getattr(raster, "name", "")

    if attempted == 0:
        updates_csv.writerow({
            "layer_name": getattr(src, "name", "<unnamed>"),
            "route": route,
            "where_clause": where_clause,
            "raster_used": raster_name,
            "attempted_count": attempted,
            "status": "skipped",
            "message": "selection_empty"
        })
        log(f"[INFO] {getattr(src,'name','<unnamed>')}: selection empty; route={route}; raster={raster_name}")
        arcpy.management.Delete(lyr)
        return 0

    # Geometry guard (tool supports z-aware points/lines/polys)
    geom = _shape_type(lyr)
    if geom not in ("point", "polyline", "polygon", "multipoint"):
        updates_csv.writerow({
            "layer_name": getattr(src, "name", "<unnamed>"),
            "route": route,
            "where_clause": where_clause,
            "raster_used": raster_name,
            "attempted_count": attempted,
            "status": "skipped",
            "message": f"unsupported_geometry:{geom}"
        })
        log(f"[INFO] {getattr(src,'name','<unnamed>')}: unsupported geometry '{geom}'")
        arcpy.management.Delete(lyr)
        return attempted

    # Scoped transformation (no global side effects)
    try:
        with _scoped_env(GEOGRAPHIC_TRANSFORM):
            arcpy.ddd.UpdateFeatureZ(in_surface=raster, in_features=lyr)
        updates_csv.writerow({
            "layer_name": getattr(src, "name", "<unnamed>"),
            "route": route,
            "where_clause": where_clause,
            "raster_used": raster_name,
            "attempted_count": attempted,
            "status": "updated",
            "message": ""
        })
        log(f"[OK] {getattr(src,'name','<unnamed>')}: updated {attempted} features; route={route}; raster={raster_name}")
    except Exception as e:
        updates_csv.writerow({
            "layer_name": getattr(src, "name", "<unnamed>"),
            "route": route,
            "where_clause": where_clause,
            "raster_used": raster_name,
            "attempted_count": attempted,
            "status": "error",
            "message": str(e)
        })
        log(f"[ERR] {getattr(src,'name','<unnamed>')}: {e}")
    finally:
        arcpy.management.Delete(lyr)

    return attempted


if __name__ == "__main__":
    run()
