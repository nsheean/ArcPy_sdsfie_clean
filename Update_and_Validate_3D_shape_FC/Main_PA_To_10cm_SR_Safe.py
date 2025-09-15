# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
===================================================================================
Script: Main_PA_To_10cm_SR_Safe.py
Author: Nathanael Sheean
Date: 2025-09-15
Version: 2.1.0 (Curve-Safe, XY-Identity Audit)

Purpose:
  Update Z-values for CIP layers using a LiDAR-derived base DEM only.
  Route all *_P, *_A, *_L features to the base surface. No offset surfaces here.
  Preserve geometry shape. Update Z only.

Scope (group-scoped, nested-group aware):
  - Processes ONLY layers contained in the group '{BASE_CODE}_UTM40N' and all subgroups.
  - Ignores layers outside this group.

Required raster (LiDAR-derived DEM; naming in centimeters for clarity, data in meters):
  - {BASE_CODE}_DEM_10cm

Routing rules (CIP only):
  - Feature class suffixes *_P, *_A, *_L → use BASE DEM.

Safety:
  - Requires 3D Analyst. Checks out/in license.
  - Honors nested groups; validates Z-awareness before updates.
  - Skips any dataset or feature that contains true curves to avoid linearization.
  - XY identity audit for non-curve polylines and polygons. Part and vertex counts must match.
  - Geographic transformation is scoped per operation. No global env side effects.
  - No deletes, no appends, no temp writes to sources.

QC Logging (written to project home folder; fallback to arcpy.env.scratchFolder):
  1) <stamp>_main_layers_resolved.csv   — inventory of resolved CIP layers
  2) <stamp>_main_updates_applied.csv   — each update attempt, raster, counts, status, audit
  3) <stamp>_Main_PA_To_10cm_SR_Safe.log — human-readable log

Separation of concerns:
  - This script targets CIP layers under '{BASE_CODE}_UTM40N'.
  - Utilities with vertical offsets and exemptions run in:
      'Utilities_All_With_Exemption_SR_Safe.py'
===================================================================================
"""

import arcpy
import os
import csv
import datetime
import traceback
from contextlib import contextmanager

# -------------------------- Configuration --------------------------
GROUP_NAME           = "{BASE_CODE}_UTM40N"
RASTER_BASE_NAME     = "{BASE_CODE}_DEM_10cm"
GEOGRAPHIC_TRANSFORM = ""   # e.g., "WGS_1984_(ITRF00)_To_NAD_1983"

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
    base = os.path.join(home, stamp + "_Main_PA_To_10cm_SR_Safe")
    return {
        "layers":  base + "_main_layers_resolved.csv",
        "updates": base + "_main_updates_applied.csv",
        "textlog": base + ".log",
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
    if "." in name:
        name = name.split(".")[-1]
    return name

def _shape_type(ds):
    try:
        return (arcpy.Describe(ds).shapeType or "").lower()
    except Exception:
        return ""

def _has_z(ds):
    try:
        return bool(getattr(arcpy.Describe(ds), "hasZ", False))
    except Exception:
        return False

def _dataset_has_curves(ds):
    try:
        return bool(getattr(arcpy.Describe(ds), "hasCurves", False))
    except Exception:
        return False

def _make_feature_layer_from_layer(layer_obj, name, where=None):
    # Wrap the existing map layer so we carry selection and definition query forward
    return arcpy.management.MakeFeatureLayer(layer_obj, name, where).getOutput(0)

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
    key = display_name.strip().lower()
    rasters = []
    for top in group_layer.listLayers():
        _collect_rasters_recursive(top, rasters)
    for r in rasters:
        nm = (r.name or "").strip()
        if nm.lower() == key:
            return r
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

def _suffix_supported(layer_name):
    nm = (layer_name or "").strip().lower()
    return nm.endswith(("_p", "_a", "_l"))

# -------------------------- Main ------------------------------
def run():
    aprx, home = _project_context()
    m = aprx.activeMap
    if m is None:
        _add_err("No active map detected. Open a map and try again.")
        return

    stamp = _now_stamp()
    paths = _log_paths(home, stamp)

    layers_fh, layers_csv = _open_csv(paths["layers"], [
        "layer_name", "dataset_basename", "catalog_path", "geometry",
        "has_z", "dataset_has_curves"
    ])
    updates_fh, updates_csv = _open_csv(paths["updates"], [
        "layer_name", "route", "raster_used",
        "attempted_count", "updated_count",
        "status", "message",
        "audit_part_count_before", "audit_part_count_after",
        "audit_vertex_count_before", "audit_vertex_count_after"
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
        layers_fh.close(); updates_fh.close(); log_fh.close()
        return

    try:
        group = _find_group(m, GROUP_NAME)
        base_ras = _resolve_raster_in_group(group, RASTER_BASE_NAME)

        _wlog(f"[INFO] Base DEM: '{base_ras.name}'")
        if GEOGRAPHIC_TRANSFORM:
            _wlog(f"[INFO] Geographic Transformation: '{GEOGRAPHIC_TRANSFORM}'")
        else:
            _wlog("[INFO] Geographic Transformation: <default/none>")

        for lyr in _iter_feature_layers(group):
            cat = _catalog_path(lyr)
            if not cat or not arcpy.Exists(cat):
                _wlog(f"[WARN] Skipping (no catalog): {lyr.name}")
                continue

            base = _basename_no_ext(cat)
            geom = _shape_type(lyr)
            hasz = _has_z(lyr)
            ds_has_curves = _dataset_has_curves(cat)

            layers_csv.writerow({
                "layer_name": lyr.name,
                "dataset_basename": base,
                "catalog_path": cat,
                "geometry": geom,
                "has_z": hasz,
                "dataset_has_curves": ds_has_curves
            })

            if not _suffix_supported(lyr.name):
                _wlog(f"[INFO] Skip (unsupported suffix): {lyr.name}")
                continue

            if not hasz:
                msg = "not_z_aware; no update attempted"
                updates_csv.writerow({
                    "layer_name": lyr.name, "route": "BASE",
                    "raster_used": base_ras.name, "attempted_count": 0,
                    "updated_count": 0, "status": "skipped",
                    "message": msg,
                    "audit_part_count_before": "", "audit_part_count_after": "",
                    "audit_vertex_count_before": "", "audit_vertex_count_after": ""
                })
                _wlog(f"[INFO] {lyr.name}: {msg}")
                continue

            if ds_has_curves and geom in ("polyline", "polygon"):
                msg = "dataset_contains_true_curves; skipped to preserve geometry"
                updates_csv.writerow({
                    "layer_name": lyr.name, "route": "BASE",
                    "raster_used": base_ras.name, "attempted_count": 0,
                    "updated_count": 0, "status": "skipped_curves",
                    "message": msg,
                    "audit_part_count_before": "", "audit_part_count_after": "",
                    "audit_vertex_count_before": "", "audit_vertex_count_after": ""
                })
                _wlog(f"[INFO] {lyr.name}: {msg}")
                continue

            _apply_update(
                src_layer=lyr,
                raster=base_ras,
                geom_type=geom,
                updates_csv=updates_csv,
                log=_wlog
            )

        _add_msg("CIP Z-update processing complete.")
        _add_msg(f"Logs written:\n  {paths['layers']}\n  {paths['updates']}\n  {paths['textlog']}")

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
        layers_fh.close(); updates_fh.close(); log_fh.close()

def _feature_counts(lyr, geom_type):
    """Return part_count, vertex_count for current selection."""
    part_count = 0
    vertex_count = 0
    if geom_type in ("point", "multipoint"):
        try:
            c = int(arcpy.management.GetCount(lyr).getOutput(0))
            part_count = c
            vertex_count = c
        except Exception:
            pass
        return part_count, vertex_count

    fields = ["SHAPE@"]  # safe and fast
    with arcpy.da.SearchCursor(lyr, fields) as cur:
        for (shape,) in cur:
            if shape is None:
                continue
            try:
                part_count += len(shape.parts)
                for part in shape.parts:
                    for _ in part:
                        vertex_count += 1
            except Exception:
                # Fallback if parts iterator fails
                try:
                    vertex_count += shape.pointCount
                except Exception:
                    pass
    return part_count, vertex_count

def _apply_update(src_layer, raster, geom_type, updates_csv, log):
    """
    Apply UpdateFeatureZ_3d to 'src_layer' using 'raster'.
    Curve-safe: skips any feature with true curves.
    Audits XY identity by comparing part and vertex counts before and after.
    """
    lyr = _make_feature_layer_from_layer(src_layer, f"u_{_now_stamp()}")

    try:
        attempted = int(arcpy.management.GetCount(lyr).getOutput(0))
    except Exception:
        attempted = 0

    raster_name = getattr(raster, "name", "")
    layer_name = getattr(src_layer, "name", "<unnamed>")

    if attempted == 0:
        updates_csv.writerow({
            "layer_name": layer_name,
            "route": "BASE",
            "raster_used": raster_name,
            "attempted_count": 0,
            "updated_count": 0,
            "status": "skipped",
            "message": "selection_empty",
            "audit_part_count_before": "", "audit_part_count_after": "",
            "audit_vertex_count_before": "", "audit_vertex_count_after": ""
        })
        log(f"[INFO] {layer_name}: selection empty; raster={raster_name}")
        arcpy.management.Delete(lyr)
        return

    # Per-feature curve guard for non-point types
    if geom_type in ("polyline", "polygon"):
        # If any selected feature has curves, skip entire attempt to avoid partial edits
        has_curve_feature = False
        with arcpy.da.SearchCursor(lyr, ["OID@", "SHAPE@"]) as cur:
            for oid, shp in cur:
                try:
                    if shp and shp.hasCurves:
                        has_curve_feature = True
                        break
                except Exception:
                    continue
        if has_curve_feature:
            updates_csv.writerow({
                "layer_name": layer_name, "route": "BASE",
                "raster_used": raster_name, "attempted_count": attempted,
                "updated_count": 0, "status": "skipped_curves",
                "message": "selection_contains_true_curves; skipped to preserve geometry",
                "audit_part_count_before": "", "audit_part_count_after": "",
                "audit_vertex_count_before": "", "audit_vertex_count_after": ""
            })
            log(f"[INFO] {layer_name}: selection contains true curves; skipped to preserve geometry")
            arcpy.management.Delete(lyr)
            return

    # Pre-audit counts for XY identity check (non-points)
    parts_before = ""
    verts_before = ""
    if geom_type in ("polyline", "polygon"):
        pb, vb = _feature_counts(lyr, geom_type)
        parts_before, verts_before = str(pb), str(vb)

    updated_count = 0
    try:
        with _scoped_env(GEOGRAPHIC_TRANSFORM):
            arcpy.ddd.UpdateFeatureZ(in_surface=raster, in_features=lyr)
        updated_count = attempted

        # Post-audit counts
        parts_after = ""
        verts_after = ""
        if geom_type in ("polyline", "polygon"):
            pa, va = _feature_counts(lyr, geom_type)
            parts_after, verts_after = str(pa), str(va)

            # Identity check: part and vertex counts must be equal
            if parts_before != parts_after or verts_before != verts_after:
                # Report and mark as error. Geometry change detected.
                msg = "xy_identity_failed; part_or_vertex_count_changed"
                updates_csv.writerow({
                    "layer_name": layer_name, "route": "BASE",
                    "raster_used": raster_name, "attempted_count": attempted,
                    "updated_count": 0, "status": "error",
                    "message": msg,
                    "audit_part_count_before": parts_before,
                    "audit_part_count_after": parts_after,
                    "audit_vertex_count_before": verts_before,
                    "audit_vertex_count_after": verts_after
                })
                log(f"[ERR] {layer_name}: {msg}; raster={raster_name}")
                arcpy.management.Delete(lyr)
                return

        updates_csv.writerow({
            "layer_name": layer_name, "route": "BASE",
            "raster_used": raster_name, "attempted_count": attempted,
            "updated_count": updated_count, "status": "updated",
            "message": "",
            "audit_part_count_before": parts_before,
            "audit_part_count_after": parts_after if geom_type in ("polyline","polygon") else "",
            "audit_vertex_count_before": verts_before,
            "audit_vertex_count_after": verts_after if geom_type in ("polyline","polygon") else ""
        })
        log(f"[OK] {layer_name}: updated {updated_count} features; raster={raster_name}")

    except Exception as e:
        updates_csv.writerow({
            "layer_name": layer_name, "route": "BASE",
            "raster_used": raster_name, "attempted_count": attempted,
            "updated_count": 0, "status": "error",
            "message": str(e),
            "audit_part_count_before": parts_before,
            "audit_part_count_after": "",
            "audit_vertex_count_before": verts_before,
            "audit_vertex_count_after": ""
        })
        log(f"[ERR] {layer_name}: {e}")
    finally:
        arcpy.management.Delete(lyr)

if __name__ == "__main__":
    run()
