# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
===================================================================================
Script: Utilities_All_With_Exemption_SR_Safe.py
Author: Nathanael Sheean
Date: 2025-09-15
Version: 2.1.0 (Curve-Safe, XY-Identity Audit, Cross-user QC)

Purpose:
  Update Z-values for UTILITY INFRASTRUCTURE using configured surface rasters.
  Apply an explicit exemption for a defined utility line.
  Produce audit-grade logs for QA/QC across users and projects.
  Preserve geometry shape. Update Z only.

Scope (group-scoped, nested-group aware):
  - Processes ONLY layers contained in the group '{BASE_CODE}_UTM40N_Utilities' and all subgroups.
  - Ignores layers outside this group.

Required rasters (LiDAR-derived DEMs; naming in centimeters for clarity, data in meters):
  - {BASE_CODE}_DEM_10cm
  - {BASE_CODE}_DEM_10cm_PLUS_60cm
  - {BASE_CODE}_DEM_10cm_MINUS_60cm

Routing rules:
  - *_P or *_A → BASE DEM
  - *_L       → MINUS_60cm DEM
  - EXEMPTION (only OInstallationPipeline_L):
      facilityNumber = '23325' → PLUS_60cm DEM
      else (or NULL)           → MINUS_60cm DEM

Safety:
  - Requires 3D Analyst. Checks out/in license.
  - Honors nested groups; validates Z-awareness before updates.
  - Geographic transformation is scoped per operation; no global env side effects.
  - Curve-safe: skip datasets or selections containing true curves to preserve arcs.
  - XY identity audit on lines/polygons: part and vertex counts must match pre/post.
  - No deletes, no appends, no temp writes to sources.

QC Logging (written to project home folder; fallback to arcpy.env.scratchFolder):
  1) <stamp>_layers_resolved.csv
  2) <stamp>_updates_applied.csv
  3) <stamp>_exemptions_applied.csv
  4) <stamp>_Utilities_All_With_Exemption_SR_Safe.log
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

RASTER_BASE_NAME      = "{BASE_CODE}_DEM_10cm"
RASTER_MINUS60_NAME   = "{BASE_CODE}_DEM_10cm_MINUS_60cm"
RASTER_PLUS60_NAME    = "{BASE_CODE}_DEM_10cm_PLUS_60cm"

EXEMPT_LAYER_NAME     = "OInstallationPipeline_L"
EXEMPT_FIELD_NAME     = "facilityNumber"
EXEMPT_FIELD_VALUE    = "23325"

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

def _suffix_route(layer_name):
    nm = (layer_name or "").strip().lower()
    if nm.endswith("_l"): return "LINE"
    if nm.endswith("_p"): return "POINT"
    if nm.endswith("_a"): return "AREA"
    return "UNKNOWN"

# -------- Curve detection and XY audit (non-destructive guarantees) --------
def _dataset_has_curves(obj):
    try:
        return bool(getattr(arcpy.Describe(obj), "hasCurves", False))
    except Exception:
        return False

def _selection_contains_curves(lyr):
    try:
        with arcpy.da.SearchCursor(lyr, ["OID@", "SHAPE@"]) as cur:
            for oid, shp in cur:
                try:
                    if shp and getattr(shp, "hasCurves", False):
                        return True
                except Exception:
                    continue
    except Exception:
        # Fail-safe to avoid accidental linearization
        return True
    return False

def _feature_counts(lyr, geom_type):
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

    with arcpy.da.SearchCursor(lyr, ["SHAPE@"]) as cur:
        for (shape,) in cur:
            if shape is None:
                continue
            try:
                part_count += shape.partCount
                vertex_count += shape.pointCount
            except Exception:
                try:
                    for part in shape.parts:
                        for _ in part:
                            vertex_count += 1
                        part_count += 1
                except Exception:
                    pass
    return part_count, vertex_count

# -------------------------- Main ------------------------------
def run():
    aprx, home = _project_context()
    m = aprx.activeMap
    if m is None:
        _add_err("No active map detected. Open a map and try again.")
        return

    stamp = _now_stamp()
    paths = _log_paths(home, stamp)

    # Log files
    layers_fh, layers_csv = _open_csv(paths["layers"], [
        "layer_name", "dataset_basename", "catalog_path", "geometry", "has_z", "dataset_has_curves"
    ])
    updates_fh, updates_csv = _open_csv(paths["updates"], [
        "layer_name", "route", "where_clause", "raster_used",
        "attempted_count", "status", "message",
        "audit_parts_before", "audit_parts_after",
        "audit_verts_before", "audit_verts_after"
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
        group = _find_group(m, GROUP_NAME)

        base_ras   = _resolve_raster_in_group(group, RASTER_BASE_NAME)
        minus_ras  = _resolve_raster_in_group(group, RASTER_MINUS60_NAME)
        plus_ras   = _resolve_raster_in_group(group, RASTER_PLUS60_NAME)

        _wlog(f"[INFO] Rasters -> BASE='{base_ras.name}', MINUS='{minus_ras.name}', PLUS='{plus_ras.name}'")
        if GEOGRAPHIC_TRANSFORM:
            _wlog(f"[INFO] Geographic Transformation: '{GEOGRAPHIC_TRANSFORM}'")
        else:
            _wlog("[INFO] Geographic Transformation: <default/none>")

        for lyr in _iter_feature_layers(group):
            cat = _catalog_path(lyr)
            if not cat or not arcpy.Exists(cat):
                _wlog(f"[WARN] Skipping (no catalog): {lyr.name}"); continue

            base = _basename_no_ext(cat)
            geom = _shape_type(lyr)
            hasz = _has_z(lyr)
            ds_has_curves = _dataset_has_curves(lyr)

            layers_csv.writerow({
                "layer_name": lyr.name, "dataset_basename": base,
                "catalog_path": cat, "geometry": geom, "has_z": hasz,
                "dataset_has_curves": ds_has_curves
            })

            route = _suffix_route(lyr.name)
            if route == "UNKNOWN":
                _wlog(f"[INFO] Skip (unsupported suffix): {lyr.name}")
                continue

            if not hasz:
                msg = "not_z_aware; no update attempted"
                updates_csv.writerow({
                    "layer_name": lyr.name, "route": route, "where_clause": "",
                    "raster_used": "", "attempted_count": 0,
                    "status": "skipped", "message": msg,
                    "audit_parts_before": "", "audit_parts_after": "",
                    "audit_verts_before": "", "audit_verts_after": ""
                })
                _wlog(f"[INFO] {lyr.name}: {msg}")
                continue

            def _raster_for(route_key):
                return {"POINT": base_ras, "AREA": base_ras, "LINE": minus_ras}[route_key]

            if base.lower() == EXEMPT_LAYER_NAME.lower() and route == "LINE":
                fld = _resolve_field(lyr, EXEMPT_FIELD_NAME)
                if not fld:
                    _wlog(f"[WARN] Exemption field '{EXEMPT_FIELD_NAME}' missing in {lyr.name}; treating all as non-exempt.")
                    _apply_update(src=lyr, route="LINE", raster=minus_ras, where_clause="",
                                  updates_csv=updates_csv, log=_wlog)
                else:
                    wc_exempt = f"{arcpy.AddFieldDelimiters(lyr, fld)} = '{EXEMPT_FIELD_VALUE}'"
                    wc_non    = f"({arcpy.AddFieldDelimiters(lyr, fld)} <> '{EXEMPT_FIELD_VALUE}' OR {arcpy.AddFieldDelimiters(lyr, fld)} IS NULL)"

                    count_exempt = _apply_update(src=lyr, route="LINE", raster=plus_ras, where_clause=wc_exempt,
                                                 updates_csv=updates_csv, log=_wlog)
                    if count_exempt > 0:
                        oid_name = arcpy.Describe(lyr).OIDFieldName
                        tmp = _make_feature_layer(lyr, f"exempt_{_now_stamp()}", wc_exempt)
                        with arcpy.da.SearchCursor(tmp, [oid_name]) as sc:
                            for (oid,) in sc:
                                exempt_csv.writerow({"layer_name": lyr.name, "oid_field": oid_name, "OBJECTID": oid})
                        arcpy.management.Delete(tmp)

                    _apply_update(src=lyr, route="LINE", raster=minus_ras, where_clause=wc_non,
                                  updates_csv=updates_csv, log=_wlog)
                continue

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
    Non-destructive: skip true curves; audit XY identity on non-curve lines/polys.
    Returns attempted record count for logging.
    """
    lyr = _make_feature_layer(src, f"u_{_now_stamp()}", where_clause if where_clause else None)

    try:
        attempted = int(arcpy.management.GetCount(lyr).getOutput(0))
    except Exception:
        attempted = 0

    raster_name = getattr(raster, "name", "")
    layer_name = getattr(src, "name", "<unnamed>")
    geom = _shape_type(lyr)

    if attempted == 0:
        updates_csv.writerow({
            "layer_name": layer_name, "route": route, "where_clause": where_clause,
            "raster_used": raster_name, "attempted_count": attempted,
            "status": "skipped", "message": "selection_empty",
            "audit_parts_before": "", "audit_parts_after": "",
            "audit_verts_before": "", "audit_verts_after": ""
        })
        log(f"[INFO] {layer_name}: selection empty; route={route}; raster={raster_name}")
        arcpy.management.Delete(lyr)
        return 0

    if geom not in ("point", "polyline", "polygon", "multipoint"):
        updates_csv.writerow({
            "layer_name": layer_name, "route": route, "where_clause": where_clause,
            "raster_used": raster_name, "attempted_count": attempted,
            "status": "skipped", "message": f"unsupported_geometry:{geom}",
            "audit_parts_before": "", "audit_parts_after": "",
            "audit_verts_before": "", "audit_verts_after": ""
        })
        log(f"[INFO] {layer_name}: unsupported geometry '{geom}'")
        arcpy.management.Delete(lyr)
        return attempted

    # Curve-safety: protect arcs and true curves from linearization
    if geom in ("polyline", "polygon"):
        if _dataset_has_curves(lyr) or _selection_contains_curves(lyr):
            updates_csv.writerow({
                "layer_name": layer_name, "route": route, "where_clause": where_clause,
                "raster_used": raster_name, "attempted_count": attempted,
                "status": "skipped_curves", "message": "selection_contains_true_curves; preserving geometry",
                "audit_parts_before": "", "audit_parts_after": "",
                "audit_verts_before": "", "audit_verts_after": ""
            })
            log(f"[INFO] {layer_name}: selection contains true curves; skipped to preserve geometry")
            arcpy.management.Delete(lyr)
            return attempted

        # Pre-audit counts for XY identity
        pb, vb = _feature_counts(lyr, geom)
    else:
        pb = vb = ""

    # Scoped transformation
    try:
        with _scoped_env(GEOGRAPHIC_TRANSFORM):
            arcpy.ddd.UpdateFeatureZ(in_surface=raster, in_features=lyr)

        # Post-audit for non-points
        if geom in ("polyline", "polygon"):
            pa, va = _feature_counts(lyr, geom)
            if (pb != pa) or (vb != va):
                msg = "xy_identity_failed; part_or_vertex_count_changed"
                updates_csv.writerow({
                    "layer_name": layer_name, "route": route, "where_clause": where_clause,
                    "raster_used": raster_name, "attempted_count": attempted,
                    "status": "error", "message": msg,
                    "audit_parts_before": pb, "audit_parts_after": pa,
                    "audit_verts_before": vb, "audit_verts_after": va
                })
                log(f"[ERR] {layer_name}: {msg}; route={route}; raster={raster_name}")
                return attempted

        updates_csv.writerow({
            "layer_name": layer_name, "route": route, "where_clause": where_clause,
            "raster_used": raster_name, "attempted_count": attempted,
            "status": "updated", "message": "",
            "audit_parts_before": pb, "audit_parts_after": (pa if geom in ("polyline","polygon") else ""),
            "audit_verts_before": vb, "audit_verts_after": (va if geom in ("polyline","polygon") else "")
        })
        log(f"[OK] {layer_name}: updated {attempted} features; route={route}; raster={raster_name}")

    except Exception as e:
        updates_csv.writerow({
            "layer_name": layer_name, "route": route, "where_clause": where_clause,
            "raster_used": raster_name, "attempted_count": attempted,
            "status": "error", "message": str(e),
            "audit_parts_before": pb if geom in ("polyline","polygon") else "",
            "audit_parts_after": "",
            "audit_verts_before": vb if geom in ("polyline","polygon") else "",
            "audit_verts_after": ""
        })
        log(f"[ERR] {layer_name}: {e}")
    finally:
        arcpy.management.Delete(lyr)

    return attempted


if __name__ == "__main__":
    run()
