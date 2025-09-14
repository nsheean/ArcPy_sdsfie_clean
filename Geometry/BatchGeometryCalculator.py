# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
===================================================================================
Script: BatchGeometryCalculator.py
Author: Nathanael Sheean
Date: 2025-09-14
Version: 3.3 (Geodesic-only; per-layer CRS; cross-user; updated & skipped CSVs)

Purpose:
  Calculate geometry attributes for SDSFIE-style layers:
    *_A (Polygon) → area (geodesic), perimeter (geodesic), centroid lat/long
    *_L (Polyline) → length (geodesic), start/end lat/long, optional Z
    *_P (Point) → lat/long, optional Z

Standards:
  - All applicable measures are GEODESIC:
      LENGTH_GEODESIC, PERIMETER_LENGTH_GEODESIC, AREA_GEODESIC
  - Each layer’s own spatial reference (projection + datum) is used.

Unit policy (explicit and auditable):
  - Declare which *_A layers compute in square yards vs square feet vs acres.
  - Anything not listed falls to AREA_DEFAULT_A.
  - Optionally derive units from the layer SR (UNITS_FROM_LAYER=True).

Outputs (project home):
  - geometry_calc_audit_<ts>.csv        → all layers with status and details
  - geometry_calc_updated_<ts>.csv      → only layers that were updated (processed)
  - geometry_calc_skipped_<ts>.csv      → layers not updated (skipped/non_compliant/error)
  - geometry_calc_<ts>.log              → verbose log
===================================================================================
"""

import arcpy, logging, os, csv
from datetime import datetime
from collections import deque

# =================== CONFIG ===================

ACTIVE_MAP_ONLY   = False     # True = only active map; False = all maps in the project
CREATE_MESSAGES   = True      # Emit GP messages

# Optional strict validator for polygon area classification
STRICT_AREA_POLICY = False    # True = abort if any *_A unclassified or sets overlap

# Unit policy
UNITS_FROM_LAYER  = False     # True = derive from layer linear units; False = US survey defaults

# Length defaults (used when UNITS_FROM_LAYER = False)
LENGTH_UNIT_US     = "FEET_US"

# Polygon area policy — explicit and transparent (edit to your standards)
AREA_SQYD_A   = {
    # e.g., "Roadway_A", "Sidewalk_A", "PavementMarking_A"
}
AREA_SQFT_A   = {
    "Building_A", "Structure_A", "RecreationFeature_A", "Tower_A", "WPumpStation_A"
}
AREA_ACRES_A  = {
    "LandParcel_A", "Installation_A"
}
# Default for any *_A not in the sets above:
AREA_DEFAULT_A = "SQUARE_YARDS_US"    # choose "SQUARE_YARDS_US" or "SQUARE_FEET_US"

# =================== OUTPUT PATHS ===================

_aprx = arcpy.mp.ArcGISProject("CURRENT")
_home = _aprx.homeFolder or os.path.dirname(_aprx.defaultGeodatabase or "") or arcpy.env.scratchFolder or os.getcwd()
os.makedirs(_home, exist_ok=True)

_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_PATH   = os.path.join(_home, f"geometry_calc_{_ts}.log")
AUDIT_CSV  = os.path.join(_home, f"geometry_calc_audit_{_ts}.csv")
UPDATED_CSV= os.path.join(_home, f"geometry_calc_updated_{_ts}.csv")
SKIPPED_CSV= os.path.join(_home, f"geometry_calc_skipped_{_ts}.csv")

# =================== LOGGING ===================

logging.basicConfig(filename=LOG_PATH, level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("GeomCalc")

def _msg(s):  arcpy.AddMessage(s) if CREATE_MESSAGES else None;  log.info(s)
def _warn(s): arcpy.AddWarning(s) if CREATE_MESSAGES else None; log.warning(s)
def _err(s):  arcpy.AddError(s) if CREATE_MESSAGES else None;   log.error(s)

# =================== CSV SCHEMAS & BUFFERS ===================

_AUDIT_HEADER = [
    "timestamp_utc","map","layer","shape_type","has_z","action","reason","outputs",
    "created_fields","updated_fields","area_unit","length_unit",
    "layer_sr_wkid","layer_sr_name","coord_format","catalog_path"
]
_UPDATED_HEADER = [
    "timestamp_utc","map","layer","shape_type","has_z",
    "outputs","created_fields","updated_fields","area_unit","length_unit",
    "layer_sr_wkid","layer_sr_name","catalog_path"
]
_SKIPPED_HEADER = [
    "timestamp_utc","map","layer","shape_type","has_z",
    "action","reason","layer_sr_wkid","layer_sr_name","catalog_path"
]

_UPDATED_ROWS = []  # populated when action == "processed"
_SKIPPED_ROWS = []  # populated when action in {"skipped","non_compliant","error"}

def _ensure_header(path, header):
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(header)

def _write_updated_skipped_reports():
    if _UPDATED_ROWS:
        _ensure_header(UPDATED_CSV, _UPDATED_HEADER)
        with open(UPDATED_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for r in _UPDATED_ROWS:
                w.writerow([
                    r["timestamp_utc"], r["map"], r["layer"], r["shape_type"], r["has_z"],
                    r["outputs"], r["created_fields"], r["updated_fields"],
                    r["area_unit"], r["length_unit"], r["layer_sr_wkid"], r["layer_sr_name"], r["catalog_path"]
                ])
    if _SKIPPED_ROWS:
        _ensure_header(SKIPPED_CSV, _SKIPPED_HEADER)
        with open(SKIPPED_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for r in _SKIPPED_ROWS:
                w.writerow([
                    r["timestamp_utc"], r["map"], r["layer"], r["shape_type"], r["has_z"],
                    r["action"], r["reason"], r["layer_sr_wkid"], r["layer_sr_name"], r["catalog_path"]
                ])

def _audit_row(map_name, layer_name, shape_type, has_z, action, reason, outputs,
               created_fields, updated_fields, area_unit, length_unit,
               sr_wkid, sr_name, coord_format, catalog_path):
    # master audit (append-as-you-go)
    _ensure_header(AUDIT_CSV, _AUDIT_HEADER)
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    with open(AUDIT_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow([
            ts, map_name or "", layer_name or "", shape_type or "", bool(has_z),
            action or "", reason or "",
            ";".join(outputs) if outputs else "",
            ";".join(created_fields) if created_fields else "",
            ";".join(updated_fields) if updated_fields else "",
            area_unit or "", length_unit or "",
            sr_wkid or "", sr_name or "", coord_format or "", catalog_path or ""
        ])
    # secondary reports
    if action == "processed":
        _UPDATED_ROWS.append({
            "timestamp_utc": ts, "map": map_name or "", "layer": layer_name or "",
            "shape_type": shape_type or "", "has_z": bool(has_z),
            "outputs": ";".join(outputs) if outputs else "",
            "created_fields": ";".join(created_fields) if created_fields else "",
            "updated_fields": ";".join(updated_fields) if updated_fields else "",
            "area_unit": area_unit or "", "length_unit": length_unit or "",
            "layer_sr_wkid": sr_wkid or "", "layer_sr_name": sr_name or "",
            "catalog_path": catalog_path or ""
        })
    elif action in {"skipped","non_compliant","error"}:
        _SKIPPED_ROWS.append({
            "timestamp_utc": ts, "map": map_name or "", "layer": layer_name or "",
            "shape_type": shape_type or "", "has_z": bool(has_z),
            "action": action, "reason": reason or "",
            "layer_sr_wkid": sr_wkid or "", "layer_sr_name": sr_name or "",
            "catalog_path": catalog_path or ""
        })

# =================== HELPERS ===================

def _targets_suffix(lname: str) -> bool:
    return isinstance(lname, str) and lname.endswith(("_A","_L","_P"))

def _suffix_matches(lname: str, shape_type: str) -> bool:
    return (
        (lname.endswith("_P") and shape_type == "Point") or
        (lname.endswith("_L") and shape_type == "Polyline") or
        (lname.endswith("_A") and shape_type == "Polygon")
    )

def _catalog_path(lyr) -> str:
    try: return arcpy.Describe(lyr).catalogPath
    except Exception:
        try: return getattr(lyr, "dataSource", "")
        except Exception: return ""

def _layer_sr_info(lyr):
    try:
        sr = arcpy.Describe(lyr).spatialReference
        return getattr(sr, "factoryCode", ""), getattr(sr, "name", "")
    except Exception:
        return "", ""

def _exists(path: str) -> bool:
    try: return bool(path) and arcpy.Exists(path)
    except Exception: return False

def _workspace_of(path: str) -> str:
    try: return arcpy.Describe(path).path
    except Exception:
        try: return arcpy.da.Describe(path).get("path", "")
        except Exception: return ""

def _workspace_reachable(path: str) -> bool:
    wsp = _workspace_of(path)
    return _exists(wsp) if wsp else _exists(path)

def _is_feature_layer(lyr) -> bool:
    if not getattr(lyr, "isFeatureLayer", False): return False
    try:
        d = arcpy.Describe(lyr)
        return getattr(d, "dataType", "") in {"FeatureLayer","FeatureLayerView"} and hasattr(d, "shapeType")
    except Exception:
        return False

def _geom_type(lyr) -> str:
    return arcpy.Describe(lyr).shapeType

def _has_z(lyr) -> bool:
    try: return bool(getattr(arcpy.Describe(lyr), "hasZ", False))
    except Exception: return False

def _is_virtual_or_service(lyr) -> bool:
    try:
        d = arcpy.Describe(lyr)
        src = (getattr(d, "dataSource", "") or "").lower()
        is_service = any(s in src for s in (".mapserver",".featureserver","/wms","/wmts"))
        has_join  = bool(getattr(d, "hasJoin", False))
        return is_service or has_join
    except Exception:
        return False

def _schema_lock_ok(cat_path: str) -> bool:
    try: return arcpy.TestSchemaLock(cat_path)
    except Exception: return False

def _field_names_lower(obj):
    try: return {f.name.lower() for f in arcpy.ListFields(obj)}
    except Exception: return set()

def _classify_fields(existing_lower, targets):
    to_create, to_update = [], []
    for t in targets:
        (to_update if t.lower() in existing_lower else to_create).append(t)
    return to_create, to_update

def _noncompliant(map_name, lyr, reason, shape_type=""):
    lname = getattr(lyr, "name", "?")
    cat = _catalog_path(lyr)
    sr_wkid, sr_name = _layer_sr_info(lyr)
    _warn(f"[{map_name}] {lname}: {reason}")
    _audit_row(map_name, lname, shape_type, False, "non_compliant", reason, [], [], [],
               "", "", sr_wkid, sr_name, "", cat)

# Units derived from layer SR (optional)
def _units_from_sr(sr):
    try:
        unit = (sr.linearUnitName or "").lower()
    except Exception:
        unit = ""
    if "meter" in unit:
        return ("METERS", "SQUARE_METERS", "HECTARES")
    if "foot" in unit and ("us" in unit or "survey" in unit):
        return ("FEET_US", "SQUARE_FEET_US", "ACRES_US")
    if "foot" in unit:
        return ("FEET", "SQUARE_FEET", "ACRES")
    # fallback to US survey
    return (LENGTH_UNIT_US, "SQUARE_FEET_US", "ACRES_US")

# =================== AREA POLICY VALIDATION (optional strict mode) ===========

def _validate_area_policy(map_list):
    # Overlap check
    overlaps = [
        ("AREA_SQYD_A ∩ AREA_SQFT_A", AREA_SQYD_A & AREA_SQFT_A),
        ("AREA_SQYD_A ∩ AREA_ACRES_A", AREA_SQYD_A & AREA_ACRES_A),
        ("AREA_SQFT_A ∩ AREA_ACRES_A", AREA_SQFT_A & AREA_ACRES_A),
    ]
    for name, inter in overlaps:
        if inter:
            _err(f"Area policy overlap: {name}: {sorted(inter)}")
            if STRICT_AREA_POLICY:
                raise RuntimeError("Overlapping area policy sets. Resolve before running.")

    # Unclassified *_A present in maps
    seen_A = set()
    for m in map_list:
        for lyr in iter_leaf_layers(m):
            lname = getattr(lyr, "name", "")
            if lname.endswith("_A"):
                seen_A.add(lname)
    classified = AREA_SQYD_A | AREA_SQFT_A | AREA_ACRES_A
    unclassified = sorted(seen_A - classified)
    if unclassified:
        _warn(f"Unclassified *_A using default ({AREA_DEFAULT_A}): {unclassified}")
        if STRICT_AREA_POLICY:
            raise RuntimeError("Unclassified *_A found with STRICT_AREA_POLICY=True.")

# =================== CALCULATIONS (geodesic, per-layer SR) ===================

def _calc_lines(lyr, lname, map_name, hasz, shape_type, cat):
    sr = arcpy.Describe(lyr).spatialReference
    if not sr or getattr(sr, "factoryCode", 0) in (0, None):
        _noncompliant(map_name, lyr, "Layer has unknown spatial reference", shape_type)
        return

    length_unit = _units_from_sr(sr)[0] if UNITS_FROM_LAYER else LENGTH_UNIT_US
    coord_format = "DD" if getattr(sr, "type", "").lower() == "geographic" else ""

    outputs = ["lengthSize","latitudeFrom","latitudeTo","longitudeFrom","longitudeTo"]
    if hasz: outputs += ["elevationFrom","elevationTo"]
    to_create, to_update = _classify_fields(_field_names_lower(lyr), outputs)

    props = [
        "lengthSize LENGTH_GEODESIC",
        "latitudeFrom LINE_START_Y",
        "latitudeTo LINE_END_Y",
        "longitudeFrom LINE_START_X",
        "longitudeTo LINE_END_X",
    ] + (["elevationFrom LINE_START_Z","elevationTo LINE_END_Z"] if hasz else [])

    arcpy.management.CalculateGeometryAttributes(
        in_features=lyr,
        geometry_property=";".join(props),
        length_unit=length_unit,
        area_unit="",
        coordinate_system=sr,
        coordinate_format=coord_format,
    )

    _msg(f"[{map_name}] {lname}: lines calculated (GEODESIC; SR={sr.factoryCode} {sr.name}).")
    _audit_row(map_name, lname, shape_type, hasz, "processed", "",
               outputs, to_create, to_update, "", length_unit,
               getattr(sr,"factoryCode",""), getattr(sr,"name",""), coord_format, cat)

def _resolve_polygon_area_unit(lname, sr):
    if lname in AREA_SQFT_A:  return "SQUARE_FEET_US"
    if lname in AREA_SQYD_A:  return "SQUARE_YARDS_US"
    if lname in AREA_ACRES_A: return "ACRES_US"
    if not UNITS_FROM_LAYER:  return AREA_DEFAULT_A
    # UNITS_FROM_LAYER: derive sensible default from SR (acres only when explicit)
    return _units_from_sr(sr)[1]  # SQUARE_METERS or SQUARE_FEET_US

def _calc_polygons(lyr, lname, map_name, hasz, shape_type, cat):
    sr = arcpy.Describe(lyr).spatialReference
    if not sr or getattr(sr, "factoryCode", 0) in (0, None):
        _noncompliant(map_name, lyr, "Layer has unknown spatial reference", shape_type)
        return

    length_unit = _units_from_sr(sr)[0] if UNITS_FROM_LAYER else LENGTH_UNIT_US
    area_unit = _resolve_polygon_area_unit(lname, sr)
    coord_format = "DD" if getattr(sr, "type", "").lower() == "geographic" else ""

    outputs = ["areaSize","perimeterSize","latitude","longitude"]
    to_create, to_update = _classify_fields(_field_names_lower(lyr), outputs)

    props = [
        "areaSize AREA_GEODESIC",
        "perimeterSize PERIMETER_LENGTH_GEODESIC",
        "latitude INSIDE_Y",
        "longitude INSIDE_X",
    ]

    arcpy.management.CalculateGeometryAttributes(
        in_features=lyr,
        geometry_property=";".join(props),
        length_unit=length_unit,
        area_unit=area_unit,
        coordinate_system=sr,
        coordinate_format=coord_format,
    )

    _msg(f"[{map_name}] {lname}: polygons calculated (GEODESIC; area={area_unit}; SR={sr.factoryCode} {sr.name}).")
    _audit_row(map_name, lname, shape_type, hasz, "processed", "",
               outputs, to_create, to_update, area_unit, length_unit,
               getattr(sr,"factoryCode",""), getattr(sr,"name",""), coord_format, cat)

def _calc_points(lyr, lname, map_name, hasz, shape_type, cat):
    sr = arcpy.Describe(lyr).spatialReference
    if not sr or getattr(sr, "factoryCode", 0) in (0, None):
        _noncompliant(map_name, lyr, "Layer has unknown spatial reference", shape_type)
        return

    coord_format = "DD" if getattr(sr, "type", "").lower() == "geographic" else ""

    outputs = ["longitude","latitude"] + (["elevation"] if hasz else [])
    to_create, to_update = _classify_fields(_field_names_lower(lyr), outputs)

    props = ["longitude POINT_X","latitude POINT_Y"] + (["elevation POINT_Z"] if hasz else [])

    arcpy.management.CalculateGeometryAttributes(
        in_features=lyr,
        geometry_property=";".join(props),
        length_unit="", area_unit="",
        coordinate_system=sr,
        coordinate_format=coord_format,
    )

    _msg(f"[{map_name}] {lname}: points calculated (SR={sr.factoryCode} {sr.name}).")
    _audit_row(map_name, lname, shape_type, hasz, "processed", "",
               outputs, to_create, to_update, "", "",
               getattr(sr,"factoryCode",""), getattr(sr,"name",""), coord_format, cat)

# =================== PROCESS ONE LAYER ===================

def process_layer(lyr, map_name, counters):
    lname = getattr(lyr, "name", "?")
    cat = _catalog_path(lyr)
    sr_wkid, sr_name = _layer_sr_info(lyr)

    if not _targets_suffix(lname):
        _audit_row(map_name, lname, "", False, "skipped", "Suffix policy not targeted",
                   [], [], [], "", "", sr_wkid, sr_name, "", cat)
        counters["skipped"] += 1; return
    if not _is_feature_layer(lyr):
        _noncompliant(map_name, lyr, "Not a feature layer with geometry"); counters["noncompliant"] += 1; return

    if getattr(lyr, "isBroken", False):
        _noncompliant(map_name, lyr, "Layer is broken"); counters["noncompliant"] += 1; return
    if not cat or not _exists(cat):
        _noncompliant(map_name, lyr, "Catalog path not found"); counters["noncompliant"] += 1; return
    if not _workspace_reachable(cat):
        _noncompliant(map_name, lyr, f"Workspace unreachable: {_workspace_of(cat) or cat}"); counters["noncompliant"] += 1; return
    if _is_virtual_or_service(lyr):
        _noncompliant(map_name, lyr, "Virtual/service/joined layer not eligible"); counters["noncompliant"] += 1; return
    if not _schema_lock_ok(cat):
        _noncompliant(map_name, lyr, "Schema lock (dataset in edit/use)"); counters["noncompliant"] += 1; return

    try:
        shape_type = _geom_type(lyr)
        if not _suffix_matches(lname, shape_type):
            _noncompliant(map_name, lyr, f"Name-suffix vs shapeType mismatch: {shape_type}", shape_type)
            counters["noncompliant"] += 1; return

        hasz = _has_z(lyr)

        if lname.endswith("_L"): _calc_lines(lyr, lname, map_name, hasz, shape_type, cat)
        elif lname.endswith("_A"): _calc_polygons(lyr, lname, map_name, hasz, shape_type, cat)
        elif lname.endswith("_P"): _calc_points(lyr, lname, map_name, hasz, shape_type, cat)

        counters["processed"] += 1

    except arcpy.ExecuteError as ee:
        msg = arcpy.GetMessages(2) or str(ee)
        _err(f"[{map_name}] {lname}: ExecuteError: {msg}")
        _audit_row(map_name, lname, "", False, "error", f"ExecuteError: {msg}", [], [], [],
                   "", "", sr_wkid, sr_name, "", cat)
        counters["errors"] += 1
    except Exception as e:
        _err(f"[{map_name}] {lname}: {type(e).__name__}: {e}")
        _audit_row(map_name, lname, "", False, "error", f"{type(e).__name__}: {e}", [], [], [],
                   "", "", sr_wkid, sr_name, "", cat)
        counters["errors"] += 1

# =================== TRAVERSAL ===================

def iter_leaf_layers(map_obj):
    """Depth-first traversal. Descend into any layer exposing listLayers(); yield leaves."""
    stack = deque(map_obj.listLayers())
    while stack:
        lyr = stack.pop()
        try:
            subs = lyr.listLayers()
            if subs:
                for sub in subs: stack.append(sub)
                continue
        except Exception:
            pass
        yield lyr

# =================== MAIN ===================

def run():
    arcpy.env.addOutputsToMap = False
    maps = [_aprx.activeMap] if ACTIVE_MAP_ONLY and _aprx.activeMap else _aprx.listMaps()
    if not maps:
        _err("No maps found in the project."); return

    # Optional area-policy validation up front
    _validate_area_policy(maps)

    _msg("Geometry calc start | Maps: " + ", ".join(m.name for m in maps))

    counters = {"processed":0, "skipped":0, "noncompliant":0, "errors":0}
    for m in maps:
        _msg(f"Scanning map: {m.name}")
        for lyr in iter_leaf_layers(m):
            process_layer(lyr, m.name, counters)

    # Write secondary reports
    _write_updated_skipped_reports()

    # Summary
    _msg("----- Summary -----")
    _msg(f"Processed (updated): {counters['processed']}  | Updated CSV: {UPDATED_CSV}")
    _msg(f"Skipped:            {counters['skipped']}")
    _msg(f"Non-compliant:      {counters['noncompliant']}")
    _msg(f"Errors:             {counters['errors']}     | Skipped CSV: {SKIPPED_CSV}")
    _msg(f"Audit CSV:          {AUDIT_CSV}")
    _msg(f"Log file:           {LOG_PATH}")

if __name__ == "__main__":
    run()
