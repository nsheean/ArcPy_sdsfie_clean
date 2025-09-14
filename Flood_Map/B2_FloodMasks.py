# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
===================================================================================
Script: B2_FloodMasks.py
Author: Nathanael Sheean
Date: 2025-09-14
Version: 3.0.0 (Cross-user, CRF-aware, manifest-aware)

Purpose:
  Build geometric HAND masks (1/5/10 cm) and rainfall-equivalent masks by watershed.
  Persist rasters (+ 10 cm polygons) and emit a manifest.

Units and naming:
  - Names use centimeters for re{BASE_CODE}ility; all math is meters.

Inputs (from manifest_B1_*):
  - HAND raster, Watershed raster

Outputs:
  - Flood01cm, Flood05cm, Flood10cm (+ Flood10cm_fc)
  - Flood01cmRain, Flood05cmRain, Flood10cmRain (+ Flood10cmRain_fc)
  - Manifest: manifest_B2_<timestamp>.json
===================================================================================
"""

import os, json, time, gc, logging
from datetime import datetime
from contextlib import contextmanager

import numpy as np
import arcpy
from arcpy.sa import Con, Reclassify, RemapValue

# -------------------------- Configuration -----------------------------------
RAINFALL_SET        = [("01cmRain", 0.01), ("05cmRain", 0.05), ("10cmRain", 0.10)]
RUNOFF_MODE         = "CN"     # "CN" (Curve Number) or "C" (coefficient)
CN_DEFAULT          = 90.0
IA_RATIO            = 0.20     # Ia = IA_RATIO * S
C_EFF               = 0.80
MAX_STAGE_CAP_M     = 1.0

BIGDATA = {
    "PYRAMIDS": True,
    "STATS": True,
    "BACKOFF_TRIES": 6,
    "BACKOFF_BASE_SEC": 0.7,
}

# -------------------------- Logging -----------------------------------------
def _init_logger(aprx):
    home = aprx.homeFolder or os.path.dirname(aprx.defaultGeodatabase or "") or arcpy.env.scratchFolder or os.getcwd()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(home, f"B2_masks_{ts}.log")
    logging.basicConfig(filename=log_path, filemode='w',
                        format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
    return logging.getLogger("B2_Masks"), home, ts, log_path

# -------------------------- License + env -----------------------------------
@contextmanager
def _sa_license():
    status = arcpy.CheckExtension("Spatial")
    if status != "Available":
        raise RuntimeError(f"Spatial Analyst extension not available: {status}")
    try:
        arcpy.CheckOutExtension("Spatial")
        yield
    finally:
        try: arcpy.CheckInExtension("Spatial")
        except Exception: pass

@contextmanager
def _scoped_env(**kwargs):
    saved = {
        "outputCoordinateSystem": arcpy.env.outputCoordinateSystem,
        "snapRaster": arcpy.env.snapRaster,
        "extent": arcpy.env.extent,
        "cellSize": arcpy.env.cellSize,
        "mask": arcpy.env.mask,
        "workspace": arcpy.env.workspace,
        "scratchWorkspace": arcpy.env.scratchWorkspace,
        "overwriteOutput": arcpy.env.overwriteOutput,
    }
    try:
        for k, v in kwargs.items():
            setattr(arcpy.env, k, v)
        yield
    finally:
        for k, v in saved.items():
            setattr(arcpy.env, k, v)

# -------------------------- Helpers -----------------------------------------
def _discover_latest_B1_manifest(aprx):
    home = aprx.homeFolder or os.getcwd()
    cands = [f for f in os.listdir(home) if f.startswith("manifest_B1_") and f.endswith(".json")]
    if not cands: raise RuntimeError("B1 manifest not found in project home.")
    cands.sort(reverse=True)
    with open(os.path.join(home, cands[0]), "r", encoding="utf-8") as f:
        return json.load(f), os.path.join(home, cands[0])

def _save_raster(sa_raster_obj, out_path):
    sa_raster_obj.save(out_path)
    for i in range(BIGDATA["BACKOFF_TRIES"]):
        if arcpy.Exists(out_path): break
        time.sleep(BIGDATA["BACKOFF_BASE_SEC"] * (1.7 ** i))
    if not arcpy.Exists(out_path):
        raise RuntimeError(f"Save failed or locked: {out_path}")
    if BIGDATA["PYRAMIDS"] or BIGDATA["STATS"]:
        arcpy.management.BuildPyramidsAndStatistics(
            out_path, "CALCULATE_STATISTICS" if BIGDATA["STATS"] else "NO_STATISTICS",
            "", "", "", "SKIP_EXISTING", "-1", "", "BILINEAR", "DEFAULT", ""
        )
    try:
        del sa_raster_obj; gc.collect(); arcpy.ClearWorkspaceCache_management()
    except Exception:
        pass

def _units_are_meters(spref) -> bool:
    try: return "meter" in (spref.linearUnitName or "").lower()
    except Exception: return False

def _runoff_depth_from_rainfall(P_m: float, mode: str, cn: float, ia_ratio: float, c_eff: float) -> float:
    P = float(P_m)
    if P <= 0.0: return 0.0
    if mode.upper() == "CN":
        S_m = max((25400.0 / cn - 254.0) / 1000.0, 0.0)
        Ia_m = ia_ratio * S_m
        if P <= Ia_m or S_m <= 0.0: return 0.0
        return max(((P - Ia_m) ** 2.0) / (P + (1.0 - ia_ratio) * S_m), 0.0)
    return max(c_eff * P, 0.0)

def _build_zone_stage_threshold_raster(watershed_ras_path: str, hand_ras_path: str,
                                       rainfall_name: str, rainfall_depth_m: float,
                                       ws_gdb: str, runoff_mode: str, cn: float,
                                       ia_ratio: float, c_eff: float, max_stage_cap_m: float):
    hand = arcpy.Raster(hand_ras_path)
    wshed = arcpy.Raster(watershed_ras_path)
    same_grid = (
        hand.meanCellWidth == wshed.meanCellWidth and
        hand.width == wshed.width and hand.height == wshed.height
    )
    if not same_grid:
        raise RuntimeError("HAND and Watershed rasters are not grid-aligned.")
    hand_arr = arcpy.RasterToNumPyArray(hand, nodata_to_value=float("nan"))
    ws_arr = arcpy.RasterToNumPyArray(wshed, nodata_to_value=-2147483648)
    cell_area = float(hand.meanCellWidth) * float(hand.meanCellHeight)
    zones = np.unique(ws_arr); zones = zones[zones >= 0]
    if zones.size == 0:
        raise RuntimeError("No watershed zones found.")

    Qm = _runoff_depth_from_rainfall(rainfall_depth_m, runoff_mode, cn, ia_ratio, c_eff)
    zone_to_h = {}
    v = hand_arr
    for z in zones:
        hv = v[ws_arr == z]
        hv = hv[np.isfinite(hv)]
        if hv.size == 0:
            zone_to_h[int(z)] = 0.0
            continue
        hv.sort()
        A_zone = hv.size * cell_area
        V_target = Qm * A_zone

        prefix = np.cumsum(hv)
        def volume_at(h):
            k = np.searchsorted(hv, h, side="right")
            if k <= 0: return 0.0
            Sv = prefix[k-1]
            return cell_area * (k * h - Sv)

        h_lo, h_hi = 0.0, min(max_stage_cap_m, float(hv.max()) + 0.01)
        if volume_at(h_hi) < V_target:
            zone_to_h[int(z)] = h_hi
            continue
        for _ in range(40):
            h_mid = 0.5 * (h_lo + h_hi)
            if volume_at(h_mid) < V_target: h_lo = h_mid
            else: h_hi = h_mid
        zone_to_h[int(z)] = h_hi

    remap = RemapValue([[int(z), float(zone_to_h[int(z)])] for z in zone_to_h.keys()])
    hthresh_path = arcpy.CreateUniqueName(f"Hthresh_{rainfall_name}", ws_gdb)
    Reclassify(wshed, "Value", remap, missing_values="NODATA").save(hthresh_path)
    return hthresh_path, zone_to_h, Qm

# -------------------------- Main --------------------------------------------
def run():
    aprx = arcpy.mp.ArcGISProject("CURRENT")
    logger, home, ts, log_path = _init_logger(aprx)
    logger.info(f"Log: {log_path}")
    if not aprx.defaultGeodatabase:
        raise RuntimeError("Set a default geodatabase in the project.")
    ws = aprx.defaultGeodatabase

    with _sa_license():
        manB1, manB1_path = _discover_latest_B1_manifest(aprx)
        hand_path = manB1["outputs"]["hand"]
        watershed_ras = manB1["outputs"]["watershed_ras"]
        dem_sr = arcpy.Describe(hand_path).spatialReference

        with _scoped_env(
            workspace=ws, scratchWorkspace=ws, overwriteOutput=False,
            outputCoordinateSystem=dem_sr, snapRaster=hand_path,
            extent=arcpy.Describe(hand_path).extent, cellSize=hand_path, mask=None
        ):
            if not _units_are_meters(dem_sr):
                arcpy.AddWarning("Linear units are not meters. Thresholds assume meters.")

            # Geometric masks
            masks_geom = []
            for name, h in [("Flood01cm", 0.01), ("Flood05cm", 0.05), ("Flood10cm", 0.10)]:
                out_r = arcpy.CreateUniqueName(name, ws)
                _save_raster(Con(arcpy.Raster(hand_path) <= h, 1, 0), out_r)
                masks_geom.append(out_r)
            flood10_fc = arcpy.CreateUniqueName("Flood10cm_fc", ws)
            arcpy.conversion.RasterToPolygon(masks_geom[2], flood10_fc, "NO_SIMPLIFY", "Value", "SINGLE_OUTER_PART")

            # Rainfall-equivalent masks
            masks_rain = []
            for tag, P in RAINFALL_SET:
                hthresh_path, zone_to_h, Qm = _build_zone_stage_threshold_raster(
                    watershed_ras, hand_path, tag, P, ws,
                    RUNOFF_MODE, CN_DEFAULT, IA_RATIO, C_EFF, MAX_STAGE_CAP_M
                )
                out_r = arcpy.CreateUniqueName(f"Flood{tag}", ws)
                _save_raster(Con(arcpy.Raster(hand_path) <= arcpy.Raster(hthresh_path), 1, 0), out_r)
                masks_rain.append(out_r)
            flood10P_fc = arcpy.CreateUniqueName("Flood10cmRain_fc", ws)
            arcpy.conversion.RasterToPolygon(masks_rain[2], flood10P_fc, "NO_SIMPLIFY", "Value", "SINGLE_OUTER_PART")

    # Manifest
    manifest = {
        "timestamp": ts,
        "workspace": ws,
        "home": home,
        "b1_manifest": manB1_path,
        "outputs": {
            "geom_masks": masks_geom,
            "flood10_geom_fc": flood10_fc,
            "rain_masks": masks_rain,
            "flood10_rain_fc": flood10P_fc
        }
    }
    man_path = os.path.join(home, f"manifest_B2_{ts}.json")
    with open(man_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    logging.getLogger("B2_Masks").info(f"Manifest: {man_path}")
    arcpy.AddMessage(f"B2 complete. Manifest: {man_path}")

if __name__ == "__main__":
    run()
