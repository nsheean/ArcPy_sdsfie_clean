# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
===================================================================================
Script: A1_Preprocess_DEM.py
Author: Nathanael Sheean
Date: 2025-09-14
Version: 3.0.0 (Cross-user, CRF-aware)

Purpose:
  Build hydro primitives from a LiDAR DEM: Fill → FlowDirection (+Drop) → FlowAccumulation.
  Write durable outputs, verify, optimize, and emit a manifest for downstream stages.

Scope:
  - Optionally constrain to a Map + Group. Traverse nested groups safely.
  - Resolve DEM by exact name, then prefix match, case-insensitive.

Units and naming:
  - Dataset names use centimeters for re{BASE_CODE}ility (e.g., {BASE_CODE}_DEM_10cm).
  - All data and math use meters. No code-side unit conversion.

Outputs:
  - Fill_{BASE_CODE}_DEM_10cm
  - FlowDir_{BASE_CODE}_DEM_10cm
  - Drop_FlowDir_{BASE_CODE}_DEM_10cm
  - FlowAcc_{BASE_CODE}_DEM_10cm
  - Manifest: manifest_A1_<timestamp>.json (absolute paths)

Safety:
  - Checks Spatial Analyst availability.
  - Uses scoped arcpy.env with restore.
  - CRF for very large rasters to avoid FGDB limits and locks.
===================================================================================
"""

import os, json, time, gc, logging
from datetime import datetime
from contextlib import contextmanager

import arcpy
from arcpy.sa import Fill, FlowDirection, FlowAccumulation

# -------------------------- Configuration -----------------------------------
MAP_NAME            = None                     # e.g., "Hydrology"; None = active map
GROUP_NAME          = "{BASE_CODE}_UTM40N_Hydro"      # set None to search all layers/maps
DEM_NAME            = "{BASE_CODE}_DEM_10cm"          # exact or prefix match

# Fill z-limit in meters. Set to None to disable.
FILL_Z_LIMIT_M      = 0.10

# Big raster policy
BIGDATA = {
    "SIZE_THRESHOLD_CELLS": 100_000_000,   # switch to CRF around 100M cells
    "USE_CRF_IF_BIG": True,
    "CRF_SUBFOLDER": "crf_outputs",
    "PARALLEL": "75%",                     # set "0" if RAM is constrained
    "PYRAMIDS": True,
    "STATS": True,
    "BACKOFF_TRIES": 6,
    "BACKOFF_BASE_SEC": 0.7,
}

# -------------------------- Logging -----------------------------------------
def _init_logger(aprx):
    home = aprx.homeFolder or os.path.dirname(aprx.defaultGeodatabase or "") or arcpy.env.scratchFolder or os.getcwd()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(home, f"A1_preprocess_{ts}.log")
    logging.basicConfig(filename=log_path, filemode='w',
                        format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
    return logging.getLogger("A1_Preprocess"), home, ts, log_path

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
        "parallelProcessingFactor": arcpy.env.parallelProcessingFactor,
    }
    try:
        for k, v in kwargs.items():
            setattr(arcpy.env, k, v)
        yield
    finally:
        for k, v in saved.items():
            setattr(arcpy.env, k, v)

# -------------------------- Helpers -----------------------------------------
def _active_map(aprx):
    if MAP_NAME:
        for m in aprx.listMaps():
            if (m.name or "").strip().lower() == MAP_NAME.strip().lower():
                return m
    return aprx.activeMap

def _find_group_recursive(layer, key_lower):
    if getattr(layer, "isGroupLayer", False):
        if (layer.name or "").strip().lower() == key_lower:
            return layer
        for child in layer.listLayers():
            found = _find_group_recursive(child, key_lower)
            if found: return found
    else:
        try:
            subs = layer.listLayers()
            if subs:
                for c in subs:
                    found = _find_group_recursive(c, key_lower)
                    if found: return found
        except Exception:
            pass
    return None

def _resolve_group(aprx):
    if not GROUP_NAME:
        return None
    m = _active_map(aprx)
    if not m:
        raise RuntimeError("No active map. Open a map or set MAP_NAME.")
    key = GROUP_NAME.strip().lower()
    for top in m.listLayers():
        g = _find_group_recursive(top, key)
        if g: return g
    raise RuntimeError(f"Group '{GROUP_NAME}' not found in map '{m.name}'.")

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

def _resolve_dem(aprx):
    # Prefer group if set
    if GROUP_NAME:
        try:
            grp = _resolve_group(aprx)
            bag = []
            for top in grp.listLayers():
                _collect_rasters_recursive(top, bag)
            key = DEM_NAME.strip().lower()
            for r in bag:
                if (r.name or "").strip().lower() == key:
                    return r
            for r in bag:
                if (r.name or "").strip().lower().startswith(key):
                    return r
        except Exception:
            pass
    # Project-wide search
    for m in aprx.listMaps():
        for lyr in m.listLayers():
            try:
                d = arcpy.Describe(lyr)
                if getattr(d, "dataType", "") in ("RasterLayer", "MosaicLayer"):
                    nm = (lyr.name or "").strip().lower()
                    if nm == DEM_NAME.lower() or nm.startswith(DEM_NAME.lower()):
                        return arcpy.Raster(d.catalogPath)
            except Exception:
                continue
    # Default GDB fallback
    prev = arcpy.env.workspace
    try:
        arcpy.env.workspace = aprx.defaultGeodatabase
        hits = arcpy.ListRasters(DEM_NAME + "*") or []
        if hits: return arcpy.Raster(hits[0])
    finally:
        arcpy.env.workspace = prev
    return None

def _is_big(ras):
    try:
        return int(ras.width) * int(ras.height) >= BIGDATA["SIZE_THRESHOLD_CELLS"]
    except Exception:
        return False

def _out_path(base_name, ws, home, prefer_crf):
    if prefer_crf and BIGDATA["USE_CRF_IF_BIG"]:
        out_dir = os.path.join(home, BIGDATA["CRF_SUBFOLDER"])
        os.makedirs(out_dir, exist_ok=True)
        return os.path.join(out_dir, f"{base_name}.crf")
    return arcpy.CreateUniqueName(base_name, ws)

def _save_raster(sa_raster_obj, out_path):
    sa_raster_obj.save(out_path)
    for i in range(BIGDATA["BACKOFF_TRIES"]):
        if arcpy.Exists(out_path):
            break
        time.sleep(BIGDATA["BACKOFF_BASE_SEC"] * (1.7 ** i))
    if not arcpy.Exists(out_path):
        raise RuntimeError(f"Save failed or locked: {out_path}")
    if BIGDATA["PYRAMIDS"] or BIGDATA["STATS"]:
        arcpy.management.BuildPyramidsAndStatistics(
            out_path,
            "CALCULATE_STATISTICS" if BIGDATA["STATS"] else "NO_STATISTICS",
            "", "", "", "SKIP_EXISTING", "-1", "", "BILINEAR", "DEFAULT", ""
        )
    try:
        del sa_raster_obj
        gc.collect()
        arcpy.ClearWorkspaceCache_management()
    except Exception:
        pass

def _verify_exists(path):
    for i in range(BIGDATA["BACKOFF_TRIES"]):
        if arcpy.Exists(path):
            return True
        time.sleep(BIGDATA["BACKOFF_BASE_SEC"] * (1.7 ** i))
    return False

# -------------------------- Main --------------------------------------------
def run():
    aprx = arcpy.mp.ArcGISProject("CURRENT")
    logger, home, ts, log_path = _init_logger(aprx)
    logger.info(f"Log: {log_path}")

    if not aprx.defaultGeodatabase:
        raise RuntimeError("Set a default geodatabase in the project.")
    ws = aprx.defaultGeodatabase

    with _sa_license():
        dem = _resolve_dem(aprx)
        if not dem:
            raise RuntimeError(f"Could not find DEM '{DEM_NAME}' by exact or prefix match.")
        logger.info(f"DEM resolved: {dem.name}")

        prefer_crf = _is_big(dem)

        fill_path    = _out_path("Fill_{BASE_CODE}_DEM_10cm", ws, home, prefer_crf)
        flowdir_path = _out_path("FlowDir_{BASE_CODE}_DEM_10cm", ws, home, prefer_crf)
        drop_path    = _out_path("Drop_FlowDir_{BASE_CODE}_DEM_10cm", ws, home, prefer_crf)
        flowacc_path = _out_path("FlowAcc_{BASE_CODE}_DEM_10cm", ws, home, prefer_crf)

        with _scoped_env(
            workspace=ws, scratchWorkspace=ws, overwriteOutput=False,
            outputCoordinateSystem=dem.spatialReference,
            snapRaster=dem, extent=arcpy.Describe(dem).extent,
            cellSize=dem, mask=None, parallelProcessingFactor=BIGDATA["PARALLEL"]
        ):
            # Fill
            fill_out = Fill(dem, FILL_Z_LIMIT_M) if FILL_Z_LIMIT_M is not None else Fill(dem)
            _save_raster(fill_out, fill_path)
            logger.info(f"Saved: {fill_path}")

            # FlowDirection (+ Drop written via parameter)
            fd = FlowDirection(fill_path, "FORCE", drop_path, "D8")
            _save_raster(fd, flowdir_path)
            if not _verify_exists(drop_path):
                raise RuntimeError("Drop raster not created by FlowDirection.")
            logger.info(f"Saved: {flowdir_path}")
            logger.info(f"Saved: {drop_path}")

            # FlowAccumulation
            fa = FlowAccumulation(flowdir_path, data_type="FLOAT", flow_direction_type="D8")
            _save_raster(fa, flowacc_path)
            logger.info(f"Saved: {flowacc_path}")

            # Cell area
            try:
                cell_area_m2 = float(dem.meanCellWidth) * float(dem.meanCellHeight)
            except Exception:
                csx = float(arcpy.GetRasterProperties_management(dem, "CELLSIZEX").getOutput(0))
                csy = float(arcpy.GetRasterProperties_management(dem, "CELLSIZEY").getOutput(0))
                cell_area_m2 = csx * csy

        # Manifest
        manifest = {
            "timestamp": ts,
            "workspace": ws,
            "home": home,
            "dem": {"name": dem.name, "path": arcpy.Describe(dem).catalogPath},
            "outputs": {
                "fill": fill_path,
                "flowdir": flowdir_path,
                "drop": drop_path,
                "flowacc": flowacc_path,
                "cell_area_m2": cell_area_m2
            }
        }
        man_path = os.path.join(home, f"manifest_A1_{ts}.json")
        with open(man_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        logger.info(f"Manifest: {man_path}")
        arcpy.AddMessage(f"A1 complete. Manifest: {man_path}")

if __name__ == "__main__":
    run()
