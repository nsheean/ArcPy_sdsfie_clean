# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
===================================================================================
Script: A2_Streams_Watersheds.py
Author: Nathanael Sheean
Date: 2025-09-14
Version: 3.0.0 (Cross-user, CRF-aware)

Purpose:
  Derive channels and basins from A1 products.
  Thresholds → combine → StreamLink/Order → StreamToFeature → Watershed (+ ZonalStats).
  Write durable outputs and a manifest for B-stage consumers.

Units and naming:
  - Names use centimeters for re{BASE_CODE}ility; all math is meters.

Inputs (from manifest_A1_*):
  - Fill, FlowDir, Drop, FlowAcc, cell_area_m2

Outputs:
  - Micro_drainage, Main_channels, Slope_contingent_semi_urban, Combine_streamCalcs
  - StreamLink_{BASE_CODE}, StreamOrder_{BASE_CODE}, StreamBinary_fc
  - Watershed_{BASE_CODE} (raster), Watershed_{BASE_CODE}_fc (polygons)
  - ZonalStats tables
  - Manifest: manifest_A2_<timestamp>.json
===================================================================================
"""

import os, json, time, gc, logging
from datetime import datetime
from contextlib import contextmanager

import arcpy
from arcpy.sa import (Con, SetNull, IsNull, Int, Float, FlowDirection, FlowAccumulation,
                      StreamLink, StreamOrder, Watershed, Plus, Times, Divide,
                      GreaterThanEqual, LessThan, GreaterThan, BooleanOr)

# -------------------------- Configuration -----------------------------------
# Thresholds (meters or m²)
MICRO_THRESHOLD_M2     = 2_500.0
MAIN_THRESHOLD_M2      = 10_000.0
SLOPE_WEIGHT_THRESHOLD = 500.0

# Big raster policy (same as A1)
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
    log_path = os.path.join(home, f"A2_streams_{ts}.log")
    logging.basicConfig(filename=log_path, filemode='w',
                        format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
    return logging.getLogger("A2_Streams"), home, ts, log_path

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
def _save_raster(sa_raster_obj, out_path):
    sa_raster_obj.save(out_path)
    for i in range(BIGDATA["BACKOFF_TRIES"]):
        if arcpy.Exists(out_path): break
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
        del sa_raster_obj; gc.collect(); arcpy.ClearWorkspaceCache_management()
    except Exception:
        pass

def _discover_latest_A1_manifest(aprx):
    home = aprx.homeFolder or os.getcwd()
    cands = [f for f in os.listdir(home) if f.startswith("manifest_A1_") and f.endswith(".json")]
    if not cands: raise RuntimeError("A1 manifest not found in project home.")
    cands.sort(reverse=True)
    with open(os.path.join(home, cands[0]), "r", encoding="utf-8") as f:
        return json.load(f), os.path.join(home, cands[0])

# -------------------------- Main --------------------------------------------
def run():
    aprx = arcpy.mp.ArcGISProject("CURRENT")
    logger, home, ts, log_path = _init_logger(aprx)
    logger.info(f"Log: {log_path}")

    if not aprx.defaultGeodatabase:
        raise RuntimeError("Set a default geodatabase in the project.")
    ws = aprx.defaultGeodatabase

    with _sa_license():
        manA1, manA1_path = _discover_latest_A1_manifest(aprx)
        fill_path    = manA1["outputs"]["fill"]
        flowdir_path = manA1["outputs"]["flowdir"]
        drop_path    = manA1["outputs"]["drop"]
        flowacc_path = manA1["outputs"]["flowacc"]
        cell_area_m2 = float(manA1["outputs"]["cell_area_m2"])
        dem_sr = arcpy.Describe(fill_path).spatialReference

        micro_path              = arcpy.CreateUniqueName("Micro_drainage", ws)
        main_path               = arcpy.CreateUniqueName("Main_channels", ws)
        drop_clean_path         = arcpy.CreateUniqueName("DropClean_{BASE_CODE}_DEM_10cm", ws)
        drop_clean_nonull_path  = arcpy.CreateUniqueName("DropClean_NoNull", ws)
        slope_cont_path         = arcpy.CreateUniqueName("Slope_contingent_semi_urban", ws)
        combine_path            = arcpy.CreateUniqueName("Combine_streamCalcs", ws)
        streamlink_path         = arcpy.CreateUniqueName("StreamLink_{BASE_CODE}", ws)
        streamorder_path        = arcpy.CreateUniqueName("StreamOrder_{BASE_CODE}", ws)
        streams_fc              = arcpy.CreateUniqueName("StreamBinary_fc", ws)
        watershed_ras           = arcpy.CreateUniqueName("Watershed_{BASE_CODE}", ws)
        watershed_fc            = arcpy.CreateUniqueName("Watershed_{BASE_CODE}_fc", ws)
        zonal_tbl_streams       = arcpy.CreateUniqueName("ZonalStats_Watershed_streams", ws)
        zonal_tbl_slope         = arcpy.CreateUniqueName("ZonalStats_Watershed_slope", ws)

        with _scoped_env(
            workspace=ws, scratchWorkspace=ws, overwriteOutput=False,
            outputCoordinateSystem=dem_sr,
            snapRaster=fill_path, extent=arcpy.Describe(fill_path).extent,
            cellSize=fill_path, mask=None
        ):
            accum_area = Times(flowacc_path, cell_area_m2)
            _save_raster(Con(GreaterThanEqual(accum_area, MICRO_THRESHOLD_M2), 1, 0), micro_path)
            _save_raster(Con(GreaterThanEqual(accum_area, MAIN_THRESHOLD_M2), 1, 0),  main_path)

            drop = arcpy.Raster(drop_path)
            drop_clean = SetNull(BooleanOr(LessThan(drop, 0.01), GreaterThan(drop, 100.0)), drop)
            _save_raster(drop_clean, drop_clean_path)
            _save_raster(Float(Con(IsNull(drop_clean_path), 0, drop_clean_path)), drop_clean_nonull_path)

            weighted = Times(accum_area, Divide(drop_clean_nonull_path, 100.0))
            _save_raster(Con(GreaterThanEqual(weighted, SLOPE_WEIGHT_THRESHOLD), 1, 0), slope_cont_path)

            sum12 = Plus(Int(main_path), Int(micro_path))
            sum123 = Plus(sum12, Int(slope_cont_path))
            _save_raster(Con(GreaterThanEqual(sum123, 2), 1, 0), combine_path)

            _save_raster(StreamLink(combine_path, flowdir_path), streamlink_path)
            _save_raster(StreamOrder(streamlink_path, flowdir_path, "SHREVE"), streamorder_path)
            arcpy.sa.StreamToFeature(streamlink_path, flowdir_path, streams_fc, "NO_SIMPLIFY")
            _save_raster(Watershed(flowdir_path, streamlink_path, "Value"), watershed_ras)

            arcpy.sa.ZonalStatisticsAsTable(watershed_ras, "Value", combine_path, zonal_tbl_streams, "DATA", "MAXIMUM")
            arcpy.sa.ZonalStatisticsAsTable(watershed_ras, "Value", slope_cont_path, zonal_tbl_slope, "DATA", "MEAN")

            arcpy.conversion.RasterToPolygon(watershed_ras, watershed_fc, "NO_SIMPLIFY", "Value", "SINGLE_OUTER_PART")

        # Manifest
        manifest = {
            "timestamp": ts,
            "workspace": ws,
            "home": home,
            "a1_manifest": manA1_path,
            "outputs": {
                "micro": micro_path, "main": main_path,
                "drop_clean": drop_clean_path, "drop_clean_nonull": drop_clean_nonull_path,
                "slope_cont": slope_cont_path, "combine": combine_path,
                "streamlink": streamlink_path, "streamorder": streamorder_path, "streams_fc": streams_fc,
                "watershed_ras": watershed_ras, "watershed_fc": watershed_fc
            }
        }
        man_path = os.path.join(home, f"manifest_A2_{ts}.json")
        with open(man_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        logger.info(f"Manifest: {man_path}")
        arcpy.AddMessage(f"A2 complete. Manifest: {man_path}")

if __name__ == "__main__":
    run()
