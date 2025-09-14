# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
===================================================================================
Script: B1_HAND.py
Author: Nathanael Sheean
Date: 2025-09-14
Version: 3.0.0 (Cross-user, CRF-aware)

Purpose:
  Compute HAND (vertical flow distance from streams) using A2 outputs.
  Save durable HAND raster and emit a manifest for B2.

Units and naming:
  - Names use centimeters for re{BASE_CODE}ility; all math is meters.

Inputs (from manifest_A2_*):
  - Fill (surface), FlowDir, StreamLink

Outputs:
  - HAND50cm (raster; path may be CRF for large cases)
  - Manifest: manifest_B1_<timestamp>.json
===================================================================================
"""

import os, json, time, gc, logging
from datetime import datetime
from contextlib import contextmanager

import arcpy
from arcpy.sa import Con

# -------------------------- Configuration -----------------------------------
BIGDATA = {
    "SIZE_THRESHOLD_CELLS": 100_000_000,
    "USE_CRF_IF_BIG": True,
    "CRF_SUBFOLDER": "crf_outputs",
    "PYRAMIDS": True,
    "STATS": True,
    "BACKOFF_TRIES": 6,
    "BACKOFF_BASE_SEC": 0.7,
}

# -------------------------- Logging -----------------------------------------
def _init_logger(aprx):
    home = aprx.homeFolder or os.path.dirname(aprx.defaultGeodatabase or "") or arcpy.env.scratchFolder or os.getcwd()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(home, f"B1_hand_{ts}.log")
    logging.basicConfig(filename=log_path, filemode='w',
                        format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)
    return logging.getLogger("B1_HAND"), home, ts, log_path

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
def _discover_latest_A2_manifest(aprx):
    home = aprx.homeFolder or os.getcwd()
    cands = [f for f in os.listdir(home) if f.startswith("manifest_A2_") and f.endswith(".json")]
    if not cands: raise RuntimeError("A2 manifest not found in project home.")
    cands.sort(reverse=True)
    with open(os.path.join(home, cands[0]), "r", encoding="utf-8") as f:
        return json.load(f), os.path.join(home, cands[0])

def _is_big(ras):
    try: return int(ras.width) * int(ras.height) >= BIGDATA["SIZE_THRESHOLD_CELLS"]
    except Exception: return False

def _out_path(base_name, ws, home, prefer_crf):
    if prefer_crf and BIGDATA["USE_CRF_IF_BIG"]:
        folder = os.path.join(home, BIGDATA["CRF_SUBFOLDER"]); os.makedirs(folder, exist_ok=True)
        return os.path.join(folder, f"{base_name}.crf")
    return arcpy.CreateUniqueName(base_name, ws)

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

def _gp_flow_distance_paths(source_path, surface_path, flowdir_path, out_dist_path,
                            statistics_type="WEIGHTED_MEAN",
                            maximum_distance="", out_back_direction_raster="", distance_type="VERTICAL"):
    gp = arcpy.gp
    try:
        gp.FlowDistance_sa(source_path, surface_path, flowdir_path,
                           statistics_type, maximum_distance,
                           out_back_direction_raster, distance_type,
                           out_dist_path)
    except Exception:
        gp.FlowDistance_sa(source_path, surface_path, flowdir_path,
                           "MEAN", maximum_distance,
                           out_back_direction_raster, distance_type,
                           out_dist_path)
    return arcpy.Raster(out_dist_path)

# -------------------------- Main --------------------------------------------
def run():
    aprx = arcpy.mp.ArcGISProject("CURRENT")
    logger, home, ts, log_path = _init_logger(aprx)
    logger.info(f"Log: {log_path}")
    if not aprx.defaultGeodatabase:
        raise RuntimeError("Set a default geodatabase in the project.")
    ws = aprx.defaultGeodatabase

    with _sa_license():
        manA2, manA2_path = _discover_latest_A2_manifest(aprx)
        fill_path      = manA2["outputs"]["drop_clean_nonull"] if manA2["outputs"].get("drop_clean_nonull") else manA2["outputs"]["fill"] if "fill" in manA2["outputs"] else None
        if not fill_path:
            # Prefer Fill from A1 if not present in A2 manifest
            manA1_path = manA2.get("a1_manifest")
            if not manA1_path or not os.path.exists(manA1_path):
                raise RuntimeError("Cannot resolve Fill surface from manifests.")
            with open(manA1_path, "r", encoding="utf-8") as f:
                fill_path = json.load(f)["outputs"]["fill"]

        flowdir_path   = manA2["outputs"]["flowdir"] if "flowdir" in manA2["outputs"] else json.load(open(manA2["a1_manifest"]))["outputs"]["flowdir"]
        streamlink_path= manA2["outputs"]["streamlink"]

        dem_sr = arcpy.Describe(fill_path).spatialReference
        prefer_crf = _is_big(arcpy.Raster(fill_path))
        hand_path = _out_path("HAND50cm", ws, home, prefer_crf)

        with _scoped_env(
            workspace=ws, scratchWorkspace=ws, overwriteOutput=False,
            outputCoordinateSystem=dem_sr, snapRaster=fill_path,
            extent=arcpy.Describe(fill_path).extent, cellSize=fill_path, mask=None
        ):
            # Build HAND sources: binary from StreamLink > 0
            hand_sources = arcpy.CreateUniqueName("HAND_sources", ws)
            Con(arcpy.Raster(streamlink_path) > 0, 1).save(hand_sources)

            # FlowDistance_sa (VERTICAL)
            _gp_flow_distance_paths(
                source_path=arcpy.Describe(hand_sources).catalogPath,
                surface_path=arcpy.Describe(fill_path).catalogPath,
                flowdir_path=arcpy.Describe(flowdir_path).catalogPath,
                out_dist_path=hand_path,
                statistics_type="WEIGHTED_MEAN",
                distance_type="VERTICAL"
            )
            if not arcpy.Exists(hand_path):
                raise RuntimeError("HAND raster not created.")
            if BIGDATA["PYRAMIDS"] or BIGDATA["STATS"]:
                arcpy.management.BuildPyramidsAndStatistics(
                    hand_path, "CALCULATE_STATISTICS" if BIGDATA["STATS"] else "NO_STATISTICS",
                    "", "", "", "SKIP_EXISTING", "-1", "", "BILINEAR", "DEFAULT", ""
                )

        # Manifest
        manifest = {
            "timestamp": ts,
            "workspace": ws,
            "home": home,
            "a2_manifest": manA2_path,
            "outputs": {"hand": hand_path, "watershed_ras": manA2["outputs"]["watershed_ras"]}
        }
        man_path = os.path.join(home, f"manifest_B1_{ts}.json")
        with open(man_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        logger.info(f"Manifest: {man_path}")
        arcpy.AddMessage(f"B1 complete. Manifest: {man_path}")

if __name__ == "__main__":
    run()
