# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
================================================================================
Script: Script_to_modify_Linear_Segmentation_Segment_IDs_embedded.py
Author: Nathanael Sheean
Date: 2025-09-14

Purpose:
  Populate Segment ID fields using an embedded rules table and produce
  audit-grade CSV logs. No environment hardcoding. Works with nested groups.

Outputs (written to the ArcGIS Pro project's home folder; fallback: scratch):
  1) <stamp>_layers_catalog.csv
     - All layers found with a resolvable Segment ID field.
  2) <stamp>_edited_rows.csv
     - One row per feature updated (OBJECTID + assigned Segment ID).
  3) <stamp>_not_edited.csv
     - One row per target that produced no edits, with reason and counts.

ID format:
  PREFIX-0000000 (seven digits). Only empty cells are filled.

Determinism:
  - The embedded SEGMENT_RULES are the only hardcoded “table of definitions”.
  - Targets resolve by TOC name OR dataset basename (exact, case-insensitive).
================================================================================
"""

import os
import re
import csv
import datetime
import arcpy

# -----------------------------------------------------------------------------
# Embedded rules table (the only hardcoded content)
# -----------------------------------------------------------------------------
SEGMENT_RULES = [
    {"layer_name": "EMeter_P",             "segment_prefix": "E09"},
    {"layer_name": "EAirfieldLight_P",     "segment_prefix": "E27"},
    {"layer_name": "EAccessPoint_P",       "segment_prefix": "E01"},
    {"layer_name": "EExteriorLight_P",     "segment_prefix": "E06"},
    {"layer_name": "EGenerator_P",         "segment_prefix": "E07"},
    {"layer_name": "EPanel_P",             "segment_prefix": "E11"},
    {"layer_name": "EPanel_A",             "segment_prefix": "E11"},
    {"layer_name": "EPole_P",              "segment_prefix": "E12"},
    {"layer_name": "EServicePoint_P",      "segment_prefix": "E13"},
    {"layer_name": "ESubstation_A",        "segment_prefix": "E14"},
    {"layer_name": "ESwitch_P",            "segment_prefix": "E15"},
    {"layer_name": "ETransformer_P",       "segment_prefix": "E16"},
    {"layer_name": "WTower_P",             "segment_prefix": "W22"},
    {"layer_name": "WMeter_P",             "segment_prefix": "W09"},
    {"layer_name": "WValve_P",             "segment_prefix": "W21"},
    {"layer_name": "WValve_A",             "segment_prefix": "W21"},
    {"layer_name": "WHydrant_P",           "segment_prefix": "W05"},
    {"layer_name": "WServicePoint_P",      "segment_prefix": "W19"},
    {"layer_name": "WServicePoint_A",      "segment_prefix": "W19"},
    {"layer_name": "WStorageFacility_A",   "segment_prefix": "W20"},
    {"layer_name": "WPump_P",              "segment_prefix": "W16"},
    {"layer_name": "WBackflowPreventer_P", "segment_prefix": "W01"},
    {"layer_name": "WBlowOffValve_P",      "segment_prefix": "W02"},
    {"layer_name": "WControlValve_P",      "segment_prefix": "W03"},
    {"layer_name": "WFlowMeter_P",         "segment_prefix": "W04"},
    {"layer_name": "WMain_L",              "segment_prefix": "W08"},
    {"layer_name": "WMain_A",              "segment_prefix": "W08"},
    {"layer_name": "WManhole_P",           "segment_prefix": "W10"},
    {"layer_name": "WPressureRegulator_P", "segment_prefix": "W12"},
    {"layer_name": "WPressureZone_A",      "segment_prefix": "W13"},
    {"layer_name": "WSampleStation_P",     "segment_prefix": "W14"},
    {"layer_name": "WServiceLine_L",       "segment_prefix": "W18"},
    {"layer_name": "WServiceValve_P",      "segment_prefix": "W19"},
    {"layer_name": "WPumpStation_A",       "segment_prefix": "W29"},
    {"layer_name": "NGMeterStation_A",     "segment_prefix": "G09"},
    {"layer_name": "NGMeter_P",            "segment_prefix": "G09"},
    {"layer_name": "NGStation_A",          "segment_prefix": "G10"},
    {"layer_name": "NGValve_P",            "segment_prefix": "G12"},
    {"layer_name": "NGValve_A",            "segment_prefix": "G12"},
    {"layer_name": "NGMain_L",             "segment_prefix": "G08"},
    {"layer_name": "NGMain_A",             "segment_prefix": "G08"},
    {"layer_name": "NGServiceLine_L",      "segment_prefix": "G18"},
    {"layer_name": "NGServicePoint_P",     "segment_prefix": "G19"},
    {"layer_name": "NGServicePoint_A",     "segment_prefix": "G19"},
    {"layer_name": "NGAirReleaseValve_P",  "segment_prefix": "G01"},
    {"layer_name": "NGEmergencyValve_P",   "segment_prefix": "G02"},
    {"layer_name": "NGFlowMeter_P",        "segment_prefix": "G04"},
    {"layer_name": "NGGovernor_P",         "segment_prefix": "G05"},
    {"layer_name": "NGJunction_P",         "segment_prefix": "G06"},
    {"layer_name": "NGManhole_P",          "segment_prefix": "G10"},
    {"layer_name": "NGRegulatorStation_A", "segment_prefix": "G13"},
    {"layer_name": "NGSampleStation_P",    "segment_prefix": "G14"},
    {"layer_name": "NGValveChamber_A",     "segment_prefix": "G21"},
    {"layer_name": "NGValveChamber_P",     "segment_prefix": "G21"},
    {"layer_name": "NGValveChamber_L",     "segment_prefix": "G21"},
    {"layer_name": "NGStation_P",          "segment_prefix": "G10"},
    {"layer_name": "NGTower_P",            "segment_prefix": "G22"},
    {"layer_name": "NGAirReleaseValve_A",  "segment_prefix": "G01"},
    {"layer_name": "NGFlowMeter_A",        "segment_prefix": "G04"},
    {"layer_name": "NGGovernor_A",         "segment_prefix": "G05"},
    {"layer_name": "NGValve_A",            "segment_prefix": "G12"}
]

# -----------------------------------------------------------------------------
# Helpers (discovery, formatting, logging)
# -----------------------------------------------------------------------------
def _segment_counter(value, prefix):
    if value is None:
        return None
    txt = str(value).strip()
    m = re.match(rf"^{re.escape(prefix)}-(\d{{7}})$", txt)
    return int(m.group(1)) if m else None

def _next_id(prefix, counter):
    return f"{prefix}-{counter:07d}"

def _basename_no_ext(path):
    if not path:
        return ""
    base = os.path.basename(path)
    name, _ext = os.path.splitext(base)
    if "." in name:  # strip schema owner for enterprise
        name = name.split(".")[-1]
    return name

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

def _layer_catalog_path(lyr):
    try:
        if hasattr(lyr, "dataSource") and lyr.dataSource:
            return lyr.dataSource
    except Exception:
        pass
    try:
        desc = arcpy.Describe(lyr)
        return getattr(desc, "catalogPath", None)
    except Exception:
        return None

def _find_segment_field(target):
    fields = arcpy.ListFields(target)
    for f in fields:
        if "segment id" in (f.aliasName or "").lower():
            return f.name
    for f in fields:
        if "segmentid" in (f.name or "").lower():
            return f.name
    return None

def _resolve_targets(map_obj, expected_name):
    key = (expected_name or "").strip().lower()
    hits, seen = [], set()
    for lyr in _all_feature_layers(map_obj):
        toc_name = (lyr.name or "").strip()
        cat = _layer_catalog_path(lyr)
        base = _basename_no_ext(cat)
        if toc_name.lower() == key or base.lower() == key:
            dedupe = (cat or getattr(lyr, "longName", toc_name)).lower()
            if dedupe in seen:
                continue
            seen.add(dedupe)
            primary = lyr if getattr(lyr, "isFeatureLayer", False) else (cat or toc_name)
            pretty = f"{toc_name} [{base}]" if base and base.lower() != toc_name.lower() else toc_name
            hits.append((primary, cat, pretty))
    return hits

def _get_output_folder():
    try:
        aprx = arcpy.mp.ArcGISProject("CURRENT")
        home = aprx.homeFolder
        if home and os.path.isdir(home):
            return home
    except Exception:
        pass
    return arcpy.env.scratchFolder or os.getcwd()

def _open_csv(path, header):
    fh = open(path, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(fh, fieldnames=header)
    writer.writeheader()
    return fh, writer

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def run():
    aprx = arcpy.mp.ArcGISProject("CURRENT")
    m = aprx.activeMap
    if m is None:
        arcpy.AddWarning("No active map detected. Open a map and try again.")
        return

    # Output setup
    out_dir = _get_output_folder()
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    layers_csv = os.path.join(out_dir, f"{stamp}_layers_catalog.csv")
    edited_csv = os.path.join(out_dir, f"{stamp}_edited_rows.csv")
    skipped_csv = os.path.join(out_dir, f"{stamp}_not_edited.csv")

    # CSV schemas
    layers_header = [
        "layer_name", "dataset_basename", "catalog_path",
        "segment_field", "segment_prefix", "feature_count", "existing_max_counter"
    ]
    edited_header = [
        "layer_name", "dataset_basename", "catalog_path",
        "oid_field", "OBJECTID", "segment_field",
        "segment_prefix", "assigned_counter", "new_segment_id", "previous_value"
    ]
    skipped_header = [
        "target_rule_name", "segment_prefix", "status",
        "layer_name", "dataset_basename", "catalog_path",
        "reason", "details", "feature_count",
        "blank_cells_found", "preserved_existing_values", "existing_max_counter"
    ]

    # Open CSVs
    layers_fh, layers_writer = _open_csv(layers_csv, layers_header)
    edited_fh, edited_writer = _open_csv(edited_csv, edited_header)
    skipped_fh, skipped_writer = _open_csv(skipped_csv, skipped_header)

    # Processing
    try:
        for rule in SEGMENT_RULES:
            target_name = (rule.get("layer_name") or "").strip()
            prefix = (rule.get("segment_prefix") or "").strip()

            if not target_name or not prefix:
                skipped_writer.writerow({
                    "target_rule_name": target_name, "segment_prefix": prefix,
                    "status": "rule_skipped",
                    "layer_name": "", "dataset_basename": "", "catalog_path": "",
                    "reason": "missing_rule_fields",
                    "details": "layer_name or segment_prefix is empty",
                    "feature_count": 0, "blank_cells_found": 0,
                    "preserved_existing_values": 0, "existing_max_counter": 0
                })
                continue

            targets = _resolve_targets(m, target_name)
            if not targets:
                skipped_writer.writerow({
                    "target_rule_name": target_name, "segment_prefix": prefix,
                    "status": "not_found",
                    "layer_name": "", "dataset_basename": "", "catalog_path": "",
                    "reason": "layer_not_in_active_map",
                    "details": "Resolved by TOC name or dataset basename",
                    "feature_count": 0, "blank_cells_found": 0,
                    "preserved_existing_values": 0, "existing_max_counter": 0
                })
                continue

            for primary, catalog_path, pretty in targets:
                base = _basename_no_ext(catalog_path)
                # Determine field and OID name
                target_for_fields = catalog_path or primary
                if not target_for_fields or not arcpy.Exists(target_for_fields):
                    skipped_writer.writerow({
                        "target_rule_name": target_name, "segment_prefix": prefix,
                        "status": "data_source_missing",
                        "layer_name": pretty, "dataset_basename": base, "catalog_path": catalog_path or "",
                        "reason": "catalog_not_accessible",
                        "details": "Layer catalog path not available or not exists",
                        "feature_count": 0, "blank_cells_found": 0,
                        "preserved_existing_values": 0, "existing_max_counter": 0
                    })
                    continue

                seg_field = _find_segment_field(target_for_fields)
                if not seg_field:
                    skipped_writer.writerow({
                        "target_rule_name": target_name, "segment_prefix": prefix,
                        "status": "field_missing",
                        "layer_name": pretty, "dataset_basename": base, "catalog_path": catalog_path or "",
                        "reason": "segment_field_not_found",
                        "details": "Alias 'Segment ID' or name contains 'segmentid'",
                        "feature_count": 0, "blank_cells_found": 0,
                        "preserved_existing_values": 0, "existing_max_counter": 0
                    })
                    continue

                # Feature count
                try:
                    fc_count = int(arcpy.management.GetCount(primary)[0])
                except Exception:
                    fc_count = int(arcpy.management.GetCount(target_for_fields)[0])

                # OID field name
                try:
                    desc = arcpy.Describe(target_for_fields)
                    oid_field = desc.OIDFieldName
                except Exception:
                    oid_field = "OBJECTID"

                # Discover current max counter
                existing_max = 0
                preserved_existing = 0
                blanks_found = 0
                try:
                    with arcpy.da.SearchCursor(primary, [seg_field]) as sc:
                        for (val,) in sc:
                            n = _segment_counter(val, prefix)
                            if n is not None:
                                preserved_existing += 1
                                if n > existing_max:
                                    existing_max = n
                            elif val is None or str(val).strip() == "":
                                blanks_found += 1
                except Exception:
                    with arcpy.da.SearchCursor(target_for_fields, [seg_field]) as sc:
                        for (val,) in sc:
                            n = _segment_counter(val, prefix)
                            if n is not None:
                                preserved_existing += 1
                                if n > existing_max:
                                    existing_max = n
                            elif val is None or str(val).strip() == "":
                                blanks_found += 1

                # Write to layers catalog
                layers_writer.writerow({
                    "layer_name": pretty,
                    "dataset_basename": base,
                    "catalog_path": catalog_path or "",
                    "segment_field": seg_field,
                    "segment_prefix": prefix,
                    "feature_count": fc_count,
                    "existing_max_counter": existing_max
                })

                # Perform updates
                next_counter = existing_max + 1
                edited_rows_this_layer = 0

                # Attempt update via layer (honors selection); fallback to path
                def _update_cursor(target):
                    nonlocal next_counter, edited_rows_this_layer
                    with arcpy.da.UpdateCursor(target, [oid_field, seg_field]) as uc:
                        for oid, current in uc:
                            if current is None or str(current).strip() == "":
                                assigned = next_counter
                                new_id = _next_id(prefix, assigned)
                                prev_value = current
                                uc.updateRow((oid, new_id))
                                # Log edited row
                                edited_writer.writerow({
                                    "layer_name": pretty,
                                    "dataset_basename": base,
                                    "catalog_path": catalog_path or "",
                                    "oid_field": oid_field,
                                    "OBJECTID": oid,
                                    "segment_field": seg_field,
                                    "segment_prefix": prefix,
                                    "assigned_counter": assigned,
                                    "new_segment_id": new_id,
                                    "previous_value": "" if prev_value is None else str(prev_value)
                                })
                                next_counter += 1
                                edited_rows_this_layer += 1

                try:
                    _update_cursor(primary)
                except Exception:
                    _update_cursor(target_for_fields)

                # If no edits, record reason in not_edited
                if edited_rows_this_layer == 0:
                    status = "no_edits"
                    reason = "no_blank_cells_found" if blanks_found == 0 else "blanks_blocked"
                    details = "All rows already have values for this prefix" if blanks_found == 0 else "Blanks exist but were not writable"
                else:
                    status = "edited"
                    reason = ""
                    details = ""

                # Record the layer outcome in not_edited (even if edited, for QA rollup)
                skipped_writer.writerow({
                    "target_rule_name": target_name,
                    "segment_prefix": prefix,
                    "status": status,
                    "layer_name": pretty,
                    "dataset_basename": base,
                    "catalog_path": catalog_path or "",
                    "reason": reason,
                    "details": details,
                    "feature_count": fc_count,
                    "blank_cells_found": blanks_found,
                    "preserved_existing_values": preserved_existing,
                    "existing_max_counter": existing_max
                })

    finally:
        layers_fh.close()
        edited_fh.close()
        skipped_fh.close()

    arcpy.AddMessage(f"Logs written:\n  {layers_csv}\n  {edited_csv}\n  {skipped_csv}\n")
    arcpy.AddMessage("Processing complete.")

if __name__ == "__main__":
    run()
