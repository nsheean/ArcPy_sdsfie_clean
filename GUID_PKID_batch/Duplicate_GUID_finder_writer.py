# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
=====================================================================================
Script: Duplicate_GUID_finder_writer.py
Author: Nathanael Sheean
Date: 2025-09-14
Version: 4.0.0 (Active-map only, deep nested group aware, canonical 32-hex, managed rewrites)

Purpose:
  Detect duplicated GUID values across ALL fields that can carry GUIDs:
    • GlobalID (read-only, treated as master)
    • GUID typed fields
    • TEXT fields that contain GUID-like values
  Canonicalization:
    • Strip surrounding {} if present
    • Remove hyphens
    • Lowercase
    • Validate 32 hex chars
  Rewrite policy (in this order):
    1) Rewrite TEXT field duplicates to new unique GUIDs
    2) Then rewrite GUID field duplicates to new unique GUIDs
    GlobalID is never edited and anchors the duplicate set as the master.

Scope:
  - ACTIVE MAP only (deep traversal of group and nested sublayers).
  - Deduplicate datasets by catalogPath (same FC referenced many times is read once).

Outputs (to project home):
  • GUID_Duplicates_<ts>.csv            → every duplicate occurrence (value seen ≥ 2)
  • GUID_All_<ts>.csv                   → every discovered occurrence (provenance)
  • GUID_Layers_With_Fields_<ts>.csv    → datasets scanned + fields inspected
  • GUID_Layers_Without_Fields_<ts>.csv → datasets with no candidate fields
  • GUID_Skipped_<ts>.csv               → datasets/layers skipped with reasons
  • GUID_Updates_<ts>.csv               → all fields updated: where, old→new, and why

Safety:
  - Read-only scan phase; edit phase skips schema-locked datasets.
  - Never edits GlobalID fields.
=====================================================================================
"""

import arcpy, os, csv, re, uuid
from collections import OrderedDict, deque, defaultdict
from datetime import datetime

# ---------------- Project + paths ----------------
aprx = arcpy.mp.ArcGISProject("CURRENT")
active_map = aprx.activeMap
if active_map is None:
    raise RuntimeError("No active map. Activate a map and run again.")

PROJECT_HOME = aprx.homeFolder or os.path.dirname(aprx.defaultGeodatabase or "") or os.path.expanduser("~")
os.makedirs(PROJECT_HOME, exist_ok=True)
TS = datetime.now().strftime("%Y%m%d_%H%M%S")

CSV_DUPES   = os.path.join(PROJECT_HOME, f"GUID_Duplicates_{TS}.csv")
CSV_ALL     = os.path.join(PROJECT_HOME, f"GUID_All_{TS}.csv")
CSV_WITH    = os.path.join(PROJECT_HOME, f"GUID_Layers_With_Fields_{TS}.csv")
CSV_WITHOUT = os.path.join(PROJECT_HOME, f"GUID_Layers_Without_Fields_{TS}.csv")
CSV_SKIPPED = os.path.join(PROJECT_HOME, f"GUID_Skipped_{TS}.csv")
CSV_UPDATES = os.path.join(PROJECT_HOME, f"GUID_Updates_{TS}.csv")

def msg(s): arcpy.AddMessage(s)
def warn(s): arcpy.AddWarning(s)
def err(s): arcpy.AddError(s)

# ---------------- Traversal: deep groups + composites (ACTIVE MAP ONLY) -----
def iter_layers_deep(map_obj):
    """Depth-first traversal over the active map, GroupLayers, and composite layers; yields leaf layers."""
    stack = deque(map_obj.listLayers())
    while stack:
        lyr = stack.pop()
        # Descend into any layer that can contain children
        descended = False
        try:
            subs = lyr.listLayers()
            if subs:
                descended = True
                for sub in subs:
                    stack.append(sub)
        except Exception:
            pass
        if descended:
            continue
        yield lyr  # leaf layer

def is_concrete_feature_class(path):
    """Accept file/SDE/shapefile feature classes; reject services, joins, or tables."""
    if not path or not arcpy.Exists(path):
        return False
    try:
        d = arcpy.Describe(path)
        st = getattr(d, "shapeType", None)
        return st in ("Point", "Polyline", "Polygon", "Multipoint")
    except Exception:
        return False

def collect_unique_datasources_from_active_map():
    """
    Scan the ACTIVE MAP only. Traverse all groups and nested groups. Gather concrete FCs.
    Returns OrderedDict: catalogPath -> {'workspace':..., 'layer_names':set()}
    """
    collected = OrderedDict()
    for lyr in iter_layers_deep(active_map):
        try:
            if not hasattr(lyr, "supports") or not lyr.supports("DATASOURCE"):
                continue
            ds = lyr.dataSource
        except Exception:
            continue
        if not is_concrete_feature_class(ds):
            continue
        d = arcpy.Describe(ds)
        cat = d.catalogPath
        entry = collected.setdefault(cat, {"workspace": d.path, "layer_names": set()})
        entry["layer_names"].add(lyr.name)
    return collected

# ---------------- GUID normalization (canonical 32-hex) ----------------
HEX_RE = re.compile(r'^[0-9a-f]{32}$', re.IGNORECASE)
GUID_ANY_RE = re.compile(
    r'^\s*\{?([0-9a-fA-F]{8})-?([0-9a-fA-F]{4})-?([0-9a-fA-F]{4})-?([0-9a-fA-F]{4})-?([0-9a-fA-F]{12})\}?\s*$'
)

def canon32(value):
    """
    Return lowercase 32-hex canonical GUID or None.
    Accepts:
      {xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx}
      xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
      xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    """
    if value is None:
        return None
    s = str(value)
    m = GUID_ANY_RE.match(s)
    if not m:
        return None
    compact = (m.group(1) + m.group(2) + m.group(3) + m.group(4) + m.group(5)).lower()
    return compact if HEX_RE.match(compact) else None

def detect_style(value):
    """
    Return style hint for formatting new GUIDs back into a TEXT field:
    HYPHEN, COMPACT32, BRACED_HYPHEN, BRACED_COMPACT32
    """
    if value is None:
        return "HYPHEN"
    s = str(value).strip()
    braced = s.startswith("{") and s.endswith("}")
    core = s[1:-1].strip() if braced else s
    has_hyphen = "-" in core
    if braced and has_hyphen: return "BRACED_HYPHEN"
    if braced and not has_hyphen: return "BRACED_COMPACT32"
    if not braced and has_hyphen: return "HYPHEN"
    return "COMPACT32"

def hyphenate32(c32):
    """Convert 32-hex canonical to 36-char hyphenated."""
    return f"{c32[0:8]}-{c32[8:12]}-{c32[12:16]}-{c32[16:20]}-{c32[20:32]}"

def format_for_field(c32, field_type, original_value):
    """
    Return a string formatted for the field type while honoring original STYLE for TEXT fields.
    - GUID/GlobalID: return 36-char hyphenated string (no braces).
    - TEXT: preserve original style (braces and hyphens).
    """
    if field_type in ("GUID", "GlobalID"):
        return hyphenate32(c32)
    style = detect_style(original_value)
    if style == "HYPHEN":            return hyphenate32(c32)
    if style == "COMPACT32":         return c32
    if style == "BRACED_HYPHEN":     return "{" + hyphenate32(c32) + "}"
    if style == "BRACED_COMPACT32":  return "{" + c32 + "}"
    return hyphenate32(c32)

def test_schema_lock(path):
    try:
        return arcpy.TestSchemaLock(path)
    except Exception:
        return False

# ---------------- Main ------------------------------------------------------
def run():
    datasets = collect_unique_datasources_from_active_map()
    if not datasets:
        msg("Active map contains no eligible feature classes.")
        return

    msg(f"Eligible datasets in active map: {len(datasets)}")

    # Trackers
    with_fields, without_fields, skipped = [], [], []

    # canonical32 -> set of occurrences
    # occurrence tuple: (DatasetName, DatasetPath, FieldName, FieldType, OID, LayerRefs, OriginalValue)
    guid_occurrences = defaultdict(set)

    # Collect phase
    for cat_path, meta in datasets.items():
        d = arcpy.Describe(cat_path)
        ds_name = getattr(d, "name", os.path.basename(cat_path))
        ws = meta["workspace"]
        lyr_names = ";".join(sorted(meta["layer_names"]))

        # Scan is read-only; but some drivers throw if locked
        if not test_schema_lock(cat_path):
            skipped.append({"dataset": ds_name, "catalogPath": cat_path, "layers": lyr_names,
                            "reason": "Schema lock (exclusive edit/in use)"})
            warn(f"Skipped (lock): {cat_path}")
            continue

        fields = arcpy.ListFields(cat_path)
        if not fields:
            skipped.append({"dataset": ds_name, "catalogPath": cat_path, "layers": lyr_names,
                            "reason": "No fields returned"})
            continue

        oid_field = next((f.name for f in fields if f.type == "OID"), None)
        if not oid_field:
            skipped.append({"dataset": ds_name, "catalogPath": cat_path, "layers": lyr_names,
                            "reason": "No ObjectID field"})
            continue

        candidates = []
        for f in fields:
            if f.type in ("GUID", "GlobalID"):
                candidates.append((f.name, f.type, getattr(f, "length", None)))
            elif f.type == "String":
                flen = getattr(f, "length", None)
                if flen is None or flen >= 32:
                    candidates.append((f.name, f.type, flen))

        if not candidates:
            without_fields.append({"dataset": ds_name, "catalogPath": cat_path, "layers": lyr_names,
                                   "workspace": ws, "reason": "No GUID/GlobalID/Text candidates"})
            continue

        with_fields.append({"dataset": ds_name, "catalogPath": cat_path, "layers": lyr_names,
                            "workspace": ws, "fields": ";".join(n for n,_,__ in candidates)})

        field_names = [oid_field] + [n for n,_,__ in candidates]
        try:
            with arcpy.da.SearchCursor(cat_path, field_names) as cur:
                for row in cur:
                    oid = row[0]
                    for idx, (fname, ftype, flen) in enumerate(candidates, start=1):
                        raw = row[idx]
                        c32 = canon32(raw)
                        if c32 is None:
                            continue
                        guid_occurrences[c32].add((ds_name, cat_path, fname, ftype, oid, lyr_names, str(raw)))
        except Exception as ex:
            skipped.append({"dataset": ds_name, "catalogPath": cat_path, "layers": lyr_names,
                            "reason": f"SearchCursor error: {ex}"})
            continue

    # Compute duplicates on canonical 32-hex
    duplicates = {g: occ for g, occ in guid_occurrences.items() if len(occ) > 1}

    # Build a global set of used canonical GUIDs to avoid generating collisions
    used_canon = set(guid_occurrences.keys())

    # Plan updates respecting priority: TEXT first, then GUID; never change GlobalID
    # Build per-dataset update plan: dataset -> OID -> list[(field_name, new_value, reason)]
    updates_plan = defaultdict(lambda: defaultdict(list))
    updates_plan_meta = defaultdict(dict)  # dataset -> meta { 'layers':..., 'name':... }

    for c32, occs in duplicates.items():
        occ_list = list(occs)

        # Partition occurrences by field type
        globalid_occs = [o for o in occ_list if o[3] == "GlobalID"]
        guid_occs     = [o for o in occ_list if o[3] == "GUID"]
        text_occs     = [o for o in occ_list if o[3] == "String"]

        # Determine master set: any GlobalID occurrences are masters; else one master among GUIDs; else one among TEXT
        if globalid_occs:
            masters = set(globalid_occs)
            master_reason = "GlobalID anchors"
        elif guid_occs:
            masters = {sorted(guid_occs, key=lambda x: (x[1], x[2], x[4]))[0]}
            master_reason = "First GUID occurrence anchors"
        else:
            masters = {sorted(text_occs, key=lambda x: (x[1], x[2], x[4]))[0]}
            master_reason = "First TEXT occurrence anchors"

        # Targets: everything not in masters, but in priority order
        targets_text = [o for o in text_occs if o not in masters]
        targets_guid = [o for o in guid_occs if o not in masters]
        # GlobalID never edited

        # Generate and stage updates for TEXT targets first
        for ds_name, cat_path, fname, ftype, oid, layers, original in targets_text:
            # Generate new non-colliding guid
            new_c32 = None
            while True:
                new_c32_try = uuid.uuid4().hex  # 32-hex lowercase
                if new_c32_try not in used_canon:
                    new_c32 = new_c32_try
                    used_canon.add(new_c32)
                    break
            new_value = format_for_field(new_c32, ftype, original)
            reason = f"Duplicate of canonical {c32}; {master_reason}; TEXT field rewritten"
            updates_plan[cat_path][oid].append((fname, new_value, reason))
            updates_plan_meta[cat_path] = {"layers": layers, "name": ds_name}

        # Then stage updates for GUID targets
        for ds_name, cat_path, fname, ftype, oid, layers, original in targets_guid:
            new_c32 = None
            while True:
                new_c32_try = uuid.uuid4().hex
                if new_c32_try not in used_canon:
                    new_c32 = new_c32_try
                    used_canon.add(new_c32)
                    break
            new_value = format_for_field(new_c32, ftype, original)
            reason = f"Duplicate of canonical {c32}; {master_reason}; GUID field rewritten"
            updates_plan[cat_path][oid].append((fname, new_value, reason))
            updates_plan_meta[cat_path] = {"layers": layers, "name": ds_name}

    # Execute updates per dataset (skip locked), record update results
    updates_rows = []
    for cat_path, oid_changes in updates_plan.items():
        # Check write lock
        if not test_schema_lock(cat_path):
            for oid, changes in oid_changes.items():
                for (fname, new_value, reason) in changes:
                    updates_rows.append({
                        "dataset": updates_plan_meta[cat_path].get("name",""),
                        "catalogPath": cat_path,
                        "layers": updates_plan_meta[cat_path].get("layers",""),
                        "OID": oid,
                        "fieldName": fname,
                        "oldValue": "<unread during update>",
                        "newValue": new_value,
                        "status": "Skipped",
                        "why": "Schema lock (cannot edit)",
                        "rationale": reason
                    })
            warn(f"Skipped edits (lock): {cat_path}")
            continue

        # Determine unique field list for cursor
        all_fields = sorted({fname for changes in oid_changes.values() for (fname, _, _) in changes})
        # Include OID at the front
        # Find actual OID field name
        fields = arcpy.ListFields(cat_path)
        oid_field = next((f.name for f in fields if f.type == "OID"), None)
        if not oid_field:
            for oid, changes in oid_changes.items():
                for (fname, new_value, reason) in changes:
                    updates_rows.append({
                        "dataset": updates_plan_meta[cat_path].get("name",""),
                        "catalogPath": cat_path,
                        "layers": updates_plan_meta[cat_path].get("layers",""),
                        "OID": oid,
                        "fieldName": fname,
                        "oldValue": "<unavailable>",
                        "newValue": new_value,
                        "status": "Skipped",
                        "why": "No ObjectID field",
                        "rationale": reason
                    })
            continue

        cursor_fields = [oid_field] + all_fields

        # Build quick lookup for OID -> {fieldName: (newValue, reason)}
        plan_by_oid = {oid: {fn: (nv, rsn) for (fn, nv, rsn) in changes} for oid, changes in oid_changes.items()}

        try:
            with arcpy.da.UpdateCursor(cat_path, cursor_fields) as ucur:
                for row in ucur:
                    oid = row[0]
                    field_map = plan_by_oid.get(oid)
                    if not field_map:
                        continue
                    # Record old values and apply new
                    for idx, fn in enumerate(all_fields, start=1):
                        if fn in field_map:
                            old_val = row[idx]
                            new_val, rationale = field_map[fn]
                            row[idx] = new_val
                            updates_rows.append({
                                "dataset": updates_plan_meta[cat_path].get("name",""),
                                "catalogPath": cat_path,
                                "layers": updates_plan_meta[cat_path].get("layers",""),
                                "OID": oid,
                                "fieldName": fn,
                                "oldValue": str(old_val),
                                "newValue": str(new_val),
                                "status": "Success",
                                "why": "Duplicate resolution",
                                "rationale": rationale
                            })
                    ucur.updateRow(row)
        except Exception as ex:
            # Log failure for all planned changes in this dataset
            for oid, changes in oid_changes.items():
                for (fname, new_value, reason) in changes:
                    updates_rows.append({
                        "dataset": updates_plan_meta[cat_path].get("name",""),
                        "catalogPath": cat_path,
                        "layers": updates_plan_meta[cat_path].get("layers",""),
                        "OID": oid,
                        "fieldName": fname,
                        "oldValue": "<unread due to error>",
                        "newValue": new_value,
                        "status": "Failed",
                        "why": f"UpdateCursor error: {ex}",
                        "rationale": reason
                    })
            err(f"Update failed for {cat_path}: {ex}")

    # ---------------- Write CSVs ----------------
    # 1) Duplicates
    with open(CSV_DUPES, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Canonical32Hex", "DatasetName", "DatasetPath", "FieldName", "FieldType", "ObjectID", "LayerRefs", "OriginalValue"])
        if duplicates:
            for c32, occs in sorted(duplicates.items()):
                for ds_name, cat_path, fname, ftype, oid, layers, original in sorted(occs):
                    w.writerow([c32, ds_name, cat_path, fname, ftype, oid, layers, original])
        else:
            w.writerow(["No duplicate GUIDs found.", "", "", "", "", "", "", ""])

    # 2) All GUIDs (provenance)
    with open(CSV_ALL, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Canonical32Hex", "DatasetName", "DatasetPath", "FieldName", "FieldType", "ObjectID", "LayerRefs", "OriginalValue"])
        for c32, occs in sorted(guid_occurrences.items()):
            for ds_name, cat_path, fname, ftype, oid, layers, original in sorted(occs):
                w.writerow([c32, ds_name, cat_path, fname, ftype, oid, layers, original])

    # 3) Datasets with candidate fields
    with open(CSV_WITH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["dataset","catalogPath","workspace","layers","fields"])
        w.writeheader()
        w.writerows(with_fields)

    # 4) Datasets without candidate fields
    with open(CSV_WITHOUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["dataset","catalogPath","workspace","layers","reason"])
        w.writeheader()
        w.writerows(without_fields)

    # 5) Skipped
    with open(CSV_SKIPPED, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["dataset","catalogPath","layers","reason"])
        w.writeheader()
        w.writerows(skipped)

    # 6) Updates
    with open(CSV_UPDATES, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["dataset","catalogPath","layers","OID","fieldName","oldValue","newValue","status","why","rationale"]
        )
        w.writeheader()
        w.writerows(updates_rows)

    # ---------------- Summary ----------------
    total_occ = sum(len(v) for v in guid_occurrences.values())
    msg(f"Datasets scanned: {len(with_fields) + len(without_fields)}")
    msg(f"GUID occurrences discovered: {total_occ}")
    msg(f"Duplicate canonical GUIDs: {len(duplicates)}")
    msg(f"Duplicates CSV: {CSV_DUPES}")
    msg(f"All GUIDs CSV: {CSV_ALL}")
    msg(f"With-fields CSV: {CSV_WITH}")
    msg(f"Without-fields CSV: {CSV_WITHOUT}")
    msg(f"Skipped CSV: {CSV_SKIPPED}")
    msg(f"Updates CSV: {CSV_UPDATES}")

if __name__ == "__main__":
    run()
