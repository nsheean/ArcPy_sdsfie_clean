# -*- coding: utf-8 -*-
# SPDX-License-Identifier: MIT
# Copyright (c) 2025 Nathanael Sheean
"""
=====================================================================================
Script: AggregateGeometryByFacility.py
Author: Nathanael Sheean
Date: 2025-09-14
Version: 2.12 (Hardcoded CSV path; CSV field validation; whitespace-trim audit; blank-on-null GIS)

Purpose:
  Aggregate area and length per facility number from layers in the ACTIVE MAP and
  append the totals back to a selected Nexgen CSV pull report. Excludes features with
  Operational Status = 'Abandoned' or Owner in the excluded set. De-duplicates across
  multiple layers that reference the same dataset.

Active map only:
  - Traverses nested group layers.
  - Reads from dataset paths to avoid field masking by joins.

Outputs (written next to the input CSV):
  - <input>_appended_<ts>.csv
  - <input>_included_features_audit_<ts>.csv
  - <input>_excluded_features_audit_<ts>.csv
  - <input>_space_trim_audit_<ts>.csv
  - geometry_extraction_log_<ts>.txt (log file)

Behavioral guarantees:
  - Retains the Nexgen CSV exactly; only appends new columns.
  - Unmatched facilities → appended fields are blank.
  - Matched facilities with no GIS contributions (all null/empty) → TotalArea/TotalLength remain blank.
  - Matched facilities with real zeros → write "0.00" (distinguishes zero from empty).
=====================================================================================
"""

import arcpy
import os
import csv
import datetime
import logging

# ===================== HARD-CODED INPUT CSV =====================
# Replace the placeholder below with the absolute path to your Nexgen pull CSV.
INPUT_CSV = r"path\file.csv"  # <-- REQUIRED: set to your actual CSV path

# Facility-number header candidates in the Nexgen CSV (first match is used).
CSV_FACNUM_HEADERS = ["Fac Nbr", "Facility Number", "FacilityNumber"]

# Owners to exclude after domain decoding (case-insensitive).
EXCLUDED_OWNERS = {"host nation", "hn"}
# ================================================================

run_ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

# Validate CSV path (abort if placeholder not replaced)
if INPUT_CSV.strip().lower() == r"path\file.csv":
    arcpy.AddError("Update INPUT_CSV to your actual Nexgen CSV path (e.g., r'C:{NETWORK_PATH}\\\nexgen\\pull_2025-09-14.csv').")
    raise SystemExit(1)
if not os.path.isfile(INPUT_CSV) or not INPUT_CSV.lower().endswith(".csv"):
    arcpy.AddError(f"CSV not found or invalid: {INPUT_CSV}")
    raise SystemExit(1)

# Outputs alongside input CSV
_base = os.path.splitext(INPUT_CSV)[0]
output_csv           = _base + f"_appended_{run_ts}.csv"
included_audit_csv   = _base + f"_included_features_audit_{run_ts}.csv"
excluded_audit_csv   = _base + f"_excluded_features_audit_{run_ts}.csv"
space_trim_audit_csv = _base + f"_space_trim_audit_{run_ts}.csv"

# ==== LOGGING (alongside CSV) ====
log_filename = os.path.join(
    os.path.dirname(INPUT_CSV),
    f"geometry_extraction_log_{run_ts}.txt"
)
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logging.info("=== Script started ===")

# ==== PROJECT CONTEXT ====
aprx = arcpy.mp.ArcGISProject("CURRENT")
active_map = aprx.activeMap
if active_map is None:
    arcpy.AddError("No active map. Open a map and re-run.")
    raise SystemExit(2)

# ===================== HELPERS =====================

def iter_feature_layers(container):
    """Depth-first traversal to leaf feature layers."""
    from collections import deque
    stack = deque(container.listLayers() if hasattr(container, "listLayers") else [])
    while stack:
        lyr = stack.pop()
        try:
            subs = lyr.listLayers()
            if subs:
                for s in subs:
                    stack.append(s)
                continue
        except Exception:
            pass
        if getattr(lyr, "isFeatureLayer", False):
            yield lyr

def get_matching_field(fields, candidates):
    for cand in candidates:
        for f in fields:
            if f.name.lower() == cand.lower():
                return f.name
    return None

def make_field_lookup(fields):
    return {f.name.lower(): f for f in fields}

def is_double_field(field_lookup, name):
    if not name:
        return False
    fld = field_lookup.get(name.lower())
    return bool(fld and getattr(fld, "type", "").lower() == "double")

def build_alias_lookup(fields):
    return {f.name: (f.aliasName or "").strip().lower() for f in fields}

def get_field_by_alias(fields, wanted_aliases):
    wanted = {a.strip().lower() for a in wanted_aliases}
    for f in fields:
        alias = (f.aliasName or "").strip().lower()
        if alias in wanted:
            return f.name
    return None

def dedup_keep_order(seq):
    seen = set(); out = []
    for x in seq:
        if x and x not in seen:
            out.append(x); seen.add(x)
    return out

def fetch_cell_from_dataset(dataset, oid_field, oid_value, field_name):
    where = f"{arcpy.AddFieldDelimiters(dataset, oid_field)} = {oid_value}"
    with arcpy.da.SearchCursor(dataset, [field_name], where) as c:
        for r in c:
            return r[0]
    return None

def build_domain_decoder(workspace, fields):
    decoder = {}
    try:
        domains = {d.name: d for d in arcpy.da.ListDomains(workspace)}
    except Exception as e:
        logging.debug(f"ListDomains failed for '{workspace}': {e}")
        return decoder
    for f in fields:
        dname = getattr(f, "domain", None)
        if not dname:
            continue
        d = domains.get(dname)
        if hasattr(d, "codedValues") and d.codedValues:
            mapping = {str(k).strip().lower(): str(v).strip().lower() for k, v in d.codedValues.items()}
            decoder[f.name.lower()] = mapping
    return decoder

def decode_with_domain(field_name, raw_value, field_domain_maps):
    if raw_value in (None, "", "Null"):
        return ""
    mapping = field_domain_maps.get(field_name.lower()) if field_name else None
    if mapping:
        key = str(raw_value).strip().lower()
        return mapping.get(key, str(raw_value).strip().lower())
    return str(raw_value).strip().lower()

def make_dataset_key(path):
    try:
        return os.path.normcase(os.path.normpath(path))
    except Exception:
        return str(path).lower()

def norm_fac_value(value):
    """Return (lower_stripped, was_trimmed, stripped_value, original_string)."""
    s = "" if value is None else str(value)
    stripped = s.strip()
    was_trimmed = (stripped != s)
    return stripped.lower(), was_trimmed, stripped, s

# ===================== LOAD CSV + VALIDATE =====================

with open(INPUT_CSV, 'r', encoding='utf-8-sig', newline='') as f:
    reader = csv.reader(f)
    try:
        header = next(reader)
    except StopIteration:
        arcpy.AddError("CSV has no header or rows.")
        raise SystemExit(3)

# Find facility-number column
fac_col_name = None
header_lc = [h.strip().lower() for h in header]
for cand in CSV_FACNUM_HEADERS:
    if cand.strip().lower() in header_lc:
        fac_col_name = header[header_lc.index(cand.strip().lower())]
        break

if fac_col_name is None:
    arcpy.AddError(
        "Mandatory facility-number column not found in CSV. "
        f"CSV must include one of: {', '.join(CSV_FACNUM_HEADERS)}."
    )
    raise SystemExit(4)

# Re-open via DictReader to keep all columns
csv_data = {}
space_trim_rows = []  # audit of whitespace trims on CSV and dataset sides
row_count = 0
with open(INPUT_CSV, 'r', encoding='utf-8-sig', newline='') as f:
    reader = csv.DictReader(f)
    if fac_col_name not in reader.fieldnames:
        arcpy.AddError(
            f"CSV field '{fac_col_name}' missing in DictReader. "
            "Confirm the CSV delimiter and headers."
        )
        raise SystemExit(5)
    for idx, row in enumerate(reader, start=2):  # start=2 to account for header on row 1
        row_count += 1
        key_norm, was_trimmed, stripped, original = norm_fac_value(row.get(fac_col_name, ""))
        if was_trimmed:
            space_trim_rows.append({
                "Source": "CSV",
                "Location": os.path.basename(INPUT_CSV),
                "Context": f"row={idx}",
                "OriginalValue": original,
                "TrimmedValue": stripped
            })
        if key_norm:
            csv_data[key_norm] = row

if row_count == 0:
    arcpy.AddError("CSV contains a header but no data rows.")
    raise SystemExit(6)

logging.info(f"CSV loaded: {INPUT_CSV} | rows={row_count} | fac_col='{fac_col_name}' | unique keys={len(csv_data)}")

# ===================== AGG STATE & AUDIT BUFFERS =====================

results = {}  # fac -> aggregates
included_audit_rows = []
excluded_audit_rows = []

processed_oids = set()   # {(dataset_key, OBJECTID)}
first_seen_layer = {}    # {(dataset_key, OBJECTID): layer_name}

# ===================== PROCESS ACTIVE MAP =====================

for lyr in iter_feature_layers(active_map):
    try:
        desc_layer = arcpy.Describe(lyr)
        # Skip services and joined layers; dataset-level read is required
        data_src = (getattr(desc_layer, "dataSource", "") or "").lower()
        if any(s in data_src for s in (".mapserver", ".featureserver", "/wms", "/wmts")) or bool(getattr(desc_layer, "hasJoin", False)):
            excluded_audit_rows.append({
                "RunId": run_ts, "LayerName": lyr.name, "OBJECTID": "",
                "FacilityNumberRaw": "", "RPUID": "", "CategoryCode": "",
                "OperationalStatus": "", "Owner": "", "AreaSizeRaw": "", "LengthSizeRaw": "",
                "Reason": "Service or joined layer not eligible for dataset read"
            })
            continue

        dataset_path = desc_layer.catalogPath
        if not dataset_path or not arcpy.Exists(dataset_path):
            excluded_audit_rows.append({
                "RunId": run_ts, "LayerName": lyr.name, "OBJECTID": "",
                "FacilityNumberRaw": "", "RPUID": "", "CategoryCode": "",
                "OperationalStatus": "", "Owner": "", "AreaSizeRaw": "", "LengthSizeRaw": "",
                "Reason": "Dataset path not found"
            })
            continue

        dataset_key = make_dataset_key(dataset_path)
        dataset_fields = arcpy.ListFields(dataset_path)
        field_lookup = make_field_lookup(dataset_fields)
        alias_lookup = build_alias_lookup(dataset_fields)
        ws = arcpy.Describe(dataset_path).path

        # Domain maps
        dataset_domain_maps = build_domain_decoder(ws, dataset_fields)
        layer_fields = desc_layer.fields if hasattr(desc_layer, "fields") else arcpy.ListFields(lyr)
        layer_domain_maps = build_domain_decoder(ws, layer_fields)

        # Dataset field matches
        f_rpuid   = get_matching_field(dataset_fields, ["rpuid","RPUID"])
        f_facnum  = get_matching_field(dataset_fields, ["facilityNumber"])
        f_catcode = get_matching_field(dataset_fields, ["categoryCode"])
        f_area    = get_matching_field(dataset_fields, ["areaSize"])
        f_owner   = get_matching_field(dataset_fields, ["owner"])
        f_status  = get_matching_field(dataset_fields, ["operationalStatus"])
        f_area_u  = get_matching_field(dataset_fields, ["areaSizeUom"])

        # Fallbacks on layer if missing on dataset
        f_owner_layer  = None if f_owner else get_matching_field(layer_fields, ["owner"])
        f_status_layer = None if f_status else get_matching_field(layer_fields, ["operationalStatus"])

        # Length candidates
        cand_len_primary   = get_matching_field(dataset_fields, ["lengthSize"])
        cand_len_fallback  = get_matching_field(dataset_fields, ["measuredLength"])
        primary_is_double  = is_double_field(field_lookup, cand_len_primary)
        fallback_is_double = is_double_field(field_lookup, cand_len_fallback)
        if primary_is_double and fallback_is_double:
            f_length, length_choice_note = cand_len_primary, "selected 'lengthSize' (Double; both candidates Double)"
        elif primary_is_double:
            f_length, length_choice_note = cand_len_primary, "selected 'lengthSize' (Double)"
        elif fallback_is_double:
            f_length, length_choice_note = cand_len_fallback, "selected fallback 'measuredLength' (Double)"
        else:
            f_length, length_choice_note = (cand_len_primary if cand_len_primary else cand_len_fallback,
                                            "no length field available" if not (cand_len_primary or cand_len_fallback)
                                            else ("selected 'lengthSize' (non-Double)" if cand_len_primary and cand_len_primary.lower() == "lengthsize"
                                                  else "selected fallback 'measuredLength' (non-Double)"))
        if f_length and f_length.lower() == "measuredlength":
            f_length_u = get_matching_field(dataset_fields, ["measuredLengthUom"]) or get_matching_field(dataset_fields, ["lengthSizeUom"])
        else:
            f_length_u = get_matching_field(dataset_fields, ["lengthSizeUom"]) or get_matching_field(dataset_fields, ["measuredLengthUom"])

        # Facility number candidates (alias-aware)
        f_facnum_alias = get_field_by_alias(dataset_fields, ["Facility Number","FacilityNumber","Fac Nbr"])
        catcode_is_fac = bool(f_catcode and alias_lookup.get(f_catcode, "") == "facility number")
        facnum_candidates = dedup_keep_order([f_facnum, f_facnum_alias, f_catcode if catcode_is_fac else None])

        if not facnum_candidates:
            excluded_audit_rows.append({
                "RunId": run_ts, "LayerName": lyr.name, "OBJECTID": "",
                "FacilityNumberRaw": "", "RPUID": "", "CategoryCode": "",
                "OperationalStatus": "", "Owner": "", "AreaSizeRaw": "", "LengthSizeRaw": "",
                "Reason": "No facility-number candidates found in dataset fields"
            })
            continue

        # OIDs and re{BASE_CODE}le field list
        oid_dataset = arcpy.Describe(dataset_path).OIDFieldName
        oid_layer   = arcpy.Describe(lyr).OIDFieldName  # usually same
        fields_to_read = [oid_dataset] + dedup_keep_order([
            f_rpuid, f_catcode, f_area, f_length, f_owner, f_status, f_area_u, f_length_u
        ] + facnum_candidates)

        # Respect layer definition query if valid on dataset
        layer_def = getattr(lyr, "definitionQuery", None)
        where_for_dataset = layer_def
        try:
            with arcpy.da.SearchCursor(dataset_path, [oid_dataset], where_for_dataset):
                pass
        except Exception:
            where_for_dataset = None

        # Cache owner/status from the layer if they are only on the layer
        owner_cache, status_cache = {}, {}
        if f_owner_layer or f_status_layer:
            layer_fields_to_read = [oid_layer] + [n for n in [f_owner_layer, f_status_layer] if n]
            with arcpy.da.SearchCursor(lyr, layer_fields_to_read, layer_def) as lc:
                for lr in lc:
                    rd = dict(zip(layer_fields_to_read, lr))
                    oidv = rd.get(oid_layer)
                    if f_owner_layer:
                        owner_cache[oidv] = rd.get(f_owner_layer, "")
                    if f_status_layer:
                        status_cache[oidv] = rd.get(f_status_layer, "")

        # Main dataset read
        with arcpy.da.SearchCursor(dataset_path, fields_to_read, where_for_dataset) as cursor:
            for row in cursor:
                row_dict = dict(zip(fields_to_read, row))
                oid_val = row_dict.get(oid_dataset)

                # Facility number coalesce
                fac_raw_value = None
                fac_src = None
                for fn in facnum_candidates:
                    if fn in row_dict:
                        v = row_dict[fn]
                        if v not in (None, "", "Null"):
                            fac_raw_value = v
                            fac_src = fn
                            break

                key_norm, was_trimmed, stripped, original = norm_fac_value(fac_raw_value)
                if was_trimmed:
                    space_trim_rows.append({
                        "Source": "Dataset",
                        "Location": os.path.basename(dataset_path),
                        "Context": f"layer={lyr.name}; OID={oid_val}; field={fac_src}",
                        "OriginalValue": original,
                        "TrimmedValue": stripped
                    })

                if not key_norm:
                    excluded_audit_rows.append({
                        "RunId": run_ts, "LayerName": lyr.name, "OBJECTID": oid_val,
                        "FacilityNumberRaw": str(fac_raw_value).strip(),
                        "RPUID": row_dict.get(f_rpuid, ""), "CategoryCode": row_dict.get(f_catcode, ""),
                        "OperationalStatus": "", "Owner": "", "AreaSizeRaw": row_dict.get(f_area, ""),
                        "LengthSizeRaw": row_dict.get(f_length, ""),
                        "Reason": "Empty or NULL facility number"
                    })
                    continue

                # Status decode
                if f_status:
                    status_raw = row_dict.get(f_status, "")
                    status_val = decode_with_domain(f_status, status_raw, dataset_domain_maps)
                else:
                    status_raw = status_cache.get(oid_val, "")
                    status_val = decode_with_domain(f_status_layer, status_raw, layer_domain_maps)

                # Owner decode
                if f_owner:
                    owner_raw = row_dict.get(f_owner, "")
                    owner_val = decode_with_domain(f_owner, owner_raw, dataset_domain_maps)
                else:
                    owner_raw = owner_cache.get(oid_val, "")
                    owner_val = decode_with_domain(f_owner_layer, owner_raw, layer_domain_maps)

                # De-dup across layers that reference same dataset
                dup_key = (make_dataset_key(dataset_path), oid_val)
                if dup_key in processed_oids:
                    excluded_audit_rows.append({
                        "RunId": run_ts, "LayerName": lyr.name, "OBJECTID": oid_val,
                        "FacilityNumberRaw": stripped,
                        "RPUID": row_dict.get(f_rpuid, ""), "CategoryCode": row_dict.get(f_catcode, ""),
                        "OperationalStatus": status_val, "Owner": owner_val,
                        "AreaSizeRaw": row_dict.get(f_area, ""), "LengthSizeRaw": row_dict.get(f_length, ""),
                        "Reason": f"Duplicate feature in dataset; first_seen_layer='{first_seen_layer[dup_key]}'."
                    })
                    continue
                else:
                    processed_oids.add(dup_key)
                    first_seen_layer[dup_key] = lyr.name

                # CSV match
                if key_norm not in csv_data:
                    excluded_audit_rows.append({
                        "RunId": run_ts, "LayerName": lyr.name, "OBJECTID": oid_val,
                        "FacilityNumberRaw": stripped,
                        "RPUID": row_dict.get(f_rpuid, ""), "CategoryCode": row_dict.get(f_catcode, ""),
                        "OperationalStatus": status_val, "Owner": owner_val,
                        "AreaSizeRaw": row_dict.get(f_area, ""), "LengthSizeRaw": row_dict.get(f_length, ""),
                        "Reason": f"Facility number not found in CSV '{fac_col_name}'"
                    })
                    continue

                # Status exclude
                if status_val == "abandoned":
                    excluded_audit_rows.append({
                        "RunId": run_ts, "LayerName": lyr.name, "OBJECTID": oid_val,
                        "FacilityNumberRaw": stripped,
                        "RPUID": row_dict.get(f_rpuid, ""), "CategoryCode": row_dict.get(f_catcode, ""),
                        "OperationalStatus": status_val, "Owner": owner_val,
                        "AreaSizeRaw": row_dict.get(f_area, ""), "LengthSizeRaw": row_dict.get(f_length, ""),
                        "Reason": "OperationalStatus equals 'Abandoned'"
                    })
                    continue

                # Owner exclude
                if owner_val in EXCLUDED_OWNERS:
                    excluded_audit_rows.append({
                        "RunId": run_ts, "LayerName": lyr.name, "OBJECTID": oid_val,
                        "FacilityNumberRaw": stripped,
                        "RPUID": row_dict.get(f_rpuid, ""), "CategoryCode": row_dict.get(f_catcode, ""),
                        "OperationalStatus": status_val, "Owner": owner_val,
                        "AreaSizeRaw": row_dict.get(f_area, ""), "LengthSizeRaw": row_dict.get(f_length, ""),
                        "Reason": f"Owner equals '{owner_val}' (excluded set)"
                    })
                    continue

                # Recovery for masked nulls
                if f_area and row_dict.get(f_area) is None:
                    rec = fetch_cell_from_dataset(dataset_path, oid_dataset, oid_val, f_area)
                    if rec is not None:
                        row_dict[f_area] = rec
                if f_length and row_dict.get(f_length) is None:
                    rec = fetch_cell_from_dataset(dataset_path, oid_dataset, oid_val, f_length)
                    if rec is not None:
                        row_dict[f_length] = rec

                # Aggregate with contribution tracking
                area   = row_dict.get(f_area)
                length = row_dict.get(f_length)
                rpuid  = row_dict.get(f_rpuid)
                cat    = row_dict.get(f_catcode)
                area_u = str(row_dict.get(f_area_u, "")).strip() if f_area_u else ""
                len_u  = str(row_dict.get(f_length_u, "")).strip() if f_length_u else ""

                if key_norm not in results:
                    results[key_norm] = {
                        "total_area": 0.0, "total_length": 0.0, "layers": set(),
                        "rpuids": set(), "catcodes": set(), "area_uoms": set(), "length_uoms": set(),
                        "area_seen": 0, "length_seen": 0
                    }

                if area not in (None, "", "Null"):
                    try:
                        results[key_norm]["total_area"] += float(area)
                        results[key_norm]["area_seen"] += 1
                    except Exception:
                        pass
                if length not in (None, "", "Null"):
                    try:
                        results[key_norm]["total_length"] += float(length)
                        results[key_norm]["length_seen"] += 1
                    except Exception:
                        pass

                results[key_norm]["layers"].add(lyr.name)
                if rpuid:  results[key_norm]["rpuids"].add(str(rpuid))
                if cat:    results[key_norm]["catcodes"].add(str(cat))
                if area_u: results[key_norm]["area_uoms"].add(area_u)
                if len_u:  results[key_norm]["length_uoms"].add(len_u)

                included_audit_rows.append({
                    "RunId": run_ts, "LayerName": lyr.name, "OBJECTID": oid_val,
                    "FacilityNumber": stripped, "RPUID": rpuid, "CategoryCode": cat,
                    "AreaSize": area, "LengthSize": length,
                    "AreaSizeUOM": area_u, "LengthUOM": len_u,
                    "Reason": f"Matched CSV '{fac_col_name}'; {length_choice_note}; facilityNumber_source={fac_src}"
                })

        logging.info(f"Processed layer: {lyr.name}")

    except Exception as e:
        logging.error(f"Error processing layer {lyr.name}: {e}")

# ===================== WRITE OUTPUTS =====================

# Appended CSV mirrors original columns and adds totals and provenance columns
with open(INPUT_CSV, 'r', encoding='utf-8-sig', newline='') as src, \
     open(output_csv, 'w', encoding='utf-8-sig', newline='') as out_csv:

    reader = csv.DictReader(src)
    base_fields = list(reader.fieldnames or [])
    writer = csv.DictWriter(out_csv, fieldnames=base_fields + [
        "TotalArea", "TotalLength", "SourceLayers", "RPUIDs", "CategoryCodes", "AreaUOMs", "LengthUOMs"
    ])
    writer.writeheader()

    for row in reader:
        key_norm, _, _, _ = norm_fac_value(row.get(fac_col_name, ""))

        if key_norm in results:
            agg = results[key_norm]
            total_area   = "" if agg.get("area_seen", 0)   == 0 else f"{agg.get('total_area', 0.0):.2f}"
            total_length = "" if agg.get("length_seen", 0) == 0 else f"{agg.get('total_length', 0.0):.2f}"

            writer.writerow({
                **row,
                "TotalArea": total_area,
                "TotalLength": total_length,
                "SourceLayers": "; ".join(sorted(agg.get("layers", []))),
                "RPUIDs": "; ".join(sorted(agg.get("rpuids", []))),
                "CategoryCodes": "; ".join(sorted(agg.get("catcodes", []))),
                "AreaUOMs": "; ".join(sorted(agg.get("area_uoms", []))),
                "LengthUOMs": "; ".join(sorted(agg.get("length_uoms", [])))
            })
        else:
            # No GIS match → keep every original CSV column intact and leave appended fields BLANK
            writer.writerow({
                **row,
                "TotalArea": "",
                "TotalLength": "",
                "SourceLayers": "",
                "RPUIDs": "",
                "CategoryCodes": "",
                "AreaUOMs": "",
                "LengthUOMs": ""
            })

# Included features audit
included_audit_rows.sort(key=lambda r: (r["LayerName"], r["OBJECTID"]))
with open(included_audit_csv, 'w', encoding='utf-8-sig', newline='') as inc_csv:
    fieldnames = ["RunId", "LayerName", "OBJECTID", "FacilityNumber", "RPUID",
                  "CategoryCode", "AreaSize", "LengthSize", "AreaSizeUOM",
                  "LengthUOM", "Reason"]
    writer = csv.DictWriter(inc_csv, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(included_audit_rows)

# Excluded features audit
excluded_audit_rows.sort(key=lambda r: (r["LayerName"], r["OBJECTID"]))
with open(excluded_audit_csv, 'w', encoding='utf-8-sig', newline='') as exc_csv:
    fieldnames = ["RunId", "LayerName", "OBJECTID", "FacilityNumberRaw", "RPUID",
                  "CategoryCode", "OperationalStatus", "Owner", "AreaSizeRaw",
                  "LengthSizeRaw", "Reason"]
    writer = csv.DictWriter(exc_csv, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(excluded_audit_rows)

# Space-trim audit (CSV + Dataset sides)
with open(space_trim_audit_csv, 'w', encoding='utf-8-sig', newline='') as spa_csv:
    fieldnames = ["Source", "Location", "Context", "OriginalValue", "TrimmedValue"]
    writer = csv.DictWriter(spa_csv, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(space_trim_rows)

# ==== LOG COMPLETION ====
logging.info(f"Appended CSV: {output_csv}")
logging.info(f"Included Features Audit: {included_audit_csv} ({len(included_audit_rows)} rows)")
logging.info(f"Excluded Features Audit: {excluded_audit_csv} ({len(excluded_audit_rows)} rows)")
logging.info(f"Space Trim Audit: {space_trim_audit_csv} ({len(space_trim_rows)} rows)")
logging.info("=== Script complete ===")

arcpy.AddMessage(f"Output CSV: {output_csv}")
arcpy.AddMessage(f"Included Features Audit: {included_audit_csv}")
arcpy.AddMessage(f"Excluded Features Audit: {excluded_audit_csv}")
arcpy.AddMessage(f"Space Trim Audit: {space_trim_audit_csv}")
arcpy.AddMessage(f"Log: {log_filename}")
