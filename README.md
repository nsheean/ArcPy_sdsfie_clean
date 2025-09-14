# ArcPy SDSFIE Toolkit

Author: **Nathanael Sheean**
Version: **2025-09-14**
Scope: Cross-user ArcGIS Pro utilities that operate on SDSFIE-regulated geodatabases and active map contexts. All scripts favor safe defaults, explicit audits, and deterministic outcomes.

---

## Purpose

Deliver a focused set of ArcPy and Arcade utilities that standardize export, QA, geometry analytics, flood mapping, identifier governance, and field population across users and projects. Each script documents inputs, safeguards, outputs, and whether it writes to a geodatabase.

---

## Run prerequisites

* **ArcGIS Pro** with **ArcPy** available.
* **Windows** environment with access to the target file geodatabases, projects, and datasets.
* **SDSFIE** schema alignment for layer and field names where stated.
* **Active map** loaded when the script specifies “active map only.”
* Write permissions on target workspaces for scripts that update fields or geodatabase metadata.
* Close edit sessions that would hold schema locks when a script needs exclusive access.
* For CSV-based utilities, valid CSVs with required headers as described in each script header.

---

## Safety rules used across scripts

1. Detect and respect **schema locks**; abort with a clear reason.
2. Traverse **group layers and nested groups**; operate on the target feature classes regardless of depth.
3. Avoid services and joined layers for write operations; log and skip them.
4. Write **audit logs and CSVs** in the project home or alongside specified inputs.
5. Keep **overwrites explicit**; do not overwrite existing outputs unless documented.
6. Treat units and field domains explicitly; never infer silently.
7. For CSV inputs, validate **mandatory fields**; abort with a reason if missing.

---

## Repository layout (recommended)

```
/ArcPy_sdsfie/
  README.md  ← this file
  /arcade/
    common/
      uv_composite_mapper_2f3f.arcade
  /cad/
    gis_to_cad_cip.py
    gis_to_cad_utilities.py
  /flood_map/
    Script_A_Flood_Preprocess.py
    Script_B_HAND_and_Rainfall.py
    Script_C_...py
    Script_D_...py
    README.md
  /gdb_governance/
    Duplicate_GUID_finder_writer.py
    Primary_Key_Identifier_Select_fields_by_Alias_and_calculate_Field.py
    Globally_Unique_Identifier_Select_fields_by_Alias_and_calculate_Field.py
    Recalculate_Spatial_Index_ALL.py
    audit_missing_geometry.py
    Alias_Based_Field_Selection_and_Calculation.py
    BatchGeometryCalculator.py
  /migration/
    code_to_migrate_fgdb.py
  /replica/
    File_GDB_replica_checkout_verification.py
  /reports/
    (script logs and audit CSVs at runtime)
```

> If your actual layout differs, keep the README table below as the source of truth for each file’s role and behavior.

---

## Script catalog

| Script                                                                                                | Role                                                                                                                                                                                                 | Run requirements                                                                                         | Writes/Overwrites GDB data                                                      | Outputs                                                                                                                   |
| ----------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| **cad/gis\_to\_cad\_cip.py**                                                                          | Prepare CIP layers and export to DWG (R2018). Populate `Layer` field per CIP rules.                                                                                                                  | SDSFIE layer names present in the active map; file access to output DWG.                                 | **Yes**. Adds or updates text field `Layer` on input FCs.                       | DWG file with timestamp; ArcGIS messages log.                                                                             |
| **cad/gis\_to\_cad\_utilities.py**                                                                    | Prepare Utilities layers and export to DWG. Separates “abandoned” for \*\_L by suffix `_abandoned`. Optional commented annotation placeholders for up to 2 line attributes.                          | SDSFIE utilities *\_L/*\_P/\*\_A present in the active map; file access to DWG.                          | **Yes**. Adds or updates `Layer`.                                               | DWG with timestamp; messages log.                                                                                         |
| **arcade/common/uv\_composite\_mapper\_2f3f.arcade**                                                  | Arcade expression for Unique Values from 2–3 fields with domain decoding and normalization.                                                                                                          | Paste into Unique Values expression; edit config block and mapping table.                                | **No**. Expression only.                                                        | Symbol categories in the layer.                                                                                           |
| **gdb\_governance/Alias\_Based\_Field\_Selection\_and\_Calculation.py**                               | Find a field by **Alias** in active map datasets and populate empty or placeholder values with a set text. Full audit logging.                                                                       | Active map; edit rights; exclusive lock on target FCs; set TARGET\_ALIAS and value.                      | **Yes**. Calculates into the matched text field.                                | `alias_calc_audit_*.csv`, `alias_calc_updated_*.csv`, `alias_calc_skipped_*.csv`, `.log`.                                 |
| **gdb\_governance/Primary\_Key\_Identifier\_Select\_fields\_by\_Alias\_and\_calculate\_Field.py**     | Select fields by **Alias** and calculate **Primary Key** values where null or placeholder.                                                                                                           | Active map; alias present; edit rights.                                                                  | **Yes**. Updates the primary key field.                                         | Per-script CSV log and messages.                                                                                          |
| **gdb\_governance/Globally\_Unique\_Identifier\_Select\_fields\_by\_Alias\_and\_calculate\_Field.py** | Select fields by **Alias** and calculate **GUID** values where eligible.                                                                                                                             | Active map; alias present; edit rights.                                                                  | **Yes**. Updates GUID fields except `GLOBALID`.                                 | Per-script CSV log and messages.                                                                                          |
| **gdb\_governance/Duplicate\_GUID\_finder\_writer.py**                                                | Detect duplicate GUIDs across all fields. Normalize bracketed `{GUID}` and plain hex. **Rewrite duplicates** in non-`GLOBALID` GUID fields and text fields that hold GUIDs; never change `GLOBALID`. | Active map; edit rights; exclusive lock for updates.                                                     | **Yes**. Writes new GUIDs into non-`GLOBALID` GUID fields and text GUID fields. | Duplicate report CSVs, updated fields CSV, skipped/error CSVs, `.log`.                                                    |
| **gdb\_governance/Recalculate\_Spatial\_Index\_ALL.py**                                               | Rebuild spatial indexes for FCs to improve performance.                                                                                                                                              | Workspace access; exclusive schema lock.                                                                 | **Yes**. Recreates index metadata. No attribute changes.                        | Summary CSV/log of updated datasets.                                                                                      |
| **gdb\_governance/BatchGeometryCalculator.py**                                                        | Calculate area and length using **geodesic** methods. Enforce explicit sq ft vs sq yd rules per designated \*\_A feature classes.                                                                    | Active map; field presence; edit rights.                                                                 | **Yes**. Updates area/length fields only.                                       | Updated/Skipped CSVs, `.log`.                                                                                             |
| **gdb\_governance/audit\_missing\_geometry.py**                                                       | Scan active map datasets for invalid or missing geometry. No repairs.                                                                                                                                | Active map; read access.                                                                                 | **No**. Read-only.                                                              | CSV audit of features and layers flagged, `.log`.                                                                         |
| **replica/File\_GDB\_replica\_checkout\_verification.py**                                             | Report file GDB replica checkout status and full paths. No edits.                                                                                                                                    | Path to FGDB; read access.                                                                               | **No**. Read-only.                                                              | Printed paths and replica metadata; explicit failures on error.                                                           |
| **migration/code\_to\_migrate\_fgdb.py**                                                              | Safe copy or migrate FGDB contents. Abort if `.lock` files exist or if destination collision is detected.                                                                                            | Replace placeholders with absolute paths; file permissions.                                              | **No** by design. Aborts on collisions; does not overwrite.                     | Console messages; collision report; lock report.                                                                          |
| **analytics/AggregateGeometryByFacility.py**                                                          | Aggregate area and length per **facility number** from active map datasets and append to **hardcoded NexGen CSV**. Audits whitespace trimming and exclusions.                                        | Replace `INPUT_CSV` placeholder; CSV must include one of `Fac Nbr`, `Facility Number`, `FacilityNumber`. | **No**. Appends to a new CSV beside the input.                                  | `_appended_*.csv`, `_included_features_audit_*.csv`, `_excluded_features_audit_*.csv`, `_space_trim_audit_*.csv`, `.log`. |
| **flood\_map/Script\_A\_Flood\_Preprocess.py**                                                        | Prepare elevation and hydrology inputs. Enforce DEM naming clarity: `DEM_10cm`, `DEM_PLUS_60cm`, `DEM_MINUS_60cm`.                                                                                   | Input rasters available; workspace set; read/write on output location.                                   | **No** on existing features. Writes new rasters.                                | Preprocessed rasters and a run log.                                                                                       |
| **flood\_map/Script\_B\_HAND\_and\_Rainfall.py**                                                      | Compute HAND surfaces and rainfall scenarios. Chain from A outputs.                                                                                                                                  | Outputs from Script A present; workspace write access.                                                   | **No** on existing features. Writes new rasters.                                | HAND rasters, rainfall scenario rasters, log.                                                                             |
| **flood\_map/Script\_C\_...py**                                                                       | Continue flood depth, extent, or thresholding as defined in folder README.                                                                                                                           | Outputs from B present.                                                                                  | **No** on existing features. Writes new rasters.                                | Depth/extent products, log.                                                                                               |
| **flood\_map/Script\_D\_...py**                                                                       | Finalize, QA, and package deliverables for publication.                                                                                                                                              | Outputs from C present.                                                                                  | **No** on existing features. Writes reports and layers.                         | Final deliverables and QA reports.                                                                                        |
| **Utilities\_All\_With\_Exemption\_SR\_Safe.py** *(if present)*                                       | Utility infrastructure processing per SDSFIE with defined exemptions.                                                                                                                                | As noted in script header.                                                                               | Depends on script flags; review header.                                         | As noted in script header.                                                                                                |
| **Main\_PA\_To\_10cm\_SR\_Safe.py** *(if present)*                                                    | Surface generation from LiDAR at 0.10 m resolution and ±0.60 m offsets. Names must use **cm in labels** for clarity; computations remain in meters.                                                  | Input point clouds/rasters; workspace write access.                                                      | **No** on existing features. Writes new rasters.                                | DEM\_10cm, DEM\_PLUS\_60cm, DEM\_MINUS\_60cm, log.                                                                        |

> If a listed script has been renamed in your fork, keep its row and update the file path in the first column.

---

## Execution model

* **Active-map scripts**: operate only on datasets referenced in the current map. This ensures user-specific groupings do not block discovery of target feature classes.
* **Workspace scripts**: target a workspace path and iterate all feature classes with explicit filters.
* **CSV-driven scripts**: validate the presence of required headers, trim whitespace, and maintain a space-trim audit.

---

## Overwrite policy

* Scripts that **update fields or metadata** are marked “Yes” in the table. They do not touch geometry unless stated.
* Scripts that **produce rasters, CAD, or CSVs** write to new, timestamped outputs by default.
* The migration script **aborts** on `.lock` files and path collisions. The replica verification script is read-only.

---

## Logging and audits

* Attribute update scripts write **CSV audits** that separate **updated**, **skipped**, and **errors**.
* The NexGen aggregator writes a **space-trim audit** that records every normalization on both CSV and dataset sides.
* CAD and geometry calculators write **run summaries** and counts that support QA.
* All scripts write a **timestamped log file** with the command messages.

---

## Units and domains

* The geometry calculator uses **geodesic** methods when applicable.
* Polygons marked for **square yards** vs **square feet** are defined explicitly in the script header.
* Domain decoding prefers **DomainName()** values and uses code fallbacks only with audit records.

---

## Arcade expression

The generic two- or three-field Unique Values expression lives at:

```
arcade/common/uv_composite_mapper_2f3f.arcade
```

Edit only the **CONFIG** and **MAP** blocks. The header includes authorship, version, and safety rules. Keep `DEFAULT_LABEL` distinct for QA reviews.

---

## Contribution standards

* Keep user-editable configuration at the top of each script.
* Preserve existing audit schemas when you extend a script.
* Add a “Writes/Overwrites GDB data” line to every new header.
* Use timestamped outputs for generated artifacts.
* Document mandatory inputs, unit assumptions, and skip conditions.

---

## Quick start

1. Clone the repository and open an ArcGIS Pro project.
2. Load the target geodatabase and ensure the **active map** contains the datasets you plan to process.
3. For CSV-based scripts, set the input path at the top of the script and confirm required headers.
4. Run the script from the Python window in ArcGIS Pro or as a script tool.
5. Review the generated **CSV audits** and **log files**.
6. Commit the audit CSVs that support your QA workflow, or archive them under `/reports/`.

---

## License and attribution

Copyright © 2025 **Nathanael Sheean**.
Use requires retention of authorship in headers. Derivative work must keep safety rules and audit provisions intact.

---

### Final note

This README reflects the scripts and practices captured in this repository. If you add new utilities, update the catalog table with the run requirements, overwrite policy, and outputs to maintain a consistent operational standard.
