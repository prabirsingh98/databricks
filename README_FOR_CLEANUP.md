# Unity Catalog Volume - Excel File Cleanup

Automated cleanup of stale Excel files (`.xlsx`, `.xls`) from Databricks Unity Catalog Volume paths. Designed to run as a scheduled Databricks Workflow task with full observability, retry logic, and fail-safe error handling.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Permissions Required](#permissions-required)
- [Widget Parameters](#widget-parameters)
- [How to Execute](#how-to-execute)
  - [Interactive (Notebook UI)](#1-interactive-notebook-ui)
  - [Databricks Workflow (Scheduled)](#2-databricks-workflow-scheduled)
  - [Databricks CLI](#3-databricks-cli)
- [Sample Input and Expected Output](#sample-input-and-expected-output)
- [Logical Flow](#logical-flow)
- [Enterprise Features](#enterprise-features)
- [Error Handling and Failure Modes](#error-handling-and-failure-modes)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

| Requirement | Details |
|---|---|
| **Databricks Runtime** | 13.3 LTS or later (Python 3.10+) |
| **Unity Catalog** | Workspace must have Unity Catalog enabled |
| **Volumes** | Target volumes must already exist and be mounted at `/Volumes/<catalog>/<schema>/<volume>` |
| **Cluster** | Single-node or multi-node cluster with access to the target Unity Catalog volumes |
| **Python libraries** | Standard library only (`os`, `time`, `logging`, `datetime`, `sys`) - no additional `pip install` required |

---

## Permissions Required

The executing principal (user, service principal, or group) needs the following Databricks and Unity Catalog permissions:

| Permission | Scope | Why |
|---|---|---|
| **USE CATALOG** | Target catalog(s) | Required to access any object within the catalog |
| **USE SCHEMA** | Target schema(s) | Required to access volumes within the schema |
| **READ VOLUME** | Target volume(s) | Required to list and read file metadata (modification time, size) |
| **WRITE VOLUME** | Target volume(s) | Required to delete files (only needed when `dry_run=false`) |
| **CAN ATTACH TO** | Compute cluster | Required to run the notebook on the cluster |
| **CAN RUN** | Notebook | Required for workflow-based execution via a service principal |

### Recommended setup for production

- Create a **service principal** dedicated to this cleanup job.
- Grant it `READ VOLUME` + `WRITE VOLUME` on only the target volumes (least privilege).
- Assign it to a **job cluster** that auto-terminates after the run.

---

## Widget Parameters

All configuration is exposed as Databricks widgets, overridable at the job/task level without modifying the notebook.

| Widget | Type | Default | Description |
|---|---|---|---|
| `volume_paths` | text | `/Volumes/catalog1/schema1/volume1,/Volumes/catalog1/schema1/volume2` | Comma-separated list of Unity Catalog Volume paths to scan recursively |
| `retention_days` | text | `29` | Files with a modification time older than this many days are eligible for deletion |
| `file_extensions` | text | `.xlsx,.xls` | Comma-separated file extensions to target (case-insensitive matching) |
| `dry_run` | dropdown | `true` | `true` = list eligible files only (no deletions); `false` = actually delete files |
| `max_retries` | text | `3` | Number of retry attempts per file on transient OS/permission errors (exponential backoff) |

---

## How to Execute

### 1. Interactive (Notebook UI)

1. Open the notebook in the Databricks workspace.
2. Attach to a cluster with the required permissions.
3. Modify widget values in the widget panel at the top of the notebook.
4. **Run All** cells.
5. Review the structured log output in each cell's output area.

> **Tip:** Start with `dry_run=true` (the default) to preview which files would be deleted before switching to `false`.

### 2. Databricks Workflow (Scheduled)

This is the recommended production approach.

1. **Create a new Job** in the Databricks Workflows UI.
2. **Add a Notebook task** pointing to this notebook.
3. **Override widget parameters** at the task level:

   ```json
   {
     "volume_paths": "/Volumes/prod_catalog/finance/reports,/Volumes/prod_catalog/hr/exports",
     "retention_days": "29",
     "file_extensions": ".xlsx,.xls",
     "dry_run": "false",
     "max_retries": "3"
   }
   ```

4. **Set the schedule** (e.g., daily at 02:00 UTC).
5. **Configure alerting** on job failure - the notebook raises a `RuntimeError` on any deletion errors, which marks the run as **FAILED** and triggers Databricks built-in email/webhook alerts.
6. **Assign a job cluster** (recommended) or an existing interactive cluster.

### 3. Databricks CLI

```bash
# One-time run with parameter overrides
databricks jobs run-now --job-id <JOB_ID> \
  --notebook-params '{
    "volume_paths": "/Volumes/catalog/schema/volume1",
    "retention_days": "29",
    "dry_run": "false"
  }'
```

```bash
# Or run the notebook directly (ad hoc)
databricks runs submit --json '{
  "run_name": "excel-cleanup-adhoc",
  "existing_cluster_id": "<CLUSTER_ID>",
  "notebook_task": {
    "notebook_path": "/Repos/<user>/clean_up_volume/cleanup_old_excel_files",
    "base_parameters": {
      "volume_paths": "/Volumes/catalog/schema/volume1",
      "retention_days": "29",
      "dry_run": "true"
    }
  }
}'
```

---

## Sample Input and Expected Output

### Scenario: Dry run with 2 volumes

**Widget inputs:**

| Widget | Value |
|---|---|
| `volume_paths` | `/Volumes/prod/finance/reports,/Volumes/prod/hr/exports` |
| `retention_days` | `29` |
| `file_extensions` | `.xlsx,.xls` |
| `dry_run` | `true` |
| `max_retries` | `3` |

**Expected output (structured log):**

```
2026-02-24 02:00:01 | INFO     | Logger initialised
2026-02-24 02:00:01 | INFO     | ============================================================
2026-02-24 02:00:01 | INFO     | CONFIGURATION (validated)
2026-02-24 02:00:01 | INFO     | ============================================================
2026-02-24 02:00:01 | INFO     | Volume paths     : ['/Volumes/prod/finance/reports', '/Volumes/prod/hr/exports']
2026-02-24 02:00:01 | INFO     | Retention days   : 29
2026-02-24 02:00:01 | INFO     | File extensions  : ('.xlsx', '.xls')
2026-02-24 02:00:01 | INFO     | Dry run          : True
2026-02-24 02:00:01 | INFO     | Max retries      : 3
2026-02-24 02:00:01 | INFO     | Cutoff date (UTC): 2026-01-26 02:00:01
2026-02-24 02:00:01 | INFO     | ============================================================
2026-02-24 02:00:01 | INFO     | Scanning: /Volumes/prod/finance/reports
2026-02-24 02:00:02 | INFO     |   Found 150 matching file(s)
2026-02-24 02:00:02 | INFO     |   [DRY_RUN_SKIPPED] /Volumes/prod/finance/reports/Q3_2025.xlsx | age=45.2d | size=2.3 MB | modified=2026-01-10 08:30:00
2026-02-24 02:00:02 | INFO     |   [DRY_RUN_SKIPPED] /Volumes/prod/finance/reports/archive/old_report.xls | age=90.1d | size=512.0 KB | modified=2025-11-26 14:00:00
...
2026-02-24 02:00:02 | INFO     |   Volume summary: scanned=150 eligible=38 deleted=0 retained=112 errors=0 freed=0.0 B
2026-02-24 02:00:02 | INFO     | Scanning: /Volumes/prod/hr/exports
2026-02-24 02:00:03 | INFO     |   Found 42 matching file(s)
...
2026-02-24 02:00:03 | INFO     |   Volume summary: scanned=42 eligible=12 deleted=0 retained=30 errors=0 freed=0.0 B
2026-02-24 02:00:03 | INFO     |
2026-02-24 02:00:03 | INFO     | ============================================================
2026-02-24 02:00:03 | INFO     | EXECUTION SUMMARY
2026-02-24 02:00:03 | INFO     | ============================================================
2026-02-24 02:00:03 | INFO     | Mode                 : DRY RUN
2026-02-24 02:00:03 | INFO     | Run duration         : 2.1s
2026-02-24 02:00:03 | INFO     | Volume paths config  : 2
2026-02-24 02:00:03 | INFO     | Volume paths scanned : 2
2026-02-24 02:00:03 | INFO     | Volume paths skipped : 0
2026-02-24 02:00:03 | INFO     | Total files found    : 192
2026-02-24 02:00:03 | INFO     | Files retained (<29d): 142
2026-02-24 02:00:03 | INFO     | Eligible for delete  : 50 (older than 29 days)
2026-02-24 02:00:03 | INFO     | Files deleted        : 0 (dry run - no files removed)
2026-02-24 02:00:03 | INFO     | Errors               : 0
2026-02-24 02:00:03 | INFO     | ============================================================
2026-02-24 02:00:03 | INFO     |
2026-02-24 02:00:03 | INFO     | PER-VOLUME BREAKDOWN
2026-02-24 02:00:03 | INFO     | ------------------------------------------------------------
2026-02-24 02:00:03 | INFO     |   /Volumes/prod/finance/reports: scanned=150 eligible=38 deleted=0 retained=112 errors=0 freed=0.0 B
2026-02-24 02:00:03 | INFO     |   /Volumes/prod/hr/exports: scanned=42 eligible=12 deleted=0 retained=30 errors=0 freed=0.0 B
2026-02-24 02:00:03 | INFO     | ------------------------------------------------------------
2026-02-24 02:00:03 | INFO     |
2026-02-24 02:00:03 | INFO     | NOTE: Set dry_run=false to actually delete the 50 eligible file(s).
2026-02-24 02:00:03 | INFO     | Job completed successfully: DRY_RUN: scanned=192 eligible=50 deleted=0 errors=0 freed=0.0 B duration=2.1s
```

**Notebook exit value:**

```
SUCCESS: DRY_RUN: scanned=192 eligible=50 deleted=0 errors=0 freed=0.0 B duration=2.1s
```

### Scenario: Live delete with errors

When `dry_run=false` and some files fail to delete, the output includes:

```
2026-02-24 02:00:05 | WARNING  |   Attempt 1/3 failed for /Volumes/prod/finance/reports/locked.xlsx: [Errno 13] Permission denied - retrying in 1s
2026-02-24 02:00:07 | WARNING  |   Attempt 2/3 failed for /Volumes/prod/finance/reports/locked.xlsx: [Errno 13] Permission denied - retrying in 2s
2026-02-24 02:00:10 | WARNING  |   [ERROR: [ERRNO 13] PERMISSION DENIED] /Volumes/prod/finance/reports/locked.xlsx | age=35.0d | size=1.5 MB | modified=2026-01-20 10:00:00 | attempts=3
...
2026-02-24 02:00:10 | ERROR    | Job will FAIL due to 1 error(s)
```

**The job run is marked as FAILED in Databricks**, triggering any configured alerts.

---

## Logical Flow

```
START
  |
  v
[1] Create Databricks widgets (volume_paths, retention_days, file_extensions, dry_run, max_retries)
  |
  v
[2] Initialise structured logger (Python logging module)
  |
  v
[3] Parse widget values into typed Python variables
  |
  v
[4] Validate all inputs -----> FAIL FAST (raise ValueError)
  |    - volume_paths non-empty, each starts with /Volumes/
  |    - retention_days >= 1
  |    - file_extensions non-empty, each starts with "."
  |    - max_retries >= 0
  |
  v
[5] Compute cutoff timestamp = now() - (retention_days * 86400)
  |
  v
[6] FOR EACH volume_path:
  |    |
  |    +--> Path exists? --NO--> Log WARNING, skip, increment paths_skipped
  |    |
  |    +-YES
  |    |
  |    v
  |   [6a] Recursively walk all subdirectories (os.walk)
  |         Collect files matching target extensions (case-insensitive)
  |    |
  |    v
  |   [6b] FOR EACH matching file:
  |         |
  |         +--> modification_time >= cutoff? --YES--> Skip (retained)
  |         |
  |         +-NO (file is older than retention_days)
  |         |
  |         v
  |        [6c] dry_run=true?
  |         |        |
  |         |       YES --> Log as DRY_RUN_SKIPPED
  |         |        |
  |         |       NO
  |         |        |
  |         |        v
  |         |      File still exists? --NO--> Log as ALREADY_REMOVED (idempotent)
  |         |        |
  |         |       YES
  |         |        |
  |         |        v
  |         |      Attempt os.remove()
  |         |        |
  |         |       SUCCESS --> Verify file gone --> Log as DELETED
  |         |        |
  |         |       FAILURE (PermissionError / OSError)
  |         |        |
  |         |        v
  |         |      Retries remaining? --YES--> Exponential backoff (1s, 2s, 4s...) --> Retry
  |         |        |
  |         |       NO
  |         |        |
  |         |        v
  |         |      Log as ERROR, increment error counter
  |         |
  |    v
  |   [6d] Log per-volume summary stats
  |
  v
[7] Print EXECUTION SUMMARY (global stats + per-volume breakdown)
  |
  v
[8] errors > 0?
      |          |
     YES        NO
      |          |
      v          v
  Log error    Log success
  details      exit message
      |          |
      v          v
  dbutils.     dbutils.
  notebook.    notebook.
  exit(FAIL)   exit(SUCCESS)
      |
      v
  raise RuntimeError  -->  Databricks marks run as FAILED
                           (triggers workflow alerting)
```

---

## Enterprise Features

### Structured Logging
All output uses Python's `logging` module with the format:
```
%(asctime)s | %(levelname)-8s | %(message)s
```
This provides timestamped, leveled log lines that integrate with Databricks log delivery and external log aggregation (Datadog, Splunk, etc.).

### Input Validation
All widget values are validated before any file operations begin. Invalid configuration raises a `ValueError` immediately, preventing partial runs with bad parameters.

### Retry with Exponential Backoff
Transient failures (e.g., file locks, NFS hiccups) are retried up to `max_retries` times with exponential backoff (1s, 2s, 4s, ...). This handles intermittent issues on distributed filesystems without manual intervention.

### Idempotency
- Files are checked for existence **before** attempting deletion (handles concurrent cleanup jobs or manual deletions).
- `FileNotFoundError` during `os.remove()` is treated as success (race condition safe).
- Post-delete verification confirms the file is actually gone.
- Safe to re-run at any time without side effects.

### Execution Timing
Total run duration is tracked and reported in the summary, enabling performance monitoring and SLA tracking over time.

### Per-Volume Statistics
Each volume path gets its own stats breakdown (scanned, eligible, deleted, retained, errors, bytes freed), making it easy to identify which volumes are generating the most cleanup work.

### Hard Failure on Errors
When any file deletion fails (after all retries), the notebook:
1. Logs the error details at `ERROR` level
2. Calls `dbutils.notebook.exit()` with a `FAILED` prefix
3. Raises `RuntimeError` to mark the Databricks job run as **FAILED**

This integrates with Databricks workflow alerting (email, webhook, PagerDuty) so operations teams are notified immediately.

---

## Error Handling and Failure Modes

| Failure | Behavior |
|---|---|
| Invalid widget values | `ValueError` raised immediately (fail fast). Job marked as FAILED. |
| Volume path does not exist | Logged as WARNING, path skipped. Other paths still processed. |
| Permission denied on directory scan | Logged as WARNING, partial results returned for that volume. |
| Cannot stat a file (OS error) | Logged as ERROR, file skipped, error counter incremented. |
| File deletion fails (transient) | Retried up to `max_retries` times with exponential backoff. |
| File deletion fails (permanent) | Logged as ERROR after all retries exhausted. Error counter incremented. |
| File removed between scan and delete | Treated as `already_removed` (idempotent, not an error). |
| Any errors at end of run | `RuntimeError` raised, Databricks job marked FAILED, alerts triggered. |

---

## Troubleshooting

### "Path does not exist or is not a directory"
- Verify the volume path exists: `ls /Volumes/<catalog>/<schema>/<volume>` in a notebook cell.
- Confirm the executing principal has `USE CATALOG`, `USE SCHEMA`, and `READ VOLUME` permissions.

### "Permission denied" errors during deletion
- The executing principal needs `WRITE VOLUME` on the target volume.
- If using a service principal, verify it has been granted write access.
- Check if files are locked by another process (e.g., an active Spark job reading the file).

### "Configuration validation failed"
- Review the error messages in the log output - they indicate exactly which widget values are invalid.
- Common mistakes: missing the leading dot in extensions (`xlsx` instead of `.xlsx`), or non-`/Volumes/` paths.

### Job succeeds but no files deleted
- Confirm `dry_run` is set to `false` at the job task level.
- Check the `retention_days` value - files must be strictly older than this threshold.
- Verify the `file_extensions` match your files (matching is case-insensitive).

### Job takes too long
- Check the total number of files being scanned (logged per-volume).
- Consider splitting large volume lists across multiple job tasks running in parallel.
- Reduce `max_retries` if retries on permanently locked files are adding delay.
