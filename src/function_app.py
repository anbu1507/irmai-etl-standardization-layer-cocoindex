import azure.functions as func
import cocoindex.op as op
import csv
import io
import re
import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
from azure.storage.blob import BlobServiceClient
from openai import AzureOpenAI

app = func.FunctionApp()

# ---------------------------------------------------------------------------
# Domain mapping: raw-zone first subfolder → parent label in staging-zone
# ---------------------------------------------------------------------------
DOMAIN_MAP = {
    'carinsurance':  'car-insurance',
    'Car-insurance': 'car-insurance',
    'Car-Insurance': 'car-insurance',
    'car-insurance': 'car-insurance',
    'Real-Estate':   'real-estate',
}

# ---------------------------------------------------------------------------
# Column alias priority lists (first match wins)
# ---------------------------------------------------------------------------
ID_ALIASES  = [
    'case_id', 'policy_id', 'policy_no', 'policy_num', 'user_log_id',
    'workflow_instance_id', 'parent_workflow_instance_id', 'source_workflow_instance_id',
]
ACT_ALIASES = [
    'activity', 'activity_name', 'event_action',
    'billing_activity', 'activity_code', 'remarks', 'user_log',
]
TS_ALIASES  = [
    'event_timestamp', 'timestamp', 'created_at', 'tx_time', 'user_log_datetime',
]
# user_log_id first → becomes resource for Real-Estate (Aniket requirement)
RES_ALIASES = [
    'user_log_id', 'resource', 'resource_type', 'resource_id', 'user_log_added_by_name',
]

# Alias columns absorbed into pillars → remove from final output
ALIAS_COLS_TO_DROP = {
    'policy_id', 'policy_no', 'policy_num', 'user_log_id',
    'workflow_instance_id', 'parent_workflow_instance_id', 'source_workflow_instance_id',
    'activity_name', 'event_action', 'billing_activity', 'activity_code', 'remarks', 'user_log',
    'event_timestamp', 'created_at', 'tx_time', 'user_log_datetime',
    'resource_type', 'resource_id', 'user_log_added_by_name',
}


# ===========================================================================
# CocoIndex Operator 1 — Parse raw file bytes → list of row dicts
# No pandas: uses Python csv module + xlrd + openpyxl directly
# ===========================================================================
@op.function()
def ParseFile(content: bytes, filename: str) -> List[Dict[str, Any]]:
    """Detect file format and return rows as plain Python dicts."""

    # --- Legacy .xls (CFBF magic bytes) ---
    if content.startswith(b'\xd0\xcf\x11\xe0'):
        logging.info(f"[{filename}] Detected .xls — parsing with xlrd.")
        import xlrd
        wb      = xlrd.open_workbook(file_contents=content)
        ws      = wb.sheet_by_index(0)
        headers = [str(ws.cell_value(0, c)).strip() for c in range(ws.ncols)]
        rows    = []
        for r in range(1, ws.nrows):
            row = {headers[c]: ws.cell_value(r, c) for c in range(ws.ncols)}
            if any(str(v).strip() for v in row.values()):
                rows.append(row)
        logging.info(f"[{filename}] Parsed {len(rows)} rows from .xls.")
        return rows

    # --- Modern .xlsx (ZIP/PK magic bytes) ---
    if content.startswith(b'PK\x03\x04'):
        logging.info(f"[{filename}] Detected .xlsx — parsing with openpyxl.")
        import openpyxl
        wb      = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
        ws      = wb.active
        it      = ws.iter_rows(values_only=True)
        headers = [str(h).strip() for h in next(it)]
        rows    = [
            dict(zip(headers, row))
            for row in it
            if any(v is not None for v in row)
        ]
        wb.close()
        logging.info(f"[{filename}] Parsed {len(rows)} rows from .xlsx.")
        return rows

    # --- CSV: try encodings; skip junk rows before real header ---
    for enc in ['utf-8', 'cp1252', 'latin-1']:
        try:
            text  = content.decode(enc)
            lines = text.splitlines()
            # Find the header row (contains a known column name)
            start = 0
            for i, line in enumerate(lines):
                if any(k in line for k in ('user_log_id', 'case_id', 'policy_id', 'activity')):
                    start = i
                    break
            if start:
                logging.info(f"[{filename}] Header found at row {start} (enc={enc}).")
            reader = csv.DictReader(lines[start:])
            rows   = [
                {k.strip(): v.strip() for k, v in row.items() if k}
                for row in reader
                if any(v.strip() for v in row.values())
            ]
            logging.info(f"[{filename}] Parsed {len(rows)} rows from CSV.")
            return rows
        except Exception:
            continue

    logging.error(f"[{filename}] Could not decode file.")
    return []


# ===========================================================================
# CocoIndex Operator 2 — Fix timestamps (Excel serial or normal string)
# No pandas: uses Python datetime + dateutil
# ===========================================================================
@op.function()
def FixTimestamp(val: Any) -> str:
    """Convert a single timestamp value to ISO format string."""
    if val is None or str(val).strip() in ('', 'nan', 'None', 'NaT'):
        return datetime.now().isoformat(sep=' ', timespec='seconds')

    # Try Excel serial (float like 46092.10277)
    try:
        f = float(str(val))
        if 30000.0 <= f <= 60000.0:
            dt = datetime(1899, 12, 30) + timedelta(days=f)
            return dt.isoformat(sep=' ', timespec='seconds')
    except (ValueError, TypeError):
        pass

    # Try standard datetime string
    try:
        from dateutil import parser as dp
        return dp.parse(str(val)).isoformat(sep=' ', timespec='seconds')
    except Exception:
        return datetime.now().isoformat(sep=' ', timespec='seconds')


# ===========================================================================
# CocoIndex Operator 3 — AI activity derivation via Azure OpenAI
# Processes in batches of 50 to avoid token limit truncation
# ===========================================================================
BATCH_SIZE = 50

@op.function()
def DeriveActivity(user_log_texts: List[str]) -> List[str]:
    """Use Azure OpenAI to classify free-text user_log → activity labels (batched)."""
    endpoint   = os.environ.get("AZURE_OPENAI_ENDPOINT", "").strip()
    api_key    = os.environ.get("AZURE_OPENAI_API_KEY", "").strip()
    deployment = os.environ.get("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4.1-mini").strip()

    if not endpoint or not api_key:
        logging.warning("Azure OpenAI not configured — keeping raw user_log as activity.")
        return user_log_texts

    client = AzureOpenAI(azure_endpoint=endpoint, api_key=api_key, api_version="2024-02-01")
    all_labels = []

    # Split into batches of 50 to avoid response truncation
    for batch_start in range(0, len(user_log_texts), BATCH_SIZE):
        batch = user_log_texts[batch_start: batch_start + BATCH_SIZE]
        numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(batch))
        prompt = (
            "You are an ETL activity classifier for a real estate agency management system.\n"
            "For each user log entry, return a concise activity label (2-5 words, Title Case).\n"
            "Reply ONLY with the labels, one per line, same order. No numbering, no explanation.\n\n"
            f"Entries:\n{numbered}"
        )
        try:
            response = client.chat.completions.create(
                model=deployment,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=BATCH_SIZE * 15,
            )
            raw    = response.choices[0].message.content.strip().split("\n")
            labels = [l.strip().lstrip("0123456789. )-") for l in raw if l.strip()]
            if len(labels) == len(batch):
                all_labels.extend(labels)
            else:
                logging.warning(f"Batch {batch_start}: AI returned {len(labels)} for {len(batch)} — using raw text.")
                all_labels.extend(batch)
        except Exception as e:
            logging.warning(f"Batch {batch_start} failed: {e} — using raw text.")
            all_labels.extend(batch)

    logging.info(f"AI derived {len(all_labels)} activity labels in {len(user_log_texts)//BATCH_SIZE + 1} batches.")
    return all_labels


# ===========================================================================
# CocoIndex Operator 4 — Standardize rows to 4-pillar format
# No pandas: pure Python dict manipulation
# ===========================================================================
@op.function()
def StandardizeRows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Apply 4-pillar ETL: dedup, map aliases, fix timestamps, drop alias cols."""

    def _strip_prefix(val):
        s = str(val).strip()
        return re.sub(r'^[A-Z]{2}_', '', s).strip()

    def _first_val(row, aliases):
        for a in aliases:
            if a in row and str(row[a]).strip() not in ('', 'nan', 'None', 'NaT'):
                return row[a]
        return None

    # --- Dedup using frozenset of items ---
    seen, unique = set(), []
    for row in rows:
        key = frozenset((k, str(v)) for k, v in row.items())
        if key not in seen:
            seen.add(key)
            unique.append(row)
    logging.info(f"Rows after dedup: {len(unique)} (was {len(rows)})")

    # --- Activity: collect all alias texts first, then AI-derive if user_log present ---
    has_user_log      = any('user_log' in r for r in unique)
    no_activity_col   = not any('activity' in r for r in unique)

    if has_user_log and no_activity_col:
        user_log_texts = [str(r.get('user_log', '')).strip() for r in unique]
        activity_labels = DeriveActivity(user_log_texts)
    else:
        activity_labels = None

    result = []
    for i, row in enumerate(unique):
        # case_id
        ids = [_strip_prefix(row[a]) for a in ID_ALIASES if a in row
               and str(row[a]).strip() not in ('', 'nan', 'None', 'NA')]
        case_id = ', '.join(dict.fromkeys(ids)) if ids else 'UNKNOWN'

        # activity
        if activity_labels:
            activity = activity_labels[i]
        else:
            acts = [str(row[a]).strip() for a in ACT_ALIASES if a in row
                    and str(row[a]).strip() not in ('', 'nan', 'None')]
            activity = ' | '.join(dict.fromkeys(acts)) if acts else 'UNKNOWN'

        # timestamp
        ts_raw = _first_val(row, TS_ALIASES)
        timestamp = FixTimestamp(ts_raw) if ts_raw is not None else datetime.now().isoformat(sep=' ', timespec='seconds')

        # resource (user_log_id first)
        res_raw = _first_val(row, RES_ALIASES)
        resource = str(res_raw).strip() if res_raw is not None else 'SYSTEM'

        # Build output row: 4 pillars first, then remaining domain columns
        pillar_row = {
            'case_id':   case_id,
            'activity':  activity,
            'timestamp': timestamp,
            'resource':  resource,
        }
        for k, v in row.items():
            if k not in ALIAS_COLS_TO_DROP and k not in pillar_row:
                pillar_row[k] = v

        result.append(pillar_row)

    logging.info(f"Standardized {len(result)} rows. Columns: {list(result[0].keys()) if result else []}")
    return result


# ===========================================================================
# CocoIndex Operator 5 — Sort rows chronologically by case_id + timestamp
# ===========================================================================
@op.function()
def SortEvents(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort rows by case_id then timestamp — required for process mining."""
    def _sort_key(row):
        case  = str(row.get('case_id', ''))
        ts    = str(row.get('timestamp', ''))
        return (case, ts)
    sorted_rows = sorted(rows, key=_sort_key)
    logging.info(f"SortEvents: sorted {len(sorted_rows)} rows by case_id + timestamp.")
    return sorted_rows


# ===========================================================================
# CocoIndex Operator 6 — Cap future timestamps to now
# ===========================================================================
@op.function()
def CapFutureDates(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Snap any future timestamp back to current time."""
    now = datetime.now()
    capped = 0
    for row in rows:
        ts_str = str(row.get('timestamp', ''))
        try:
            from dateutil import parser as dp
            ts = dp.parse(ts_str)
            if ts > now:
                row['timestamp'] = now.isoformat(sep=' ', timespec='seconds')
                capped += 1
        except Exception:
            pass
    if capped:
        logging.warning(f"CapFutureDates: capped {capped} future timestamps.")
    return rows


# ===========================================================================
# CocoIndex Operator 7 — Clamp numeric boundaries (risk_score 0.0–1.0)
# ===========================================================================
@op.function()
def ClampNumericBoundaries(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Clamp risk_score to [0.0, 1.0] and event_duration_seconds to >= 0."""
    for row in rows:
        if 'risk_score' in row:
            try:
                v = float(row['risk_score'])
                row['risk_score'] = max(0.0, min(1.0, v))
            except (ValueError, TypeError):
                row['risk_score'] = 0.0
        if 'event_duration_seconds' in row:
            try:
                v = int(row['event_duration_seconds'])
                row['event_duration_seconds'] = max(0, v)
            except (ValueError, TypeError):
                row['event_duration_seconds'] = 0
    logging.info("ClampNumericBoundaries: risk_score and event_duration_seconds clamped.")
    return rows


# ===========================================================================
# CocoIndex Operator 8 — Group rare activities (< 1% frequency)
# ===========================================================================
@op.function()
def GroupRareActivities(rows: List[Dict[str, Any]], threshold: float = 0.01) -> List[Dict[str, Any]]:
    """Replace activities that appear less than threshold% with OTHER_MINOR_ACTIVITY."""
    if not rows:
        return rows
    total = len(rows)
    freq: Dict[str, int] = {}
    for row in rows:
        act = str(row.get('activity', ''))
        freq[act] = freq.get(act, 0) + 1
    rare = {act for act, cnt in freq.items() if cnt / total < threshold}
    replaced = 0
    for row in rows:
        if str(row.get('activity', '')) in rare:
            row['activity'] = 'OTHER_MINOR_ACTIVITY'
            replaced += 1
    if replaced:
        logging.info(f"GroupRareActivities: replaced {replaced} rare activity rows.")
    return rows


# ===========================================================================
# CocoIndex Operator 9 — Normalize free-text columns (remarks, user_log etc.)
# ===========================================================================
@op.function()
def NormalizeText(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Strip HTML tags, normalize whitespace and lowercase text columns."""
    text_cols = ['remarks', 'decision_reason', 'error_code']
    html_tag  = re.compile(r'<[^>]+>')
    for row in rows:
        for col in text_cols:
            if col in row and row[col]:
                val = str(row[col])
                val = html_tag.sub('', val)          # remove HTML tags
                val = re.sub(r'\s+', ' ', val)       # collapse whitespace
                val = val.strip().lower()
                row[col] = val
    logging.info("NormalizeText: cleaned text columns.")
    return rows


# ===========================================================================
# CocoIndex Operator 10 — Infer Schema from raw parsed rows
# ===========================================================================
@op.function()
def InferSchema(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Automatically detect column mappings, data types, confidence scores
    and unmatched columns from raw parsed rows."""

    if not rows:
        return {
            "inferred_mappings":  {},
            "column_types":       {},
            "unmatched_columns":  [],
            "warnings":           ["No rows to infer schema from."],
        }

    # Collect all column names from first 10 rows (handles sparse data)
    all_cols = set()
    for row in rows[:10]:
        all_cols.update(row.keys())
    all_cols = list(all_cols)

    # --- Detect data type for each column ---
    def _detect_type(values):
        non_null = [str(v).strip() for v in values if str(v).strip() not in ('', 'nan', 'None')]
        if not non_null:
            return 'unknown'
        # datetime check
        datetime_hits = 0
        for v in non_null[:10]:
            try:
                from dateutil import parser as dp
                dp.parse(v)
                datetime_hits += 1
            except Exception:
                pass
        if datetime_hits >= len(non_null[:10]) * 0.7:
            return 'datetime'
        # numeric check
        numeric_hits = sum(1 for v in non_null[:10] if re.match(r'^-?\d+(\.\d+)?$', v))
        if numeric_hits >= len(non_null[:10]) * 0.7:
            return 'numeric'
        return 'string'

    column_types = {}
    for col in all_cols:
        values = [row.get(col) for row in rows[:20]]
        column_types[col] = _detect_type(values)

    # --- Infer pillar mappings with confidence ---
    PILLAR_ALIASES = {
        'case_id':   ID_ALIASES,
        'activity':  ACT_ALIASES,
        'timestamp': TS_ALIASES,
        'resource':  RES_ALIASES,
    }

    inferred_mappings = {}
    mapped_cols = set()

    for pillar, aliases in PILLAR_ALIASES.items():
        matched_col  = None
        confidence   = 'Low'
        for i, alias in enumerate(aliases):
            if alias in all_cols:
                matched_col = alias
                # First alias = exact known match → High
                # Within first 3 = Medium, rest = Low
                confidence = 'High' if i == 0 else ('Medium' if i < 3 else 'Low')
                break
        if matched_col:
            inferred_mappings[pillar] = {
                'source_column': matched_col,
                'confidence':    confidence,
            }
            mapped_cols.add(matched_col)
        else:
            inferred_mappings[pillar] = {
                'source_column': None,
                'confidence':    'None',
            }

    # --- Unmatched columns ---
    unmatched = [c for c in all_cols if c not in mapped_cols]

    # --- Warnings ---
    warnings = []
    for pillar, info in inferred_mappings.items():
        if info['source_column'] is None:
            warnings.append(f"No column found for pillar '{pillar}' — will default to UNKNOWN.")
        elif info['confidence'] == 'Low':
            warnings.append(f"'{pillar}' mapped to '{info['source_column']}' with Low confidence.")
        elif pillar == 'activity' and info['source_column'] in ('remarks', 'user_log'):
            warnings.append("activity mapping is Low confidence — AI derivation recommended.")

    schema = {
        "inferred_mappings": inferred_mappings,
        "column_types":      column_types,
        "unmatched_columns": unmatched,
        "warnings":          warnings,
    }

    logging.info(f"InferSchema: mapped {len([v for v in inferred_mappings.values() if v['source_column']])} of 4 pillars. "
                 f"Unmatched cols: {unmatched}. Warnings: {len(warnings)}")
    return schema


# ===========================================================================
# CocoIndex Operator 11 — Serialize rows to CSV bytes
# ===========================================================================
@op.function()
def RowsToCsv(rows: List[Dict[str, Any]]) -> bytes:
    """Serialize list of dicts → UTF-8 CSV bytes."""
    if not rows:
        return b''
    buf       = io.StringIO()
    fieldnames = list(rows[0].keys())
    writer    = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction='ignore', lineterminator='\n')
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode('utf-8')


# ===========================================================================
# CocoIndex Operator 12 — Generate metadata JSON summary for each processed file
# ===========================================================================
@op.function()
def GenerateMetadata(
    source_file: str,
    total_input_rows: int,
    total_output_rows: int,
    rows_dropped: int,
    rare_activities_grouped: int,
    activity_counts: Dict[str, int],
    processing_time_seconds: float,
    processed_at: str,
    schema_report: Dict[str, Any],
) -> bytes:
    """Produce a JSON summary of the pipeline run for observability."""
    metadata = {
        "source_file":              source_file,
        "processed_at":             processed_at,
        "processing_time_seconds":  round(processing_time_seconds, 2),
        "total_input_rows":         total_input_rows,
        "total_output_rows":        total_output_rows,
        "rows_dropped":             rows_dropped,
        "rare_activities_grouped":  rare_activities_grouped,
        "activity_summary":         activity_counts,
        "schema_report":            schema_report,
    }
    logging.info(f"GenerateMetadata: {total_output_rows} rows, {len(activity_counts)} distinct activities.")
    return json.dumps(metadata, indent=2).encode("utf-8")


# ===========================================================================
# Azure Blob Trigger — orchestrates all CocoIndex operators
# ===========================================================================
@app.blob_trigger(arg_name="myblob", path="raw-zone/{name}", connection="MyStorageConn")
def standardize_blob(myblob: func.InputStream):
    # Build staging path with domain parent folder
    parts          = myblob.name.split('/')
    relative_parts = parts[1:]
    first_folder   = relative_parts[0] if len(relative_parts) > 1 else None
    if first_folder and first_folder in DOMAIN_MAP:
        relative_parts = [DOMAIN_MAP[first_folder]] + relative_parts[1:]
    blob_path = '/'.join(relative_parts)
    filename  = parts[-1]
    logging.info(f"Trigger: {myblob.name}  →  staging-zone/{blob_path}")

    try:
        start_time   = time.time()
        processed_at = datetime.utcnow().isoformat(sep=' ', timespec='seconds') + ' UTC'
        content      = myblob.read()

        # Step 1 — CocoIndex op: parse file → row dicts
        rows = ParseFile(content, filename)
        if not rows:
            logging.warning("No rows parsed — skipping.")
            return
        total_input_rows = len(rows)

        # Step 1b — CocoIndex op: infer schema from raw rows (before standardization)
        schema_report = InferSchema(rows)

        # Step 2 — CocoIndex op: standardize + AI activity derivation
        clean_rows = StandardizeRows(rows)
        if not clean_rows:
            logging.warning("No rows after standardization — skipping.")
            return

        # Step 3 — CocoIndex enhancement operators
        clean_rows = CapFutureDates(clean_rows)
        clean_rows = ClampNumericBoundaries(clean_rows)
        clean_rows = NormalizeText(clean_rows)

        clean_rows = GroupRareActivities(clean_rows)
        rare_activities_grouped = sum(
            1 for r in clean_rows if r.get('activity') == 'OTHER_MINOR_ACTIVITY'
        )

        clean_rows = SortEvents(clean_rows)

        # Step 4 — CocoIndex op: serialize to CSV
        csv_bytes = RowsToCsv(clean_rows)

        # Step 5 — CocoIndex op: generate metadata JSON
        activity_counts = {}
        for r in clean_rows:
            act = str(r.get('activity', 'UNKNOWN'))
            activity_counts[act] = activity_counts.get(act, 0) + 1

        processing_time = time.time() - start_time
        metadata_bytes  = GenerateMetadata(
            source_file             = myblob.name,
            total_input_rows        = total_input_rows,
            total_output_rows       = len(clean_rows),
            rows_dropped            = total_input_rows - len(clean_rows),
            rare_activities_grouped = rare_activities_grouped,
            activity_counts         = activity_counts,
            processing_time_seconds = processing_time,
            processed_at            = processed_at,
            schema_report           = schema_report,
        )

        # Step 6 — Upload CSV + metadata to staging-zone
        conn_str   = os.environ["MyStorageConn"]
        blob_svc   = BlobServiceClient.from_connection_string(conn_str)
        meta_path  = blob_path.rsplit('.', 1)[0] + '_metadata.json'

        blob_svc.get_blob_client("staging-zone", blob_path) \
            .upload_blob(csv_bytes, overwrite=True)
        blob_svc.get_blob_client("staging-zone", meta_path) \
            .upload_blob(metadata_bytes, overwrite=True)

        logging.info(f"Uploaded {len(clean_rows)} rows to staging-zone/{blob_path}")
        logging.info(f"Uploaded metadata to staging-zone/{meta_path}")

    except Exception as e:
        logging.error(f"Failed [{blob_path}]: {e}")
