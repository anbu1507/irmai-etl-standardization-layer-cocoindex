import azure.functions as func
import pandas as pd
import numpy as np
import io
import logging
import re
import os
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

# --- Column alias mappings ---
ID_ALIASES  = ['case_id', 'policy_id', 'policy_no', 'policy_num', 'user_log_id']
ACT_ALIASES = ['activity', 'activity_name', 'event_action', 'user_log']
TS_ALIASES  = ['event_timestamp', 'timestamp', 'user_log_datetime']
RES_ALIASES = ['resource', 'resource_type', 'user_log_added_by_name']

def strip_prefix(val):
    if pd.isna(val) or val == 'NA':
        return val
    return re.sub(r'^[A-Z]{2}_', '', str(val)).strip()

def read_smart_dataframe(blob_bytes):
    """Detects encoding/format and skips junk headers."""
    # 1. Try to detect if it's actually an Excel file despite .csv extension
    if blob_bytes.startswith(b'\xd0\xcf\x11\xe0'):
        logging.info("Binary signature matches .xls (Legacy Excel). Reading as Excel.")
        return pd.read_excel(io.BytesIO(blob_bytes))
    elif blob_bytes.startswith(b'PK\x03\x04'):
        logging.info("Binary signature matches .xlsx (Excel). Reading as Excel.")
        return pd.read_excel(io.BytesIO(blob_bytes))

    # 2. Try CSV with different encodings
    for enc in ['utf-8', 'cp1252', 'latin-1']:
        try:
            # First, read the whole thing as text to find the header
            text_content = blob_bytes.decode(enc)
            
            # Look for the real header row index
            if 'user_log_id' in text_content:
                lines = text_content.splitlines()
                skip_rows = next(i for i, line in enumerate(lines) if 'user_log_id' in line)
                logging.info(f"Found header at row {skip_rows} using {enc}")
                return pd.read_csv(io.StringIO(text_content), skiprows=skip_rows)
            
            # If header not found, try reading normally
            return pd.read_csv(io.BytesIO(blob_bytes), encoding=enc)
        except Exception:
            continue
            
    raise ValueError("Unable to decode file. Check if file is corrupted or unsupported.")

@app.blob_trigger(arg_name="myblob", path="raw-zone/{name}", connection="MyStorageConn")
def standardize_blob(myblob: func.InputStream):
    file_name = myblob.name.split('/')[-1]
    logging.info(f"Processing: {file_name}")

    try:
        # Load the data using the smart reader
        blob_content = myblob.read()
        df = read_smart_dataframe(blob_content)

        if df.empty:
            logging.warning("File is empty after parsing.")
            return

        # --- Standard ETL Logic ---
        df = df.drop_duplicates()

        # Identifier Consolidation
        existing_ids = [c for c in ID_ALIASES if c in df.columns]
        if existing_ids:
            df['case_id'] = df[existing_ids].apply(
                lambda row: ', '.join(dict.fromkeys(
                    [strip_prefix(x) for x in row if pd.notna(x) and str(x).strip() != 'NA']
                )), axis=1
            )
        else:
            df['case_id'] = 'UNKNOWN'

        # Activity Consolidation
        existing_acts = [c for c in ACT_ALIASES if c in df.columns]
        df['activity'] = df[existing_acts].apply(
            lambda row: ' | '.join(dict.fromkeys(
                [str(x).strip() for x in row if pd.notna(x)]
            )), axis=1
        ) if existing_acts else 'UNKNOWN'

        # Timestamp mapping
        found_ts = next((c for c in TS_ALIASES if c in df.columns), None)
        df['timestamp'] = pd.to_datetime(df[found_ts], errors='coerce') if found_ts else pd.Timestamp.now()
        df['timestamp'] = df['timestamp'].fillna(pd.Timestamp.now())

        # Resource mapping
        found_res = next((c for c in RES_ALIASES if c in df.columns), None)
        df['resource'] = df[found_res] if found_res else 'SYSTEM'

        # Final Cleanup
        mandatory_pillars = ['case_id', 'activity', 'timestamp', 'resource']
        other_cols = [c for c in df.columns if c not in mandatory_pillars]
        df = df[mandatory_pillars + other_cols]

        # Upload to staging
        conn_str = os.environ["MyStorageConn"]
        service_client = BlobServiceClient.from_connection_string(conn_str)
        output_csv = df.to_csv(index=False)
        service_client.get_blob_client("staging-zone", file_name).upload_blob(output_csv, overwrite=True)

        logging.info(f"✅ Successfully standardized {file_name}")

    except Exception as e:
        logging.error(f"❌ Error: {str(e)}")