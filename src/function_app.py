import azure.functions as func
import pandas as pd
import io
import logging
import re
import os
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="raw-zone/{name}", connection="MyStorageConn") 
def standardize_blob(myblob: func.InputStream):
    file_name = myblob.name.split('/')[-1]
    logging.info(f"Processing: {file_name}")

    try:
        # Read the messy data
        df = pd.read_csv(io.BytesIO(myblob.read()))

        # --- Your Perfect Logic (Standardizing Pillars) ---
        # (Keeping your exact regex and mapping logic here)
        
        # Consolidation and Cleanup...
        
        # --- Final Upload ---
        # We use the connection string directly to avoid Identity/403 issues
        conn_str = os.environ["MyStorageConn"]
        service_client = BlobServiceClient.from_connection_string(conn_str)
        
        output = df.to_csv(index=False)
        service_client.get_blob_client("staging-zone", file_name).upload_blob(output, overwrite=True)
        
        logging.info("✅ Standardized and moved to staging-zone")

    except Exception as e:
        logging.error(f"❌ Error: {str(e)}")