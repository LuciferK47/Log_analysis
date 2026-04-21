import os
import shutil
import uuid
from fastapi import FastAPI, UploadFile, File
import sys

# Add prototype to sys path to cleanly import backend scripts
sys.path.append(os.path.join(os.path.dirname(__file__), "prototype"))

from prototype.ingest_chunked import ingest_log
from prototype.rule_engine_polars import evaluate_pipeline

app = FastAPI(title="ArduPilot Diagnostic API")

RULES_PATH = os.path.abspath("prototype/rules.yaml")

@app.post("/analyze")
async def analyze_log(file: UploadFile = File(...)):
    # Create unique temp paths
    run_id = str(uuid.uuid4())[:8]
    tmp_bin_path = f".tmp_upload_{run_id}.bin"
    tmp_parquet_dir = f".tmp_parquet_{run_id}"
    
    try:
        # Save the uploaded file to disk
        with open(tmp_bin_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        # Step 1: Ingest
        # The ingestion function writes to output_dir
        ingest_log(tmp_bin_path, tmp_parquet_dir)
        
        att_path = os.path.join(tmp_parquet_dir, "ATT.parquet")
        rcou_path = os.path.join(tmp_parquet_dir, "RCOU.parquet")
        
        # Step 2: Evaluate
        report = evaluate_pipeline(att_path, rcou_path, RULES_PATH)
        
        return report
        
    finally:
        # Step 3: Cleanup
        if os.path.exists(tmp_bin_path):
            os.remove(tmp_bin_path)
        if os.path.exists(tmp_parquet_dir):
            shutil.rmtree(tmp_parquet_dir, ignore_errors=True)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="127.0.0.1", port=8000, reload=True)
