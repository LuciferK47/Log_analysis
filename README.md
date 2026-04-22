# ArduPilot Log Diagnostic Tool

**GSoC 2026 Prototype — Automated Root-Cause Detection for Flight Failures**

A diagnostic tool that analyzes ArduPilot DataFlash `.bin` logs to automatically pinpoint root causes of flight failures. Instead of manually graphing signals in MAVExplorer and eyeballing anomalies, this tool runs a declarative rule engine against the full telemetry timeline and highlights exactly where and why a failure occurred.

## V1 Baseline Architecture (Original Iteration)

The first iteration established the core diagnostic idea as a deterministic expert system. The design goal was interpretability: every diagnosis should map to explicit telemetry thresholds in YAML, not opaque model behavior.

```
┌─────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  .bin Log    │───▶│  Ingestion       │───▶│  Feature         │───▶│  Rule Engine      │
│  (DataFlash) │    │  (DFReader,      │    │  Abstraction     │    │  (YAML rules,     │
│              │    │   selective      │    │  (AST parser,    │    │   temporal eval,   │
│              │    │   extraction)    │    │   fallbacks)     │    │   hysteresis)      │
└─────────────┘    └──────────────────┘    └──────────────────┘    └────────┬───────────┘
                     │
                     ▼
                   ┌──────────────────┐
                   │ Diagnostic Plot   │
                   │ + JSON Report     │
                   │ (fault window +   │
                   │  causal summary)  │
                   └──────────────────┘
```

## V2 Architecture: Polars Lazy Execution & ML Hybrid Fallback

To scale from proof-of-concept to production, the execution engine was completely overhauled. We transitioned to a high-speed Rust-based dataframe backend (Polars) and integrated a Machine Learning fallback to catch complex, non-deterministic anomalies.

```
┌─────────────┐    ┌───────────────────┐    ┌───────────────────┐     ┌──────────────────┐
│  .bin Log    │───▶│  Parquet Ingestion│───▶│  Polars Rule      │───▶ │  (If Ambiguous)  │
│  (DataFlash) │    │  (PyArrow out-of- │    │  Engine (Lazy     │  │  │  XGBoost ML      │
│              │    │   core chunking)  │    │  execution)       │  │  │  Classifier      │
└─────────────┘    └───────────────────┘    └────────┬──────────┘  │  └────────┬─────────┘
                                                     │             │           │
                                                     ▼             ▼           ▼
                                                  ┌────────────────────────────────┐
                                                  │         ChromaDB RAG           │
                                                  │       Vector DB Context        │
                                                  └───────────────┬────────────────┘
                                                                  │
                                                                  ▼
                                                  ┌────────────────────────────────┐
                                                  │      FastAPI / CLI Output      │
                                                  │      (JSON Diagnostic Payload) │
                                                  └────────────────────────────────┘
```

### Key Engineering Achievements

*   **Out-of-Core Processing**: Using PyArrow, we chunk large `.bin` logs directly into partitioned Parquet files. This keeps RAM usage strictly bounded, preventing OOM crashes on massive multi-hour flight logs.
*   **Polars Lazy Evaluation**: The engine leverages `pl.scan_parquet()` and `join_asof()` to temporally align asynchronous sensors natively in Rust/Arrow before ever triggering computation. This results in incredibly fast query resolution (~50ms execution times).
*   **XGBoost Fallback (Early Detection)**: If the deterministic rules fail to catch a failure or present ambiguous data, a cost-sensitively trained XGBoost model evaluates the rolling windows to catch creeping faults (e.g., detecting motor failure via feature correlation up to 4 seconds early). It was trained using Group-Shuffle-Split to explicitly prevent time-series target leakage across sequential flight windows.
*   **ChromaDB Vector RAG**: When an anomaly is detected (deterministically or via ML), a local semantic search is triggered in a persistent ChromaDB instance. This retrieves the exact ArduPilot Wiki documentation and integrates it directly into the JSON troubleshooting payload.
*   **Kinematic Divergence Tracking**: The engine natively tracks aerodynamic control loss by calculating the absolute error between pilot setpoint (ATT.DesRoll) and actual physical response (ATT.Roll) entirely within the Rust/Arrow memory space.
*   **Meta-Log Duration Analysis**: The Causal Arbiter now tracks the complete temporal footprint of an anomaly. Instead of just flagging the onset, it calculates the hit_count and total duration_seconds of the failure state.

## Quick Start

### CLI Interface

```bash
cd prototype
pip install -r requirements.txt
cd ..

# Analyze a physical flight log automatically
python3 cli.py analyze --log /path/to/flight.BIN
```

### FastAPI Backend

This architecture is fully web-compatible for ArduPilot WebTools integration. 

**Start the Server:**
```bash
uvicorn api:app --reload
```

**Analyze a Log via REST:**
```bash
curl -X POST -F "file=@Logs/Faulty/2022-06-27 13-14-19.bin" http://127.0.0.1:8000/analyze
```

### Sample API Response

```json
{
  "status": "failure",
  "confidence": 0.85,
  "rule_triggered": "xgboost_ml_fallback",
  "description": "Anomaly detected via ML feature correlation (Deterministic rules bypassed).",
  "evidence": {
    "RCOU.C1": 1886,
    "ATT.Roll": 1.79
  },
  "timestamp_window": [
    34453552,
    null
  ],
  "meta_log": {
    "hit_count": 1356,
    "onset_timeus": 34453552,
    "resolution_timeus": 482337764,
    "duration_seconds": 447.88
  },
  "rag_context": "When an ESC fails or a motor desyncs, ArduPilot will push the corresponding RCOU channel to its maximum limit (e.g., 1900+ PWM) to compensate..."
}
```

## Project Structure

```
prototype/
├── api.py                  # FastAPI REST backend for WebTools integration
├── cli.py                  # CLI Orchestrator entry point
├── ingest_chunked.py       # Memory-safe out-of-core PyArrow ingestion
├── rule_engine_polars.py   # Polars lazy evaluation & Hybrid orchestrator
├── train_xgboost.py        # Group-Shuffle-Split ML training pipeline
├── xgboost_fallback.json   # Pre-trained XGBoost classification model
├── rag_pipeline.py         # Retrieval-Augmented Generation (ChromaDB)
├── ingest_kb.py            # Local Vector Database constructor for Wiki text
├── rules.yaml              # Declarative diagnostic rules (community-extensible)
└── requirements.txt        # Python dependencies
ardupilot_knowledge_base/   # Compiled local ChromaDB instance
Logs/                       # Real flight datasets (Healthy / Faulty splits)
```

## How It Compares to MAVExplorer

MAVExplorer is ArduPilot's standard log viewer — it's a manual graphing tool where a pilot selects signals to plot and visually inspects them. This prototype automates that process:

| | MAVExplorer | This Tool |
|---|---|---|
| **Input** | Pilot manually selects signals | Automatically reads all relevant signals |
| **Analysis** | Human eyeballs anomalies | YAML rules evaluate the full timeline (with ML fallback) |
| **Output** | Interactive plots | Diagnostic JSON payload mapped to RAG Context |
| **Speed** | Minutes per log | Seconds per log |
| **Extensibility** | N/A | Community adds rules via YAML |

## License

Part of ArduPilot ecosystem — GPLv3.
