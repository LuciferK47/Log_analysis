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

### V1 Design Characteristics

- **Telemetry contract:** `feature_registry.yaml` defined `priority_1` and fallback expressions so diagnostic features survived firmware field-name drift.
- **Compute model:** feature fallback math was parsed through a safe AST pipeline to keep expressions auditable and avoid dynamic eval risk.
- **Temporal logic model:** `rules.yaml` encoded deterministic fault conditions with sustained truth windows to suppress noise spikes.
- **Data access pattern:** selective extraction pulled only referenced `MSG.Field` pairs to reduce unnecessary parsing work.
- **Output semantics:** JSON reports and diagnostic plots were explainable, rule-linked, and easy for reviewers to trace.

This baseline was excellent for explainability and rapid iteration, but high-volume logs revealed memory pressure and asynchronous join fragility.

## V2 Architecture Update (DuckDB + Parquet)

To scale from proof-of-concept to production-size logs, the execution engine was redesigned while preserving the same YAML contracts and diagnostic semantics.

```
┌─────────────┐    ┌───────────────────┐    ┌───────────────────┐    ┌──────────────────┐
│  .bin Log    │───▶│  Ingestion       │───▶│  Feature Views     │───▶│  Rule Engine      │
│  (DataFlash) │    │  (PyArrow +      │    │  (ASOF joins +     │    │  (SQL windows +   │
│              │    │   Parquet shards)│    │   SQL COALESCE)    │    │   causal arbiter) │
└─────────────┘    └────────┬──────────┘    └───────────────────┘    └────────┬─────────┘
          │                                                 │
          ▼                                                 ▼
       ┌───────────────────┐                              ┌──────────────────┐
       │  DuckDB Engine    │                              │ Diagnostic Plot   │
       │  (out-of-core,    │                              │ + JSON Report     │
       │   memory-bounded) │                              │ (same semantics)  │
       └───────────────────┘                              └──────────────────┘
```

### V2 Design Characteristics

- **Telemetry contract:** same YAML interfaces and rule semantics were preserved, so community rule authoring workflows did not change.
- **Compute model:** execution moved to DuckDB SQL with bounded memory control (`PRAGMA memory_limit='4GB'`) for out-of-core reliability.
- **Temporal logic model:** rule windows and causality now execute natively in SQL, including sequence tracking via `diagnostic_meta_log`.
- **Data access pattern:** ingestion streams 100k-row Parquet shards with explicit PyArrow schemas to prevent sparse type inference failures.
- **Output semantics:** reports and plots remain equivalent in meaning, but are now produced through a scale-safe execution engine.

### Transition: From Idea to Scale

- **What stayed stable:** rule intent, YAML contracts, feature naming, report structure, and interpretability.
- **What shifted:** in-memory processing to out-of-core SQL, ad-hoc joins to deterministic `ASOF LEFT JOIN`, and shared temp paths to PID-isolated spill directories.
- **Why this matters:** readers can understand the continuity of diagnostic reasoning and the engineering shift required to support large real-world logs and parallel batch runs.

## Quick Start

```bash
cd prototype
pip install -r requirements.txt

# Analyze a real SITL crash log
python3 cli.py --log /path/to/flight.BIN --plot -v

# Analyze with custom rules
python3 cli.py --log flight.BIN --rules my_rules.yaml --plot --output report.json
```

For a complete guide on generating crash logs with SITL, see [docs/SITL_TUTORIAL.md](docs/SITL_TUTORIAL.md).

### Smoke Test with Dummy Data

For quick verification that the pipeline works, simulated scenarios are available:

```bash
python3 cli.py --dummy motor_loss --plot -v
python3 cli.py --dummy gps_glitch --plot -v
python3 cli.py --dummy vibration --plot -v
```

### Batch Analysis

To process multiple `.bin` logs concurrently and generate reports/plots for all of them:

```bash
python3 batch_analyze.py
```
This will automatically scan the `Logs/` directory and output all JSON reports and PNG plots into the `analysis_results/` folder.


## How It Compares to MAVExplorer

MAVExplorer is ArduPilot's standard log viewer — it's a manual graphing tool where a pilot selects signals to plot and visually inspects them. This prototype automates that process:

| | MAVExplorer | This Tool |
|---|---|---|
| **Input** | Pilot manually selects signals | Automatically reads all relevant signals |
| **Analysis** | Human eyeballs anomalies | YAML rules evaluate the full timeline |
| **Output** | Interactive plots | Diagnostic report + plot with exact fault window |
| **Speed** | Minutes per log | Seconds per log |
| **Extensibility** | N/A | Community adds rules via YAML |

The [SITL tutorial](docs/SITL_TUTORIAL.md) includes instructions for generating a side-by-side comparison between this tool and MAVExplorer on the same crash log.

## Detected Failure Classes

| Failure | Key Signals | Hysteresis | SITL Command |
|---------|------------|------------|--------------|
| Motor/ESC Loss | ATT.Roll, RCOU.C1-C4 | 1.5s | `SIM_ENGINE_FAIL=1` |
| GPS Glitch | GPS.HDop, NKF4.SP | 2.0s | `SIM_GPS_GLITCH_X=0.001` |
| Excessive Vibration | VIBE.VibeX/Y/Z, Clip0 | 1.0s | `SIM_VIB_MOT_MAX=30` |
| Battery Sag | BATT.Volt | 3.0s | `SIM_BATT_VOLTAGE=13.5` |
| EKF Divergence | NKF4.SP, GPS.HDop | 2.0s | Complex multi-sensor |

## Adding Custom Rules

Rules live in `rules.yaml`. Adding a new heuristic requires zero Python changes:

```yaml
my_custom_rule:
  conditions:
    - feature: "some_feature"
      operator: ">"
      threshold: 42
  logic: "AND"
  duration_seconds: 2.0    # Must stay true for 2s straight
  severity: "WARNING"
  confidence: 0.75
  suggested_fix: "Check XYZ."
  plot_signals:
    - "SOME.Signal"
```

## Project Structure

```
prototype/
├── cli.py                  # CLI entry point
├── ingestion.py            # Memory-efficient .bin reader (pymavlink DFReader)
├── abstraction.py          # YAML-driven feature extraction (AST-based math)
├── rule_engine.py          # Temporal rule evaluation with per-rule hysteresis
├── visualize.py            # Diagnostic plots with exact fault-window shading
├── rag_pipeline.py         # Retrieval-Augmented Generation (ChromaDB) for context
├── ingest_kb.py            # Knowledge base ingestion for RAG
├── feature_registry.yaml   # Feature definitions with version fallbacks
├── rules.yaml              # Diagnostic rules (community-extensible)
├── setup_sitl.sh           # SITL environment setup script
└── requirements.txt        # Python dependencies
docs/
├── SITL_TUTORIAL.md        # Step-by-step guide for test data
└── images/                 # Authentic diagnostic plots
batch_analyze.py            # Pipeline script for bulk log processing
debug.py                    # Sandbox script for testing telemetry
Proposal_2.tex              # Comprehensive GSoC 2026 LaTeX Proposal
```

## License

Part of ArduPilot ecosystem — GPLv3.
