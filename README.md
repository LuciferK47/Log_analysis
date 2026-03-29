# ArduPilot Log Diagnostic Tool

**GSoC 2026 Prototype — Automated Root-Cause Detection for Flight Failures**

A diagnostic tool that analyzes ArduPilot DataFlash `.bin` logs to automatically pinpoint root causes of flight failures. Instead of manually graphing signals in MAVExplorer and eyeballing anomalies, this tool runs a declarative rule engine against the full telemetry timeline and highlights exactly where and why a failure occurred.

## Architecture

```
┌─────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  .bin Log    │───▶│  Ingestion       │───▶│  Feature         │───▶│  Rule Engine      │
│  (DataFlash) │    │  (DFReader,      │    │  Abstraction     │    │  (YAML rules,     │
│              │    │   memory-        │    │  (AST parser,    │    │   temporal eval,   │
│              │    │   efficient)     │    │   YAML fallbacks)│    │   hysteresis)      │
└─────────────┘    └──────────────────┘    └──────────────────┘    └────────┬───────────┘
                                                                           │
                                                                           ▼
                                                                   ┌──────────────────┐
                                                                   │ Diagnostic Plot   │
                                                                   │ + JSON Report     │
                                                                   │ (exact fault      │
                                                                   │  window shading)  │
                                                                   └──────────────────┘
```

### Key Design Decisions

- **Version-agnostic features**: A YAML registry with fallback expressions handles firmware field renames without code changes.
- **Temporal evaluation**: Rules evaluate the full time-series — AND conditions must be simultaneously true for a configurable `duration_seconds` to prevent false positives from noise spikes.
- **Memory-efficient parsing**: Only the specific MSG.Field combinations referenced in the YAML are extracted from the `.bin` file.
- **Safe expression parsing**: Uses Python's `ast` module instead of `eval()` for computing fallback expressions.

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
├── feature_registry.yaml   # Feature definitions with version fallbacks
├── rules.yaml              # Diagnostic rules (community-extensible)
├── setup_sitl.sh           # SITL environment setup script
└── requirements.txt        # Python dependencies
docs/
├── SITL_TUTORIAL.md        # Step-by-step guide for generating authentic test data
└── images/                 # Authentic diagnostic plots (generated, not committed)
```

## License

Part of ArduPilot ecosystem — GPLv3.
