# ArduPilot Log Diagnostic Tool

**GSoC 2026 Prototype — AI-Assisted Log Diagnosis & Root-Cause Detection**

An automated diagnostic tool that analyzes ArduPilot DataFlash `.bin` logs to pinpoint root causes of flight failures. This prototype demonstrates the core pipeline architecture proposed for GSoC 2026.

## Architecture

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────┐    ┌──────────────┐
│  .bin Log    │───▶│  Log Ingestion   │───▶│  Feature     │───▶│  Rule Engine  │
│  (DataFlash) │    │  (DFReader +     │    │  Abstraction │    │  (YAML-driven │
│              │    │   10Hz resample) │    │  (YAML       │    │   heuristics) │
└─────────────┘    └──────────────────┘    │   fallbacks) │    └──────┬───────┘
                                           └──────────────┘           │
                                                                      ▼
                                                              ┌──────────────┐
                                                              │ Diagnostic   │
                                                              │ Report (JSON │
                                                              │ + Plot)      │
                                                              └──────────────┘
```

## Key Innovation: Version-Agnostic Feature Extraction

ArduPilot's existing LogAnalyzer breaks when firmware updates rename log fields. This tool uses a **declarative YAML Feature Registry** with fallback priorities:

```yaml
roll_tracking_error:
  priority_1: "ATT.ErrRP"                        # Use if available
  fallback: "abs(${ATT.DesRoll} - ${ATT.Roll})"  # Compute if not
  aggregation: "max"
```

If firmware 4.5 renames a field, only the YAML config needs updating — not the Python code.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run with simulated motor failure
python cli.py --dummy motor_loss --plot

# Run with simulated GPS glitch
python cli.py --dummy gps_glitch --plot

# Run with simulated vibration fault
python cli.py --dummy vibration --plot

# Analyze a real SITL crash log
python cli.py --log /path/to/flight.bin --plot --output report.json
```

## Testing with SITL

To generate real crash logs for testing:

```bash
# 1. Set up ArduPilot SITL (one-time)
bash setup_sitl.sh

# 2. Run the simulator
cd ardupilot && sim_vehicle.py -v ArduCopter -w

# 3. In the MAVProxy console:
#    mode GUIDED
#    arm throttle
#    takeoff 50
#    param set SIM_ENGINE_FAIL 1   ← injects motor failure
#
# 4. Wait for crash, then Ctrl+C
# 5. Find the .bin log in ardupilot/logs/

# 6. Analyze the crash log
cd ../prototype
python cli.py --log ../ardupilot/logs/00000001.BIN --plot
```

## Detected Failure Classes

| Failure | Signals Used | SITL Injection |
|---------|-------------|----------------|
| Motor/ESC Loss | ATT.Roll, RCOU.C1-C4 | `SIM_ENGINE_FAIL=1` |
| GPS Glitch | GPS.HDop, NKF4.SP | `SIM_GPS_GLITCH_X=0.001` |
| Excessive Vibration | VIBE.VibeX/Y/Z, Clip0 | `SIM_VIB_MOT_MAX=30` |
| Battery Sag | BATT.Volt | `SIM_BATT_VOLTAGE=13.5` |
| EKF Divergence | NKF4.SP, GPS.HDop | Complex multi-sensor |

## Adding Custom Rules

Rules are defined in `rules.yaml`. To add a new heuristic, just add a YAML block:

```yaml
my_custom_rule:
  conditions:
    - feature: "some_feature"
      operator: ">"
      threshold: 42
  logic: "AND"
  severity: "WARNING"
  confidence: 0.75
  suggested_fix: "Check XYZ."
  plot_signals:
    - "SOME.Signal"
```

No Python code changes needed.

## Project Structure

```
prototype/
├── cli.py                  # CLI entry point
├── ingestion.py            # DataFlash .bin reader (pymavlink DFReader)
├── abstraction.py          # YAML-driven feature extraction with safe fallbacks
├── rule_engine.py          # Declarative rule evaluation engine
├── visualize.py            # Multi-panel diagnostic plot generator
├── feature_registry.yaml   # Feature definitions with version fallbacks
├── rules.yaml              # Diagnostic rules (community-extensible)
├── setup_sitl.sh           # SITL environment setup script
└── requirements.txt        # Python dependencies
```

## License

Part of ArduPilot ecosystem — GPLv3.
