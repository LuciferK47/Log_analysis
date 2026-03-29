# Generating Authentic SITL Crash Logs

This guide walks you through generating real ArduPilot crash logs using SITL (Software In The Loop), analyzing them with this prototype, and validating against MAVExplorer.

## Prerequisites

```bash
# One-time setup (if not done already)
bash setup_sitl.sh
```

## Step 1: Start SITL and Fly

```bash
cd ardupilot

# Start ArduCopter SITL with a console and map
sim_vehicle.py -v ArduCopter --console --map
```

Wait for the "Ready to fly" message, then in the MAVProxy console:

```
mode GUIDED
arm throttle
takeoff 50
```

Wait until the copter reaches ~50m altitude (watch the console output).

## Step 2: Inject a Fault

### Motor Failure (most dramatic)
```
param set SIM_ENGINE_FAIL 1
```
This kills motor 1. The copter will spin and crash within seconds. Wait for it to hit the ground, then:
```
disarm
```

### GPS Glitch
```
param set SIM_GPS_GLITCH_X 0.001
param set SIM_GPS_GLITCH_Y 0.001
```
Watch the copter drift. After 10-15s:
```
param set SIM_GPS_GLITCH_X 0
param set SIM_GPS_GLITCH_Y 0
```

### Vibration
```
param set SIM_VIB_MOT_MAX 30
```
Let it fly for 20+ seconds, then:
```
param set SIM_VIB_MOT_MAX 0
```

## Step 3: Retrieve the .BIN Log

After disarming or crashing, press `Ctrl+C` to stop SITL.

The DataFlash `.BIN` log is saved at:
```
ardupilot/logs/00000001.BIN
```

(The number increments with each flight. Check `ls ardupilot/logs/` for the latest.)

## Step 4: Analyze with the Prototype

```bash
cd ../prototype

# Generate the diagnostic report + plot
python3 cli.py \
    --log ../ardupilot/logs/00000001.BIN \
    --plot \
    --plot-output ../docs/images/motor_loss_diagnostic.png \
    --output ../docs/motor_loss_report.json \
    -v
```

This will produce:
- `docs/images/motor_loss_diagnostic.png` — the automated diagnostic plot
- `docs/motor_loss_report.json` — the machine-readable findings

## Step 5: Cross-Validate in MAVExplorer

MAVExplorer is ArduPilot's built-in log visualization tool. Use it to manually verify what the prototype detected.

```bash
# Open the same .BIN file in MAVExplorer
MAVExplorer.py ../ardupilot/logs/00000001.BIN
```

In the MAVExplorer GUI:

1. **For motor loss**, graph these signals:
   - `ATT.Roll` and `ATT.DesRoll` (shows attitude divergence)
   - `RCOU.C1`, `RCOU.C2`, `RCOU.C3`, `RCOU.C4` (shows motor saturation)

2. **For GPS glitch**, graph:
   - `GPS.HDop` (shows position accuracy degradation)
   - `NKF4.SP` (shows EKF innovation spike)

3. **For vibration**, graph:
   - `VIBE.VibeX`, `VIBE.VibeY`, `VIBE.VibeZ`
   - `VIBE.Clip0` (IMU clipping events)

### Taking the Screenshot
- Arrange the MAVExplorer graph windows side by side
- Take a screenshot: `gnome-screenshot -a -f docs/images/mavexplorer_motor_loss.png`
- Or use `Shift+PrintScreen` to select the region

## Step 6: Side-by-Side Comparison

Place both images in `docs/images/`:
```
docs/images/
├── motor_loss_diagnostic.png    ← Your prototype's output
└── mavexplorer_motor_loss.png   ← MAVExplorer's manual view
```

This proves your automated tool correctly pinpoints the same fault that a human would find manually in MAVExplorer — the core value proposition for GSoC.
