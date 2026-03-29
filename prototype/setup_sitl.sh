#!/bin/bash
# Professional dependency setup script for ArduPilot Log Diagnosis Prototype
# This script prepares the SITL (Software In The Loop) environment for testing.

echo "================================================"
echo " Setting up ArduPilot SITL Environment"
echo "================================================"

if [ -d "ardupilot" ]; then
    echo "ArduPilot directory already exists locally. Skipping clone."
else
    echo "Cloning lightweight ArduPilot repository..."
    # Deep clone is not necessary for just running SITL, depth 1 saves time and space
    git clone -b master --depth 1 --recurse-submodules https://github.com/ArduPilot/ardupilot.git
fi

echo "Installing SITL system dependencies..."
cd ardupilot
Tools/environment_install/install-prereqs-ubuntu.sh -y

echo "================================================"
echo " SITL Setup Complete!"
echo "================================================"
echo "To generate a test crash log for the diagnostic tool:"
echo "  1. cd ardupilot/ArduCopter"
echo "  2. sim_vehicle.py -v ArduCopter -L KSFO --map --console"
echo "  3. In the MAVProxy console, type: param set SIM_ENGINE_FAIL 1"
echo "  4. Type: mode GUIDED, then: arm throttle, then: takeoff 50"
echo "  5. The drone will crash. Extract the .bin from the logs/ directory!"
