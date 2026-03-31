import os
import subprocess
import glob
import json

log_files = glob.glob('Logs/*.bin') + glob.glob('Logs/*.BIN')
results_dir = 'analysis_results'

os.makedirs(results_dir, exist_ok=True)

print(f"Found {len(log_files)} logs. Starting batch analysis...\n")

for log_path in log_files:
    basename = os.path.basename(log_path).rsplit('.', 1)[0]
    safe_name = basename.replace(' ', '_')
    plot_out = os.path.join(results_dir, f"{safe_name}_plot.png")
    json_out = os.path.join(results_dir, f"{safe_name}_report.json")
    
    print(f"Analyzing {basename}...", end="", flush=True)
    
    cmd = [
        "python3", "prototype/cli.py",
        "--log", log_path,
        "--plot-output", plot_out,
        "--output", json_out
    ]
    
    try:
        subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if os.path.exists(json_out):
            with open(json_out, 'r') as f:
                report = json.load(f)
                if report.get('status') == 'success' and report.get('primary_root_cause'):
                    rule_name = report['primary_root_cause'].get('rule_name')
                    print(f"\rAnalyzing {basename}... \033[91m[!] FAULT: {rule_name}\033[0m")
                else:
                    print(f"\rAnalyzing {basename}... \033[92m[✓] Clean.\033[0m")
        else:
            print(f"\rAnalyzing {basename}... \033[93m[-] No JSON generated.\033[0m")
            
    except subprocess.TimeoutExpired:
        print(f"\rAnalyzing {basename}... \033[93m[-] Timeout.\033[0m")
    except Exception as e:
        print(f"\rAnalyzing {basename}... \033[91m[x] Error: {e}\033[0m")

print("\nBatch processing complete! Check the analysis_results/ folder.")
