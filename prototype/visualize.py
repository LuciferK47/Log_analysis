import logging
import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

def generate_diagnostic_plot(df: pd.DataFrame, findings: list, events: list = None, output_path: str = 'output/plot.png'):
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    faults = [f for f in findings if f.get('status') == 'FAULT_DETECTED']

    if not faults:
        logger.info("No faults to visualize.")
        fig, ax = plt.subplots(figsize=(12, 6))
        for i, col in enumerate(df.columns[:4]):
            if not col.startswith('__'):
                ax.plot(df.index.total_seconds(), df[col], label=col)
        ax.set_title("Log Overview - No Faults")
        ax.set_xlabel("Time (seconds)")
        ax.legend()
        fig.savefig(output_path, dpi=150)
        plt.close(fig)
        return output_path

    n_panels = min(len(faults), 4)  # Plot top 4 faults max
    fig, axes = plt.subplots(n_panels, 1, figsize=(12, 4 * n_panels), sharex=True)
    if n_panels == 1:
        axes = [axes]
        
    time_seconds = df.index.total_seconds()

    for idx, fault in enumerate(faults[:n_panels]):
        ax = axes[idx]
        signals = fault.get('plot_signals', [])
        fault_start = fault.get('fault_start')
        fault_end = fault.get('fault_end')

        for sig in signals:
            if sig in df.columns:
                ax.plot(time_seconds, df[sig], label=sig)

        if fault_start is not None and fault_end is not None:
            fs = fault_start.total_seconds()
            fe = fault_end.total_seconds()
            ax.axvspan(fs, fe, color='red', alpha=0.3, label='Fault Window')
            ax.set_title(f"[{fault.get('severity')}] {fault.get('rule_name')} (Conf: {fault.get('confidence'):.2f})")
            
        ax.legend()
        ax.grid(True, alpha=0.3)

    plt.xlabel('Time (seconds)')
    plt.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    return output_path
