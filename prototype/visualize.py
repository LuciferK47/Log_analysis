"""
visualize.py — Diagnostic Telemetry Visualization

Generates multi-panel plots of raw telemetry signals, with EXACT fault-window
shading using the fault_start and fault_end timestamps returned by the
temporal rule engine. No more derivative-based guessing.
"""
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

# Color palette for signal lines
COLORS = [
    '#e74c3c', '#3498db', '#2ecc71', '#f39c12',
    '#9b59b6', '#1abc9c', '#e67e22', '#34495e',
]


def generate_diagnostic_plot(df: pd.DataFrame,
                              findings: list[dict],
                              output_path: str = 'diagnostic_report.png'):
    """
    Generate a multi-panel diagnostic plot based on triggered rules.

    Each triggered rule gets its own subplot showing the relevant signals.
    The exact fault window (fault_start → fault_end from the rule engine)
    is highlighted with a colored shaded region.

    Args:
        df: The raw resampled telemetry DataFrame from ingestion.
        findings: List of triggered rule findings from the rule engine.
        output_path: Path to save the output image.
    """
    # Filter to actual triggered faults (skip OK status)
    faults = [f for f in findings if f.get('status') == 'FAULT_DETECTED']

    if not faults:
        logger.info("No faults to visualize.")
        _plot_overview(df, output_path)
        return

    n_panels = len(faults)
    fig, axes = plt.subplots(n_panels, 1,
                              figsize=(14, 4.5 * n_panels),
                              sharex=True)
    if n_panels == 1:
        axes = [axes]

    plt.style.use('seaborn-v0_8-darkgrid')

    for ax, fault in zip(axes, faults):
        signals = fault.get('plot_signals', [])
        rule_name = fault.get('rule_name', 'unknown')
        severity = fault.get('severity', 'WARNING')
        confidence = fault.get('confidence', 0)
        fault_start = fault.get('fault_start')
        fault_end = fault.get('fault_end')
        duration = fault.get('fault_duration_s', 0)

        # Convert index to seconds for plotting
        time_seconds = df.index.total_seconds()

        # Plot each signal requested by the rule
        plotted = 0
        for i, sig in enumerate(signals):
            if sig in df.columns:
                color = COLORS[i % len(COLORS)]
                ax.plot(time_seconds, df[sig],
                        label=sig, color=color,
                        linewidth=1.5, alpha=0.85)
                plotted += 1

        if plotted == 0:
            ax.text(0.5, 0.5, f'Signals not found: {signals}',
                    transform=ax.transAxes, ha='center', va='center',
                    fontsize=11, color='gray')

        # Shade the EXACT fault window from the rule engine
        if fault_start is not None and fault_end is not None:
            fs = fault_start.total_seconds()
            fe = fault_end.total_seconds()
            sev_color = '#e74c3c' if severity == 'CRITICAL' else '#f39c12'
            ax.axvspan(fs, fe, alpha=0.15, color=sev_color,
                       label=f'Fault Window ({duration:.1f}s)')
            # Add vertical lines at fault boundaries
            ax.axvline(fs, color=sev_color, linestyle='--',
                       alpha=0.6, linewidth=1)
            ax.axvline(fe, color=sev_color, linestyle='--',
                       alpha=0.6, linewidth=1)

        # Title & formatting
        sev_color_text = '#e74c3c' if severity == 'CRITICAL' else '#f39c12'
        title = (f'[{severity}] {rule_name.replace("_", " ").title()} '
                 f'— Confidence: {confidence:.0%}')
        if duration > 0:
            title += f'  (Duration: {duration:.1f}s)'
        ax.set_title(title, fontsize=13, fontweight='bold',
                     color=sev_color_text)
        ax.legend(loc='upper left', fontsize=9, framealpha=0.9)
        ax.grid(True, alpha=0.3)
        ax.set_ylabel('Value', fontsize=10)

    axes[-1].set_xlabel('Time (seconds)', fontsize=11)

    fig.suptitle('ArduPilot Log Diagnostic Report',
                 fontsize=16, fontweight='bold', y=1.01)
    plt.tight_layout()
    plt.savefig(output_path, dpi=200, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.close()
    logger.info("Diagnostic plot saved to %s", output_path)


def _plot_overview(df: pd.DataFrame, output_path: str):
    """Fallback: plot an overview of all signals when no faults are found."""
    fig, ax = plt.subplots(figsize=(14, 6))
    time_seconds = df.index.total_seconds()
    for i, col in enumerate(df.columns[:8]):
        ax.plot(time_seconds, df[col],
                label=col, color=COLORS[i % len(COLORS)], alpha=0.7)
    ax.set_title('Log Overview — No Faults Detected', fontsize=14,
                 fontweight='bold', color='#2ecc71')
    ax.set_xlabel('Time (seconds)')
    ax.legend(loc='upper right', fontsize=8)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output_path, dpi=200, bbox_inches='tight')
    plt.close()
    logger.info("Overview plot saved to %s", output_path)
