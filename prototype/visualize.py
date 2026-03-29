"""
visualize.py — Interactive Diagnostic Visualization (Plotly)

Generates an interactive HTML report using Plotly instead of static PNGs.
On a 45-minute log, users can pan, zoom, and hover over data points —
mimicking MAVExplorer's utility right in the browser.

Features:
  - Stacked subplots per triggered rule
  - Exact fault-window shading via fault_start/fault_end
  - MSG/ERR event annotations on the timeline
  - Hover tooltips with exact timestamps and values
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)

# Colors for signal traces
COLORS = [
    '#e74c3c', '#3498db', '#2ecc71', '#f39c12',
    '#9b59b6', '#1abc9c', '#e67e22', '#34495e',
]

SEVERITY_COLORS = {
    'CRITICAL': 'rgba(231, 76, 60, 0.15)',
    'WARNING': 'rgba(243, 156, 18, 0.15)',
    'INFO': 'rgba(46, 204, 113, 0.15)',
}

SEVERITY_BORDER = {
    'CRITICAL': '#e74c3c',
    'WARNING': '#f39c12',
    'INFO': '#2ecc71',
}


def generate_diagnostic_plot(df: pd.DataFrame,
                              findings: list[dict],
                              events: list[dict] = None,
                              output_path: str = 'diagnostic_report.html'):
    """
    Generate an interactive Plotly HTML diagnostic report.

    Args:
        df: Raw resampled telemetry DataFrame from ingestion.
        findings: List of triggered rule findings from the rule engine.
        events: List of MSG/ERR event dicts from LogReader.
        output_path: Path to save the output HTML file.
    """
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        raise ImportError("plotly is required. Install with: pip install plotly")

    events = events or []
    faults = [f for f in findings if f.get('status') == 'FAULT_DETECTED']

    if not faults:
        logger.info("No faults to visualize.")
        _plot_overview(df, events, output_path)
        return

    n_panels = len(faults)
    subplot_titles = []
    for fault in faults:
        sev = fault.get('severity', 'INFO')
        name = fault.get('rule_name', '').replace('_', ' ').title()
        conf = fault.get('confidence', 0)
        dur = fault.get('fault_duration_s', 0)
        mode = fault.get('flight_mode', '')
        subtitle = (f"[{sev}] {name} — Confidence: {conf:.0%} "
                    f"(Duration: {dur:.1f}s, Mode: {mode})")
        subplot_titles.append(subtitle)

    fig = make_subplots(
        rows=n_panels, cols=1,
        shared_xaxes=True,
        subplot_titles=subplot_titles,
        vertical_spacing=0.08,
    )

    time_seconds = df.index.total_seconds()

    for panel_idx, fault in enumerate(faults, 1):
        signals = fault.get('plot_signals', [])
        severity = fault.get('severity', 'WARNING')
        fault_start = fault.get('fault_start')
        fault_end = fault.get('fault_end')

        # Plot each signal
        for i, sig in enumerate(signals):
            if sig not in df.columns:
                continue
            color = COLORS[i % len(COLORS)]
            fig.add_trace(
                go.Scatter(
                    x=time_seconds,
                    y=df[sig],
                    mode='lines',
                    name=sig,
                    line=dict(color=color, width=1.5),
                    legendgroup=f'panel_{panel_idx}',
                    showlegend=True,
                    hovertemplate=(
                        f'<b>{sig}</b><br>'
                        'Time: %{x:.2f}s<br>'
                        'Value: %{y:.2f}<br>'
                        '<extra></extra>'
                    ),
                ),
                row=panel_idx, col=1,
            )

        # Shade exact fault window
        if fault_start is not None and fault_end is not None:
            fs = fault_start.total_seconds()
            fe = fault_end.total_seconds()
            fill_color = SEVERITY_COLORS.get(severity, 'rgba(200,200,200,0.15)')
            border_color = SEVERITY_BORDER.get(severity, '#999')

            fig.add_vrect(
                x0=fs, x1=fe,
                fillcolor=fill_color,
                line=dict(color=border_color, width=1, dash='dash'),
                annotation_text=f"Fault: {fs:.1f}s–{fe:.1f}s",
                annotation_position="top left",
                annotation_font_size=10,
                row=panel_idx, col=1,
            )

        # Add MSG/ERR event markers within the fault window (±5s)
        fault_events = fault.get('events_in_window', [])
        if fault_events:
            evt_times = [e['time_s'] for e in fault_events]
            evt_texts = [e['text'] for e in fault_events]
            # Find a y-value to place markers
            y_vals = []
            for sig in signals:
                if sig in df.columns:
                    y_vals.append(df[sig].max())
            y_marker = max(y_vals) * 1.05 if y_vals else 1

            fig.add_trace(
                go.Scatter(
                    x=evt_times,
                    y=[y_marker] * len(evt_times),
                    mode='markers+text',
                    marker=dict(
                        symbol='triangle-down',
                        size=10,
                        color='#e74c3c',
                    ),
                    text=[t[:40] for t in evt_texts],
                    textposition='top center',
                    textfont=dict(size=8, color='#555'),
                    name='Events',
                    legendgroup=f'panel_{panel_idx}',
                    showlegend=False,
                    hovertemplate=(
                        '<b>Event</b><br>'
                        'Time: %{x:.2f}s<br>'
                        '%{text}<br>'
                        '<extra></extra>'
                    ),
                ),
                row=panel_idx, col=1,
            )

        fig.update_yaxes(title_text='Value', row=panel_idx, col=1)

    # Final layout
    fig.update_xaxes(title_text='Time (seconds)', row=n_panels, col=1)
    fig.update_layout(
        title=dict(
            text='ArduPilot Log Diagnostic Report',
            font=dict(size=20),
        ),
        height=400 * n_panels + 100,
        template='plotly_white',
        hovermode='x unified',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='center',
            x=0.5,
        ),
    )

    fig.write_html(output_path, include_plotlyjs=True)
    logger.info("Interactive diagnostic report saved to %s", output_path)


def _plot_overview(df: pd.DataFrame, events: list[dict],
                   output_path: str):
    """Fallback: interactive overview of the first 8 signals."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        raise ImportError("plotly is required.")

    fig = go.Figure()
    time_seconds = df.index.total_seconds()

    for i, col in enumerate(df.columns[:8]):
        if col.startswith('__'):
            continue
        fig.add_trace(go.Scatter(
            x=time_seconds, y=df[col],
            mode='lines', name=col,
            line=dict(color=COLORS[i % len(COLORS)], width=1.2),
        ))

    fig.update_layout(
        title='Log Overview — No Faults Detected',
        xaxis_title='Time (seconds)',
        yaxis_title='Value',
        template='plotly_white',
        hovermode='x unified',
    )

    fig.write_html(output_path, include_plotlyjs=True)
    logger.info("Overview report saved to %s", output_path)
