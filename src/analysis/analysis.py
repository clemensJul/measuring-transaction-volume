import json
import sys
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go


def main(file_name):
    THRESHOLD_D = 32 * 10**18
    MA_WINDOW = 1000
    data = []

    try:
        with open(file_name, 'r') as f:
            lines = [line.strip() for line in f if line.strip()]
            if not lines:
                print("File is empty.")
                return

            keys = json.loads(lines[0]).keys()

            for line in lines:
                b = json.loads(line)
                entry = {k: b[k] for k in keys}
                entry["ts"] = pd.to_datetime(b["timestamp"], unit="s")
                data.append(entry)

    except Exception as eps:
        print(f"Error loading file: {eps}")
        return

    df = pd.DataFrame(data).sort_values("ts")

    suffixes = [
        k[2:]
        for k in keys
        if k.startswith("tc") and f"cwc{k[2:]}" in keys
    ]

    if not suffixes:
        print("No matching tc/cwc pairs found.")
        return

    fig = make_subplots(
        rows=len(suffixes),
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        specs=[[{"secondary_y": True}] for _ in suffixes],
        subplot_titles=[f"2Δ time window: {s}" for s in suffixes],
    )
    for i, suffix in enumerate(suffixes, start=1):
        tc_col = f"tc{suffix}"
        cwc_col = f"cwc{suffix}"

        eps = df[tc_col].median() * 0.01
        diff = df[cwc_col] / df[tc_col].clip(lower=eps) * 100
        diff_ma = diff.rolling(MA_WINDOW).mean()

        fig.add_trace(
            go.Scatter(
                x=df["ts"],
                y=df[tc_col],
                name=f"Transaction Volume {suffix}",
                line=dict(width=2, dash="dash"),
            ),
            row=i,
            col=1,
            secondary_y=False,
        )

        fig.add_trace(
            go.Scatter(
                x=df["ts"],
                y=df[cwc_col],
                name=f"Cumulative Wealth Gain {suffix}",
                line=dict(width=2, dash="dash"),
            ),
            row=i,
            col=1,
            secondary_y=False,
        )

        fig.add_trace(
            go.Scatter(
                x=df["ts"],
                y=diff,
                name=f"Cumulative Wealth Gain / Transaction Volume in %{suffix}",
                line=dict(width=1, dash="dot"),
            ),
            row=i,
            col=1,
            secondary_y=True,
        )

        # Secondary axis: Δ moving average
        fig.add_trace(
            go.Scatter(
                x=df["ts"],
                y=diff_ma,
                name=f"Δ% MA({MA_WINDOW}) {suffix}",
                line=dict(width=3),
            ),
            row=i,
            col=1,
            secondary_y=True,
        )

        fig.add_hline(
            y=THRESHOLD_D,
            line_width=1.2,
            line_color="black",
            row=i,
            col=1,
        )

        fig.update_yaxes(
            title_text=f"Gain / Volume {suffix}",
            row=i,
            col=1,
            secondary_y=False,
        )
        fig.update_yaxes(
            title_text="Δ% Difference",
            row=i,
            col=1,
            secondary_y=True,
        )

    fig.update_layout(
        title="Dynamic Transaction Data Analysis",
        height=400 * len(suffixes),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        margin=dict(t=80, b=40),
    )

    fig.update_xaxes(title_text="Time")
    fig.show()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1])
