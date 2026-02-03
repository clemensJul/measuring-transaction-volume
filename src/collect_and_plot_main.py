import asyncio
from pathlib import Path
import gc
import numpy
import yaml
import os
from tqdm import tqdm
from dotenv import load_dotenv
from typing_extensions import override
from xxlimited_35 import Null

from collect.data_manager import DataCollector
from collect.cancellation_token import CancellationToken
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from processing.alg_cumulative_wealth_gain import CumulativeWealthGain
from processing.alg_transaction_counting import TransactionCounting
from analysis.speed_comparision import SpeedComparison
from analysis.value_comparision import ValueComparison
import signal
import datetime

THRESHOLD_D = 32 * 10 ** 18
PROJECT_ROOT = Path(__file__).resolve().parents[1]

def get_config() -> dict:
    load_dotenv(PROJECT_ROOT / ".env")
    path = PROJECT_ROOT / "config" / "config.yaml"
    with path.open("r") as f:
        config = yaml.safe_load(f)
    config["COIN_GECKO_API_KEY"] = os.getenv("COIN_GECKO_API_KEY")
    config["RCP_URL"] = os.getenv("RCP_URL")
    return config

async def main():
    cancellation_token = CancellationToken()
    config = get_config()
    dc = DataCollector(config=config)

    loop = asyncio.get_running_loop()

    def handle_interrupt():
        print("\nInterrupt received — finishing current batch and exiting…")
        cancellation_token.cancel()

    loop.add_signal_handler(signal.SIGINT, handle_interrupt)
    loop.add_signal_handler(signal.SIGTERM, handle_interrupt)

    await dc.open()

    try:

        cumulative_wealth_gain = [
            ValueComparison(SpeedComparison(CumulativeWealthGain(n),n),n)
            for n in config["analysis"]["cumulative_wealth_gain"]
        ]
        transaction_counting = [
            ValueComparison(SpeedComparison(TransactionCounting(n),n),n)
            for n in config["analysis"]["transaction_counting"]
        ]

        timestamps = []

        end = config["start_block"] + 700
        progress = tqdm(
                range(config["start_block"],config["end_block"] , config["batch_size"]),
                desc="Indexing blocks",
                unit="batch",
        )

        for batch_start in progress:
            batch_end = min(
                batch_start + config["batch_size"],
                config["end_block"],
            )
            #collect
            blocks = await dc.get_blocks(batch_start, batch_end, False)

            # process
            for block in blocks:
                test_block = {
                    "timestamp": block[1].timestamp(),
                    "transactions" : list(filter(lambda x: x["is_dex_swap"] == False, block[2]))
                }
                for wg in cumulative_wealth_gain:
                    wg.run_on_block(test_block)
                for tc in transaction_counting:
                    tc.run_on_block(test_block)
                timestamps.append(datetime.datetime.fromtimestamp(test_block["timestamp"]))

        # garbage collect
        for wg in cumulative_wealth_gain:
            wg.algorithm.algorithm.previous_tx = None
        for tc in transaction_counting:
            tc.algorithm.algorithm.previous_tx = None
        gc.collect()

        for wg, tc in zip(cumulative_wealth_gain, transaction_counting):
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            # wealth gain trace
            fig.add_trace(
                go.Scatter(
                    x=timestamps,
                    y=wg.values,
                    mode="lines",
                    name=f"Cumulative Wealth Gain: {wg.delta // 12} Slots",
                )
            )
            # transaction counting trace
            fig.add_trace(
                go.Scatter(
                    x=timestamps,
                    y=tc.values,
                    mode="lines",
                    name=f"Transaction Volume: {tc.delta // 12} Slots",
                )
            )
            wg_bu = numpy.array(wg.algorithm.time_build_up_window).mean()
            wg_sw = numpy.array(wg.algorithm.time_sliding_window).mean()
            tc_bu = numpy.array(tc.algorithm.time_build_up_window).mean()
            tc_sw = numpy.array(tc.algorithm.time_sliding_window).mean()

            stats_text = (
                f"<b>Average operation time</b><br>"
                f"Cumulative Wealth Gain build-up: {wg_bu.microseconds} μs<br>"
                f"Cumulative Wealth Gain sliding : {wg_sw.microseconds} μs<br>"
                f"Total volume build-up: {tc_bu.microseconds} μs<br>"
                f"Total volume sliding : {tc_sw.microseconds} μs"
            )

            fig.add_annotation(
                x=0.01, y=0.99,
                xref="paper", yref="paper",
                xanchor="left", yanchor="top",
                text=stats_text,
                showarrow=False,
                align="left",
                bordercolor="black",
                borderwidth=0.5,
                bgcolor="rgba(255,255,255,0.85)",
                font=dict(size=10),
            )

            # line when full window is reached
            x_line = timestamps[0] + datetime.timedelta(seconds=wg.delta)
            fig.add_vline(
                x = x_line,
                line_width=1.2,
                line_color="black",
            )

            differences = numpy.divide(
                wg.values,
                tc.values,
                out=numpy.zeros_like(wg.values, dtype=float),
                where=tc.values != 0
            )

            window = max(wg.delta // 12, 3600)
            kernel = numpy.ones(window) / window
            rolling_avg = numpy.convolve(differences, kernel, mode="valid")
            rolling_timestamps = timestamps[window - 1:]
            fig.add_trace(
                go.Scatter(
                    x=rolling_timestamps,
                    y=rolling_avg * 100,
                    mode="lines",
                    name=f"Rolling Average {window} Slots",
                    line=dict(dash="dot", width=2),
                ),
                secondary_y=True
            )
            max_val = numpy.max(tc.values)
            padding_factor = 1.3
            upper_limit = max_val * padding_factor

            fig.update_yaxes(title_text=f"Window volume in USD",
                             secondary_y=False,
                             range=[0,upper_limit]
                             )
            fig.update_yaxes(title_text=f" Cumulative Wealth Gain in % of Total Volume",
                             secondary_y=True,
                             range=[0,115]
                             )
            fig.update_layout(
                title=f"Transaction Volume vs Cumulative Wealth Gain with Δ window {wg.delta // 12} Slots",
                legend_title="Metrics",
                legend=dict(
                    orientation="h",
                    yanchor="top",
                    y=-0.2,
                    xanchor="center",
                    x=0.5
                ),
            )

            fig.update_xaxes(title_text="Date")
            path = PROJECT_ROOT / "data" / f"plot_delta_{wg.delta}.svg"
            fig.write_image(path, scale=1)

        # analyse
    finally:
        await dc.close()

if __name__ == "__main__":
    asyncio.run(main())