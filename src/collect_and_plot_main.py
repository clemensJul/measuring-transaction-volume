import asyncio
from pathlib import Path

import numpy
import yaml
import os
from tqdm import tqdm
from dotenv import load_dotenv
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

        end = config["start_block"] + (config["end_block"] - config["start_block"]) // 3
        progress = tqdm(
                range(config["start_block"],end , config["batch_size"]),
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

        for wg, tc in zip(cumulative_wealth_gain, transaction_counting):
            fig = make_subplots(specs=[[{"secondary_y": True}]]
                                

                                )
            # wealth gain trace
            fig.add_trace(
                go.Scatter(
                    x=timestamps,
                    y=wg.values,
                    mode="lines",
                    name=f"Cumulative Wealth Gain: {wg.delta} slots",
                )
            )
            # transaction counting trace
            fig.add_trace(
                go.Scatter(
                    x=timestamps,
                    y=tc.values,
                    mode="lines",
                    name=f"Transaction Volume: {tc.delta} Slots",
                )
            )
            # traces[0].algorithm.time_build_up_window <- get average and display it
            # traces[0].algorithm.time_sliding_window <- get average and display it
            # traces[1].algorithm.time_build_up_window <- get average and display it
            # traces[1].algorithm.time_sliding_window <- get average and display it

            # line when full window is reached
            x_line = timestamps[0] + datetime.timedelta(seconds=12 * wg.delta)
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

            window = max(wg.delta, 1000)
            kernel = numpy.ones(window) / window
            rolling_avg = numpy.convolve(differences, kernel, mode="valid")
            rolling_timestamps = timestamps[window - 1:]
            fig.add_trace(
                go.Scatter(
                    x=rolling_timestamps,
                    y=rolling_avg * 100,
                    mode="lines",
                    name=f"Rolling Avg (Δ={wg.delta})",
                    line=dict(dash="dot", width=2),
                ),
                secondary_y=True
            )

            fig.update_yaxes(title_text=f"Transaction volume in USD",
                             secondary_y=False
                             )
            fig.update_yaxes(title_text=f" Cumulative Wealth Gain in % of Total Volume",
                             secondary_y=True,
                             range=[0,101]
                             )
            fig.update_layout(
                title=f"Rolling Window Δ={wg.delta}",
                legend_title="Metrics",
                # Legend configuration
                legend=dict(
                    orientation="h",  # Horizontal orientation
                    yanchor="top",  # Anchor the top of the legend box...
                    y=-0.2,  # ...at negative 20% of the plot height (below x-axis)
                    xanchor="center",  # Center horizontally
                    x=0.5  # At the middle of the plot
                )
            )

            fig.update_xaxes(title_text="Date")
            path = PROJECT_ROOT / "data" / f"plot_delta_{wg.delta}.svg"
            fig.write_image(path, scale=1)

        # analyse
    finally:
        await dc.close()

if __name__ == "__main__":
    asyncio.run(main())