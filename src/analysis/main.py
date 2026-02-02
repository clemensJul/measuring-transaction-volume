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
from speed_comparision import SpeedComparison
from value_comparision import ValueComparison
import signal
import datetime

THRESHOLD_D = 32 * 10 ** 18
PROJECT_ROOT = Path(__file__).resolve().parents[2]

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
            blocks = await dc.get_blocks(batch_start, batch_end, True)

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

        fig = make_subplots(
            len(cumulative_wealth_gain),
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.04,
            specs=[[{"secondary_y": True}] for _ in cumulative_wealth_gain],
        )

        for i, (wg, tc) in enumerate(zip(cumulative_wealth_gain, transaction_counting), start=1):
            subplot_id = f"Δ={wg.delta}"



            # wealth gain trace
            fig.add_trace(
                go.Scatter(
                    x=timestamps,
                    y=wg.values,
                    mode="lines",
                    name=f"Cumulative Wealth Gain: {wg.delta} slots",
                    legendgroup=subplot_id,
                    showlegend=True,
                ),
                row = i,
                col = 1,
            )
            # transaction counting trace
            fig.add_trace(
                go.Scatter(
                    x=timestamps,
                    y=tc.values,
                    mode="lines",
                    name=f"Transaction Counting: {tc.delta} Slots",
                    legendgroup=subplot_id,
                    showlegend=True,
                ),
                row=i,
                col=1,
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
                col=1,
                row=i,
                legendgroup=subplot_id,
                showlegend=True,
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
                    legendgroup=subplot_id,
                    showlegend=True,
                ),
                row=i,
                col=1,
                secondary_y=True,
            )

            fig.update_yaxes(title_text=f"Transaction volume in USD",
                             col=1,
                             row=i,
                             secondary_y=False
                             )
            fig.update_yaxes(title_text=f"Improvement in % of total volume",
                             col=1,
                             row=i,
                             secondary_y=True,
                             range=[0,101]
                             )

        fig.update_layout(
            title="Dynamic Transaction Data Analysis",
            height=400 * len(cumulative_wealth_gain),
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

        fig.update_xaxes(title_text="Date")
        fig.show()

        # analyse
    finally:
        await dc.close()

if __name__ == "__main__":
    asyncio.run(main())