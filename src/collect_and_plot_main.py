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
from processing.alg_defi_transactions import DefiTransactions
from analysis.speed_comparision import SpeedComparison
from analysis.value_comparision import ValueComparison
import signal
import datetime

THRESHOLD_D = 32 * 10 ** 18
PROJECT_ROOT = Path(__file__).resolve().parents[1]
seconds_to_string = {
    3600 : "one hour",
    86400 : "one day",
    604800 : "one week"
}

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

        # analysis
        cumulative_wealth_gain = [
            ValueComparison(SpeedComparison(CumulativeWealthGain(n),n),n)
            for n in config["analysis"]["cumulative_wealth_gain"]
        ]
        transaction_counting = [
            ValueComparison(SpeedComparison(TransactionCounting(n),n),n)
            for n in config["analysis"]["transaction_counting"]
        ]
        defi_transactions = [
            ValueComparison(SpeedComparison(DefiTransactions(n), n), n)
            for n in config["analysis"]["defi_transactions"]
        ]

        timestamps = []

        end = config["start_block"] + 700
        progress = tqdm(
                range(config["start_block"],
                      config["end_block"],
                      config["batch_size"]),
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
                    "transactions" : block[2] if block[2] is not None else [],
                }
                for dt in defi_transactions:
                    dt.run_on_block(test_block)
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


        # print to .svg
        for wg, tc, dt in zip(cumulative_wealth_gain, transaction_counting, defi_transactions):
            n_of_slots = wg.delta // 12
            string = seconds_to_string[wg.delta] if wg.delta in seconds_to_string else f"{n_of_slots} Slots"
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            # transaction counting trace
            fig.add_trace(
                go.Scatter(
                    x=timestamps[n_of_slots:],
                    y=tc.values[n_of_slots:],
                    mode="lines",
                    name=f"Transaction Volume: {string}",
                )
            )
            # wealth gain trace
            fig.add_trace(
                go.Scatter(
                    x=timestamps[n_of_slots:],
                    y=wg.values[n_of_slots:],
                    mode="lines",
                    name=f"Cumulative Wealth Gain: {string}",
                )
            )
            # defi
            fig.add_trace(
                go.Scatter(
                    x=timestamps[n_of_slots:],
                    y=dt.values[n_of_slots:],
                    mode="lines",
                    name=f"DeFi Transaction Volume: {string}",
                )
            )

            wg_bu = numpy.array(wg.algorithm.time_build_up_window).mean()
            wg_sw = numpy.array(wg.algorithm.time_sliding_window).mean()
            tc_bu = numpy.array(tc.algorithm.time_build_up_window).mean()
            tc_sw = numpy.array(tc.algorithm.time_sliding_window).mean()
            df_bu = numpy.array(dt.algorithm.time_build_up_window).mean()
            df_sw = numpy.array(dt.algorithm.time_sliding_window).mean()

            print(f"""
                    Average operation time of {n_of_slots}
                    Cumulative Wealth Gain build-up: {wg_bu.total_seconds() * 1000} ms
                    Total volume build-up: {tc_bu.total_seconds() * 1000} ms
                    DeFi Transaction Volume build-up: {df_bu.total_seconds() * 1000} ms
                    
                    Cumulative Wealth Gain sliding : {wg_sw.total_seconds() * 1000} ms
                    Total volume sliding : {tc_sw.total_seconds() * 1000} ms
                    DeFi Transaction Volume sliding : {df_sw.total_seconds() * 1000} ms
                  """)

            wg_arr = numpy.asarray(wg.values, dtype=float)
            tc_arr = numpy.asarray(tc.values, dtype=float)
            dt_arr = numpy.asarray(dt.values, dtype=float)

            window = max(wg.delta // 12, 3600)
            kernel = numpy.ones(window, dtype=float)

            wg_sum = numpy.convolve(wg_arr, kernel, mode="valid")
            tc_sum = numpy.convolve(tc_arr, kernel, mode="valid")
            dt_sum = numpy.convolve(dt_arr, kernel, mode="valid")

            rolling_pct_wg = numpy.divide(
                wg_sum, tc_sum,
                out=numpy.full_like(wg_sum, numpy.nan),
                where=tc_sum != 0
            ) * 100.0
            rolling_pct_dt = numpy.divide(
                dt_sum, tc_sum,
                out=numpy.full_like(dt_sum, numpy.nan),
                where=tc_sum != 0
            ) * 100.0
            cut_timestamps = timestamps[window - 1:]
            # wealth gain
            fig.add_trace(
                go.Scatter(
                    x=cut_timestamps,
                    y=rolling_pct_wg,
                    mode="lines",
                    name=f"CWG rolling Average in % of total volume: {'one hour' if n_of_slots < 3600 else string}",
                    line=dict(dash="dot", width=2),
                ),
                secondary_y=True
            )

            fig.add_trace(
                go.Scatter(
                    x=cut_timestamps,
                    y=rolling_pct_dt,
                    mode="lines",
                    name=f"DeFi rolling Average in % of total volume: {'one hour' if n_of_slots < 3600 else string}",
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
            fig.update_yaxes(title_text=f"Computed volume % of Transaction Volume",
                             secondary_y=True,
                             range=[0,115]
                             )
            fig.update_layout(
                title=f"Algorithm comparison in Δ window {string}",
                legend_title="Metrics",
                legend=dict(
                    orientation="h",
                    yanchor="top",
                    y=-0.2,
                    xanchor="center",
                    x=0.5
                ),
            )

            fig.update_xaxes(title_text="Date"
                             )
            path = PROJECT_ROOT / "data" / f"plot_delta_{n_of_slots}.svg"
            fig.write_image(path, scale=1)
    finally:
        await dc.close()

if __name__ == "__main__":
    asyncio.run(main())