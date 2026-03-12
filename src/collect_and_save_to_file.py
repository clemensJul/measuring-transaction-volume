import asyncio
from pathlib import Path
import gc
import numpy
import yaml
import os
from tqdm import tqdm
from dotenv import load_dotenv
from typing_extensions import override

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
    3600: "one hour",
    86400: "one day",
    604800: "one week"
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
            ValueComparison(SpeedComparison(CumulativeWealthGain(n), n), n)
            for n in config["analysis"]["cumulative_wealth_gain"]
        ]
        transaction_counting = [
            ValueComparison(SpeedComparison(TransactionCounting(n), n), n)
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
            # collect
            blocks = await dc.get_blocks(batch_start, batch_end, False)

            # process
            for block in blocks:
                test_block = {
                    "timestamp": block[1].timestamp(),
                    "transactions": block[2] if block[2] is not None else [],
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
        path_to_csv = PROJECT_ROOT / "data" / "algorithm_data.csv"
        values = []

        import csv

        with open(path_to_csv, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter=";")

            writer.writerow(
                ["timestamp"]
                + [f"ttv_{tc.delta / 12}" for tc in transaction_counting]
                + [f"defi_{defi.delta / 12}" for defi in defi_transactions]
                + [f"asp_{wg.delta / 12}" for wg in cumulative_wealth_gain]
            )

            for i, ts in enumerate(timestamps):
                writer.writerow(
                    [ts]
                    + [tc.values[i] for tc in transaction_counting]
                    + [defi.values[i] for defi in defi_transactions]
                    + [wg.values[i] for wg in cumulative_wealth_gain]
                )


            



    finally:
        await dc.close()


if __name__ == "__main__":
    asyncio.run(main())