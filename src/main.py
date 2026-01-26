import asyncio
from pathlib import Path
import yaml
import os
from tqdm import tqdm
from dotenv import load_dotenv
from collect.data_manager import DataCollector
from collect.cancellation_token import CancellationToken
import signal

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
        progress = tqdm(
                range(config["start_block"], config["end_block"] + 1, config["batch_size"]),
                desc="Indexing blocks",
                unit="batch",
        )
        def callback(t0,t1,t2,t3):
            progress.set_postfix_str(
                f"gather={t1 - t0}s fetch={t2 - t1}s blocks={t3 - t2}s",
            )

        for batch_start in progress:
            if cancellation_token.is_canceled():
                break

            batch_end = min(
                batch_start + config["batch_size"],
                config["end_block"],
            )
            
            await dc.get_blocks(batch_start, batch_end, callback)
    finally:
        await dc.close()

if __name__ == "__main__":
    asyncio.run(main())