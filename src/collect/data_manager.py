from decimal import Decimal
from pathlib import Path
import asyncio
from datetime import datetime, timezone, timedelta
from collect.db_connection import open_db
from coingecko_sdk import Coingecko
from collect.rpc_client import RPCClient
import pandas as pd
from eth_utils import keccak, event_signature_to_log_topic
import numpy as np

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "main.duckdb"
TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
BIGINT_MAX = 2**63 - 1

ASSET_PLATFORM = 'ethereum'
ETH_NAME ="ETH"

class DataCollector:
    def __init__(
        self,
        config: dict,
    ):
        self.config = config
        self.rpc_client = RPCClient(
            rpc_url=config["RCP_URL"]
            )
        self.coin_gecko = Coingecko(
            demo_api_key=config["COIN_GECKO_API_KEY"],
            environment="demo"
            )
        self.dex_swap = [
            "0x"+event_signature_to_log_topic(x).hex()
            for x in config["dex_events"]
        ]
        self.db = open_db()
        self.price_data = {}
        self.active_coins = list(filter(lambda x: x["active"] == True, self.config["token"]))
        self.active_coins_dict = {
            coin["address"].lower(): coin
            for coin in self.active_coins
        }
        self.num_active_coins = len(self.active_coins)
        self.current_prices = {}
        self.current_date = datetime.min

    async def open(self):
        await self.rpc_client.open()

    async def close(self):
        await self.rpc_client.close()
        self.db.close()

    async def make_blocks_in_db_available(self, start_block, end_block):
       # check if coins are in database
        active_coins = [x["name"] for x in self.active_coins]
        current_blocks = list(range(start_block, end_block))
        missing = await self.get_missing(current_blocks, active_coins)

       # add missing coins to database
        if len(missing) != 0:
            await self.fetch_and_add_missing_to_db(missing)

    async def get_blocks(self, start_block, end_block, with_dex = False):
        await self.make_blocks_in_db_available(start_block, end_block)
        if with_dex:
            return self.db.execute(
                """
                SELECT
                  b.number,
                  b.timestamp,
                   coalesce(
                    list(
                        struct_pack(
                          hash        := t.hash,    
                          coin        := t.coin,
                          "from"      := t.from_addr,
                          "to"        := t.to_addr,
                          amount      := t.amount,
                          usd_value   := t.usd_value,
                          is_dex_swap := t.is_dex_swap              
                        )
                    ) FILTER (WHERE t.hash IS NOT NULL),
                    []                   
                  ) AS transactions
                FROM blocks b
                LEFT JOIN transactions t
                  ON t.block_number = b.number
                WHERE b.number BETWEEN ? AND ?
                GROUP BY b.number, b.timestamp
                ORDER BY b.timestamp;
                """,
                (start_block, end_block - 1)
            ).fetchall()
        return self.db.execute(
            """
            SELECT
              b.number,
              b.timestamp,
              coalesce(
                list(
                  struct_pack(
                    hash        := t.hash,    
                    coin        := t.coin,
                    "from"      := t.from_addr,
                    "to"        := t.to_addr,
                    amount      := t.amount,
                    usd_value   := t.usd_value,
                    is_dex_swap := t.is_dex_swap
                  )
                ) FILTER (WHERE t.hash IS NOT NULL),
                []
              ) AS transactions
            FROM blocks b
            LEFT JOIN transactions t
              ON t.block_number = b.number
             AND coalesce(t.is_dex_swap, false) = false
            WHERE b.number BETWEEN ? AND ?
            GROUP BY b.number, b.timestamp
            ORDER BY b.timestamp;
            """,
            (start_block, end_block - 1),
        ).fetchall()

    async def get_missing(self,current_blocks, active_coins):
        return self.db.execute(
            """
            WITH expected AS (
                SELECT
                    b.block_number,
                    c.coin
                FROM unnest(?) AS b(block_number)
                CROSS JOIN unnest(?) AS c(coin)
            ),
            missing AS (
                SELECT
                    e.block_number,
                    e.coin
                FROM expected e
                LEFT JOIN block_ingestions bi
                  ON bi.block_number = e.block_number
                 AND bi.coin = e.coin
                WHERE bi.block_number IS NULL
            )
            SELECT
                block_number,
                list(coin ORDER BY coin) AS missing_coins
            FROM missing
            GROUP BY block_number
            ORDER BY block_number;
            """,
            (current_blocks, active_coins)
        ).fetchall()
    async def get_usd_value(self, coin, datetime_of_block, amount):
        date = datetime_of_block.date()
        if self.current_date == date:
            price = self.current_prices.get(coin["name"])
            if price is not None:
                return np.float64(amount * price / (10 ** coin["decimals"]))

        row = self.db.execute(
            """
            SELECT *
            FROM coin_values
            WHERE coin = ? AND date = ?
            """,
            (coin["name"], date.isoformat()),
        ).fetchone()

        if row is None:
            to_date = date + timedelta(days=91)
            if coin["name"] == ETH_NAME:
                resp = self.coin_gecko.coins.market_chart.get_range(
                    id=ASSET_PLATFORM,
                    vs_currency="usd",
                    from_=date.isoformat(),
                    to=to_date.isoformat(),
                )
            else:
                resp = self.coin_gecko.coins.contract.market_chart.get_range(
                    id=ASSET_PLATFORM,
                    contract_address=coin["address"],
                    vs_currency="usd",
                    from_=date.isoformat(),
                    to=to_date.isoformat(),
                )
            rows_to_insert = [
                (
                    coin["name"],
                    datetime_of_block.fromtimestamp(price[0] / 1000).date().isoformat(),
                    price[1]
                )
                for price in resp.prices
            ]

            if rows_to_insert:
                self.db.executemany(
                    """
                    INSERT INTO coin_values (coin, date, usd_value)
                    VALUES (?, ?, ?)
                    ON CONFLICT (coin, date) DO UPDATE SET usd_value = excluded.usd_value
                    """,
                    rows_to_insert,
                )
                row = rows_to_insert[0]

        self.current_date = date
        self.current_prices[coin["name"]] = row[2]
        return np.float64(amount * row[2] / (10 ** coin["decimals"]))

    async def fetch_and_add_missing_to_db(self, missing):
        missing_dict = {
            m[0]: m[1]
            for m in missing
        }
        tasks = [self.rpc_client.process_block(b[0]) for b in missing]
        gathered_blocks = await asyncio.gather(*tasks)

        blocks_in_batch = []
        digests_in_batch = []
        transactions_in_batch = []
        for i, block in enumerate(gathered_blocks):
            number = int(block["number"], 16)
            datetime_block = datetime.fromtimestamp(int(block["timestamp"], 16))

            transactions_in_block = []
            for transaction in zip(block["transactions"], block["receipts"]):
                was_dex_swap = False
                coin_movements_in_transaction = []
                coin = self.active_coins_dict.get(ASSET_PLATFORM, None)
                if coin is not None and coin["name"] in missing_dict[number]:
                    tx = transaction[0]
                    if (tx["from"] is not None
                            and tx["to"] is not None
                                and tx["from"] != ZERO_ADDRESS
                                    and tx["to"] != ZERO_ADDRESS ):
                        coin_movements_in_transaction.append(
                            [
                                tx["hash"],
                                -1,
                                number,
                                coin["name"],
                                tx["from"].lower(),
                                tx["to"].lower(),
                                min(int(tx["value"], 16),BIGINT_MAX), 
                                await self.get_usd_value(coin, datetime_block, int(tx["value"], 16), ),
                                False
                            ]
                        )
                # erc20
                for log in transaction[1]["logs"]:
                    topics = log.get("topics", [])

                    if not topics:
                        continue

                    event = topics[0].lower()

                    if event in self.dex_swap:
                        was_dex_swap = True
                        break

                    if event == TRANSFER_TOPIC:
                        token_addr = log["address"].lower()
                        coin = self.active_coins_dict.get(token_addr)
                        if coin is None:
                            continue  # not a tracked token

                        if coin["name"] not in missing_dict[number]:
                            continue  # coin has been tracked before

                        from_addr = "0x" + topics[1][-40:].lower()
                        to_addr   = "0x" + topics[2][-40:].lower()

                        if from_addr != ZERO_ADDRESS and to_addr != ZERO_ADDRESS:
                            coin_movements_in_transaction.append(
                                [
                                    log["transactionHash"],
                                    int(log["logIndex"], 16),
                                    number,
                                    coin["name"],
                                    from_addr,
                                    to_addr,
                                    min(int(log["data"], 16),BIGINT_MAX),
                                    await self.get_usd_value(coin, datetime_block, int(log["data"], 16)),
                                    False,
                                ]
                            )
                            if coin_movements_in_transaction[-1][6] > BIGINT_MAX:
                                print("value too big")
                if  was_dex_swap:
                    for tx in coin_movements_in_transaction:
                        tx[8] = True
                transactions_in_block.extend(coin_movements_in_transaction)
            digestions = [
                (number, val)
                for val in missing[i][1]
            ]
            if len(digestions) == self.num_active_coins:
                blocks_in_batch.append((number, datetime_block.isoformat()))
            digests_in_batch.extend(digestions)
            if len(transactions_in_block) > 0:
                transactions_in_batch.extend(transactions_in_block)

        blocks_df = pd.DataFrame(blocks_in_batch, columns=["number", "timestamp"])
        digests_df = pd.DataFrame(digests_in_batch, columns=["block_number", "coin"])
        tx_df = pd.DataFrame(transactions_in_batch, columns=["hash", "log_number","block_number", "coin", "from_addr", "to_addr", "amount", "usd_value", "is_dex_swap"])
        try:
            self.db.execute("BEGIN TRANSACTION")
            if not blocks_df.empty:
                self.db.execute("INSERT INTO blocks SELECT number, timestamp FROM blocks_df")
            if not digests_df.empty:
                self.db.execute("INSERT INTO block_ingestions SELECT block_number, coin FROM digests_df")
            if not tx_df.empty:
                self.db.execute("""                                                  
                        INSERT INTO transactions (hash, log_number, block_number, coin, from_addr, to_addr, amount, usd_value , is_dex_swap)
                        SELECT hash, log_number, block_number, coin, from_addr, to_addr, amount, usd_value, is_dex_swap FROM tx_df
                    """)
            self.db.execute("COMMIT")
        except Exception as e:
            self.db.execute("ROLLBACK")
            print(f"ROLLBACK batch error: {e}")