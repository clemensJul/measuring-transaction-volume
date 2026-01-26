from pathlib import Path
import asyncio
from datetime import datetime, timezone, timedelta
from collect.db_connection import open_db
from coingecko_sdk import Coingecko
from collect.rpc_client import RPCClient


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "main.duckdb"
TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
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
        self.db = open_db()
        self.price_data = {}
        self.active_coins = list(filter(lambda x: x["active"] == True, self.config["token"]))
        self.active_coins_dict = {
            coin["address"].lower(): coin
            for coin in self.active_coins
        }

    async def open(self):
        await self.rpc_client.open()

    async def close(self):
        await self.rpc_client.close()
        self.db.close()


    async def get_blocks(self, start_block, end_block, callback):
        # check if coins are in database
        t0 = datetime.now()
        active_coins = [x["name"] for x in self.active_coins]
        current_blocks = list(range(start_block, end_block + 1))
        missing = await self.get_missing(current_blocks, active_coins)
        t1 = datetime.now()

        # add missing coins to database
        if len(missing) != 0:
            await self.fetch_and_add_missing_to_db(missing)

        t2 = datetime.now()
        blocks =  self.db.execute(
            """
            SELECT
              b.number,
              b.timestamp,
              list(
                struct_pack(
                  coin    := t.coin,
                  "from"  := t."from_addr",
                  "to"    := t."to_addr",
                  amount  := t.amount,
                  value   := t.usd_value
                )
              ) AS transactions
            FROM blocks b
            LEFT JOIN transactions t
              ON t.block_number = b.number
            WHERE b.number = 100
            GROUP BY b.number, b.timestamp;
            """
        ).fetchall()
        t3 = datetime.now()
        callback(t0,t1,t2,t3)
        return blocks

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
    async def get_usd_value(self, coin ,timestamp, amount):
        date = datetime.fromisoformat(timestamp).date()

        row = self.db.execute(
            """
            SELECT usd_value
            FROM coin_values
            WHERE coin = ? AND date = ?
            """,
            (coin["name"], date.isoformat()),
        ).fetchone()

        if row is not None:
            return float(amount) * float(row[0]) / (10 ** coin["decimals"])

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
                datetime.fromtimestamp(price[0] / 1000, tz=timezone.utc).date().isoformat(),
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

        return float(amount) * rows_to_insert[0][2] / (10 ** coin["decimals"])

    async def fetch_and_add_missing_to_db(self, missing):
        # missing is a list of tuples (block_number, coin)
        missing_dict = {
            m[0]: m[1]
            for m in missing
        }
        tasks = [self.rpc_client.process_block(b[0]) for b in missing]
        gathered_blocks = await asyncio.gather(*tasks)

        for block in gathered_blocks:
            self.db.execute("START TRANSACTION")
            try:
                #insert block
                number = int(block["number"],16)
                timestamp = datetime.fromtimestamp(int(block["timestamp"],16), tz=timezone.utc).isoformat()
                self.db.execute(
                    """
                    INSERT INTO blocks (number, timestamp)
                    VALUES (?, ?)
                    ON CONFLICT (number) DO NOTHING
                    """,
                    (number, timestamp),
                )
                # insert ethereum transactions
                coin = self.active_coins_dict.get(ASSET_PLATFORM, None)
                if  coin is not None and coin["name"] in missing_dict[number]:
                    sanitized_transactions = [
                        (
                            tx["hash"],
                            -1,
                            number,
                            coin["name"],
                            tx["from"],
                            tx["to"],
                            int(tx["value"],16),
                            await self.get_usd_value(coin,timestamp, int(tx["value"],16),)
                        )
                        for tx in filter(lambda x: int(x["value"],16) != 0, block["transactions"])
                    ]
                    if len(sanitized_transactions) > 0:
                        self.db.executemany(
                            """
                            INSERT INTO transactions
                              (hash,log_number, block_number, coin, from_addr, to_addr, amount, usd_value)
                            VALUES (?,?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT (hash, log_number) DO NOTHING
                            """,
                            sanitized_transactions,
                        )
                    self.db.execute(
                        """
                        INSERT INTO block_ingestions 
                          (block_number, coin)
                        VALUES (?, ?)
                        ON CONFLICT (block_number, coin) DO NOTHING  
                        """,
                        (number, ETH_NAME),
                    )

                # insert erc-20 transactions
                erc20_transactions = []

                for receipt in block["receipts"]:
                    for log in receipt["logs"]:
                        topics = log.get("topics", [])
                        if not topics or topics[0].lower() != TRANSFER_TOPIC:
                            continue

                        token_addr = log["address"].lower()
                        coin = self.active_coins_dict.get(token_addr)
                        if coin is None:
                            continue  # not a tracked token

                        if coin["name"] not in missing_dict[number]:
                            continue # coin has been tracked before

                        erc20_transactions.append(
                            (
                                log["transactionHash"],
                                int(log["logIndex"],16),
                                number,
                                coin["name"],
                                "0x" + topics[1][-40:].lower(),
                                "0x" + topics[2][-40:].lower(),
                                int(log["data"], 16),
                                await self.get_usd_value(coin, timestamp, int(log["data"], 16)),
                            )
                        )
                if len(erc20_transactions) > 0:
                    self.db.executemany(
                        """
                        INSERT INTO transactions
                          (hash, log_number, block_number, coin, from_addr, to_addr, amount, usd_value)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (hash, log_number) DO NOTHING
                        """,
                        erc20_transactions,
                    )
                digestions = [
                    (number, val["name"])
                    for val in self.active_coins_dict.values()
                ]
                self.db.executemany(
                    """
                    INSERT INTO block_ingestions 
                      (block_number, coin)
                    VALUES (?, ?)
                    
                    ON CONFLICT (block_number, coin) DO NOTHING  
                    """,
                    digestions
                )
                self.db.execute("COMMIT")
            except Exception as e:
                self.db.execute("ROLLBACK")
                print(f"ROLLBACK block {number}: {e}")










        
