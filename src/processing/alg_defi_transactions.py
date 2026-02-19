from collections import defaultdict
from collections import deque
from processing.alg_cumulative_wealth_gain import CumulativeWealthGain

class DefiTransactions:
    def __init__(self, two_delta):
        self.two_delta = two_delta
        self.gain_total = 0
        self.previous_tx = deque()

    def run_on_block(self, block: dict) -> int:
        # remove old transactions
        current_time = block["timestamp"]
        cutoff_time = current_time - self.two_delta
        while self.previous_tx and self.previous_tx[0][0] < cutoff_time:
            self.gain_total = self.gain_total - self.previous_tx[0][1]
            self.previous_tx.popleft()

        # add new transaction
        grouped_tx = defaultdict(list)
        for tx in block['transactions']:
            if tx["usd_value"] is None:
                continue
            grouped_tx[tx["hash"]].append(tx)

        total_value = 0
        for hash, txs in grouped_tx.items():
            cumulative_wealth_gain = CumulativeWealthGain(24)
            total_value = total_value + cumulative_wealth_gain.run_on_block({
                "timestamp": block["timestamp"],
                "transactions": txs if txs is not None else [],
            })

        self.previous_tx.append((current_time, total_value))
        self.gain_total = self.gain_total + total_value
        return self.gain_total
