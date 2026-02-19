from collections import deque


class CumulativeWealthGain:
    def __init__(self, two_delta):
        self.two_delta = two_delta
        self.gain_total = 0
        self.vertex_map = {}
        self.previous_tx = deque()

    def calc_gain(self, v1, v2, val):
        u = self.vertex_map.get(v1, 0)
        v = self.vertex_map.get(v2, 0)

        delta_gain = (max(0, u + val) + max(0, v - val)) - (max(0, u) + max(0, v))

        self.vertex_map[v1] = u + val
        self.vertex_map[v2] = v - val
        return delta_gain

    def rollback_txs(self, block):
        for tx in block["transactions"]:
            self.calc_gain(tx["to"], tx["from"], tx["usd_value"])
        self.gain_total -= block["cached_gain"]


    def execute_txs(self, block):
        block_gain = 0
        for tx in block["transactions"]:
            g = self.calc_gain(tx["from"], tx["to"], tx["usd_value"])
            block_gain += g
        self.previous_tx.append({
            "timestamp": block["timestamp"],
            "transactions": block["transactions"],
            "cached_gain": block_gain,
        })
        self.gain_total += block_gain

    def run_on_block(self, block: dict) -> int:
        current_time = block["timestamp"]
        cutoff_time = current_time - self.two_delta

        while self.previous_tx and self.previous_tx[0]["timestamp"] < cutoff_time:
            self.rollback_txs(self.previous_tx.popleft())
        self.execute_txs(block)

        return self.gain_total