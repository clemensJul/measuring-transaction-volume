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

    def update_map_only(self, v1, v2, val):
        u = self.vertex_map.get(v1, 0)
        v = self.vertex_map.get(v2, 0)
        self.vertex_map[v1] = u + val
        self.vertex_map[v2] = v - val

    def rollback_map(self, txs):
        for tx in txs:
            self.update_map_only(tx["to"], tx["from"], tx["usd_value"])

    def execute_txs(self, txs: list) -> int:
        block_gain = 0
        for tx in txs:
            g = self.calc_gain(tx["from"], tx["to"], tx["usd_value"])
            block_gain += g
        return block_gain

    def run_on_block(self, block: dict) -> int:
        current_time = block["timestamp"]
        cutoff_time = current_time - self.two_delta

        while self.previous_tx and self.previous_tx[0]["timestamp"] < cutoff_time:
            old_block_data = self.previous_tx.popleft()
            self.gain_total -= old_block_data["cached_gain"]
            self.rollback_map(old_block_data["transactions"])

        txs = block["transactions"]
        current_block_gain = self.execute_txs(txs)
        self.gain_total += current_block_gain
        self.previous_tx.append({
            "timestamp": current_time,
            "transactions": txs,
            "cached_gain": current_block_gain
        })
        return self.gain_total