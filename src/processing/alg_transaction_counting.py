from collections import deque

class TransactionCounting:
    def __init__(self, two_delta ):
        self.two_delta = two_delta
        self.gain_total = 0
        self.previous_tx = deque()

    def run_on_block(self, block: dict) -> int:
        #remove old transactions
        current_time = block["timestamp"]
        cutoff_time = current_time - self.two_delta
        while self.previous_tx and self.previous_tx[0][0] < cutoff_time:
            self.gain_total = self.gain_total - self.previous_tx[0][1]
            self.previous_tx.popleft()

        #add new transaction
        transaction_sum = sum(tx['usd_value'] for tx in block['transactions'])
        self.previous_tx.append((current_time, transaction_sum))
        self.gain_total = self.gain_total + transaction_sum
        return self.gain_total