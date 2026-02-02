class ValueComparison:
    def __init__(self, algorithm, delta):
        self.algorithm = algorithm
        self.delta = delta
        self.values = []

    def run_on_block(self, block):
        self.values.append(self.algorithm.run_on_block(block))

