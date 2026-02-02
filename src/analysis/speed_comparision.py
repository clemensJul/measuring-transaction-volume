from datetime import datetime


class SpeedComparison:
    def __init__(self, algorithm, delta):
        self.algorithm = algorithm
        self.delta = delta
        self.time_sliding_window = []
        self.time_build_up_window = []
        self.iteration = 0

    def run_on_block(self, block):
        now = datetime.now()
        gain = self.algorithm.run_on_block(block)
        time_delta = datetime.now() - now
        
        self.time_sliding_window.append(time_delta)

        if self.iteration % self.delta == 0:
            self.time_build_up_window.append(time_delta)
        else:
            self.time_build_up_window[-1] += time_delta
        self.iteration += 1
        return gain