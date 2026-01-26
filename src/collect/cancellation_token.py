class CancellationToken:
    def __init__(self):
        self.cancelled = False
    def is_canceled(self):
        return self.cancelled
    def cancel(self):
        self.cancelled = True