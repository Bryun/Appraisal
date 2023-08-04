from time import perf_counter


class Timer:
    def __init__(self):
        self.initial = None

    def start(self):
        if self.initial is not None:
            raise Exception("Timer is already running.")
        self.initial = perf_counter()

    def display(self):
        elapsed = perf_counter() - self.initial
        print(f"Elapsed time: {elapsed:0.2f} seconds")

    def stop(self):
        if self.initial is None:
            raise Exception("Timer has not been started.")
        elapsed = perf_counter() - self.initial
        self.initial = None
        print(f"Completed time: {elapsed:0.2f} seconds")
