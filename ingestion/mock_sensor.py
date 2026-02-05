import time
import pandas as pd


class MockSensor:
    freq_hz = int
    name = None
    sleep_interval = float
    mock_df = None
    buffer = None
    index = 0

    def __init__(self, name, path, buffer, freq_hz=60):
        self.freq_hz = freq_hz
        self.name = name
        self.sleep_interval = 1.0 / freq_hz
        self.path = path

        self.buffer = buffer
        self.mock_df = pd.read_csv(self.path)

    # Only return one value here, to control the overall volume of the rows sent in a single iteration, change the frequency (freq_hz).
    def fetch_sensor_value(self):
        if self.index >= len(self.mock_df):
            return pd.Series()  # Data exhausted
        # Mocked sensor read
        row = self.mock_df.iloc[self.index]
        self.index += 1
        return row

    def run_ingest_loop(self):
        while True:
            start = time.perf_counter()

            read: pd.Series = self.fetch_sensor_value()
            if not read.empty:
                self.buffer.push_sample(avg=read.get("avg"), min=read.get("min"), max=read.get("max"), ts=read.get("date"))

            elapsed = time.perf_counter() - start
            sleep_time = max(0.00, self.sleep_interval - elapsed)
            time.sleep(sleep_time)