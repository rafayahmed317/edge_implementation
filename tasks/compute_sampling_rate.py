from prefect import task
from prefect.artifacts import create_markdown_artifact  # New import

# Set min interval to a single day to match the interval of the mocked data, in real life, it would be around 1/60 (60 times in a second), representing the max speed of the sensor
MIN_INTERVAL = 24 * 60 * 60
# Set max interval to 2 days to appear sensible with respect to the mocked data, in real life the value would be much, much lower, like around 60 seconds
MAX_INTERVAL = 2 * 24 * 60 * 60
# The variance threshold on which to drop to maximum speed to not miss out on important data
VAR_THRESHOLD = 0.3


@task
def compute_sampling_rate(results: list):
    sampling_reports = {}
    artifact_rows = []

    for res in results:
        param = res["stats"]["parameter"]
        df = res["daily"].copy()

        # 1. Calculate the 'variance' (Rolling Standard Deviation).
        # We look at a 3-day window to see how much the signal is wiggling, in real life we would be looking at much shorter spaced values.
        df["volatility"] = df["avg"].rolling(window=3, min_periods=1).std().fillna(0)

        current_rate = MIN_INTERVAL
        rates_over_time = []

        # 2. Simulate the Adaptive Loop.
        for _, row in df.iterrows():
            volatility = row["volatility"]

            if volatility > VAR_THRESHOLD:
                # Means there is increased activity: Reset to max speed (MIN_INTERVAL) to capture that.
                current_rate = MIN_INTERVAL
            else:
                # Increase the interval (slow down) by 1 hour (3600s) until we hit the MAX_INTERVAL cap.
                current_rate = min(MAX_INTERVAL, current_rate + 3600)

            rates_over_time.append(current_rate)

        avg_rate = sum(rates_over_time) / len(rates_over_time)
        sampling_reports[param] = {
            "avg_rate_seconds": avg_rate,
            "efficiency_gain": f"{((avg_rate - MIN_INTERVAL) / MIN_INTERVAL) * 100:.2f}%"
        }

        artifact_rows.append(
            f"| {param} | {avg_rate / 3600:.1f} hrs | {sampling_reports[param]['efficiency_gain']} |"
        )

    tabular_data = "\n".join(artifact_rows)
    sampling_md = f"""
    ### Adaptive Sampling Strategy Report
    | Sensor | Avg. Sampling Interval | Data Savings (vs Max Speed) |
    | :--- | :--- | :--- |
    {tabular_data}

    **Logic Applied:** * **Fast-Reset:** If 3-day volatility > {VAR_THRESHOLD}, interval drops to 24h.
    * **Gradual-Backoff:** If calm, interval increases +1hr/day up to 48h.
    """
    create_markdown_artifact(
        key="sampling-strategy",
        markdown=sampling_md,
        description="Edge computation sampling rate optimization"
    )

    return sampling_reports
