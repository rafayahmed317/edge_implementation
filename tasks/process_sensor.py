from prefect import task
from prefect.artifacts import create_markdown_artifact  # New import
import pandas as pd


@task
def process_sensor(parameter: str, samples: list[tuple[float, float]]):
    if not samples:
        return None

    df = pd.DataFrame(samples)
    df["date"] = pd.to_datetime(df["ts"])

    stats = {
        "parameter": parameter,
        "mean_avg": df["avg"].mean(),
        "max_peak": df["max"].max(),
        "min_trough": df["min"].min(),
        "daily_variance": df["avg"].var(),
    }

    # --- Create Artifact ---
    markdown = f"""### Sensor Stats: {parameter.upper()}
| Metric | Value |
| :--- | :--- |
| Mean | {stats['mean_avg']:.4f} |
| Max Peak | {stats['max_peak']:.4f} |
| Min Trough | {stats['min_trough']:.4f} |
| Variance | {stats['daily_variance']:.4f} |
"""
    create_markdown_artifact(
        key=f"{parameter}-stats",
        markdown=markdown,
        description=f"Processing results for {parameter}"
    )

    return {
        "stats": stats,
        "daily": df[["date", "avg", "min", "max"]].copy()
    }
