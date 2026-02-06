from inspect import cleandoc
from prefect import task
from prefect.artifacts import create_markdown_artifact  # New import
import pandas as pd


SPIKE_THRESHOLD = 9
CLEAN_THRESHOLD = -5

def emit_anomaly_markdown(events: list):
    if not events:
        md = "No cross-parameter anomaly events detected."
    else:

        rows = []

        for ev in events:

            start = ev["start_date"].strftime("%Y-%m-%d")
            end = ev["end_date"].strftime("%Y-%m-%d")

            if ev["event_type"] == "spike":
                contributors = ", ".join(ev["contributors"])
                desc = f"Spike event involving {contributors}"
            else:
                desc = "Anomalously clean air"

            rows.append(
                f"| {ev['event_type']} | {start} | {end} | {desc} |"
            )

        table = "\n".join(rows)

        note = cleandoc(f"""
        ### Spike events and their probable causes : 

        #### Group: NO<sup>2</sup>, CO and Aerosol
        Causes:   Biomass burning + other types of fires

        #### Group: NO<sup>2</sup> and SO<sup>2</sup>
        Causes: High temperature combustion involving dirty/heavy fuels.

        #### NO<sup>2</sup> alone:
        Causes: Traffic exhaust.

        #### SO<sup>2</sup> alone:
        Causes: Burning of high-sulphur fuels mostly used by power plants and ships.
        """)

        md = cleandoc(f"""
        ### Cross-Parameter Anomaly Report
        | Type | Start | End | Description |
        |------|--------|-----|-------------|
        """) + "\n" + table + "\n" + note

        return md

    return md


@task
def detect_and_summarize_anomalies(results: list):
    # -----------------------------
    # Helper: Period Grouper
    # -----------------------------
    def group_periods(indices):
        if len(indices) == 0:
            return []

        periods = []
        start = indices[0]
        prev = start

        for curr in indices[1:]:
            if (curr - prev).days > 2:
                periods.append((start, prev))
                start = curr
            prev = curr

        periods.append((start, prev))
        return periods

    # -----------------------------
    # STEP 1 — Detect anomalies
    # -----------------------------
    anomalies = []

    for res in results:
        param = res["stats"]["parameter"]
        df = res["daily"].copy()

        median = df["avg"].median()
        mad = (df["avg"] - median).abs().median()

        df["mod_zscore"] = (0.6745 * (df["avg"] - median)) / (mad + 1e-9)

        threshold = 2
        detected = df[df["mod_zscore"].abs() > threshold]

        for _, row in detected.iterrows():
            anomalies.append({
                "parameter": param,
                "date": pd.to_datetime(row["date"]),
                "value": float(row["avg"]),
                "zscore": float(row["mod_zscore"])
            })

    # If no anomalies detected
    if not anomalies:
        emit_anomaly_markdown([])
        return []

    # -----------------------------
    # STEP 2 — Cross Parameter Events
    # -----------------------------
    df = pd.DataFrame(anomalies)

    pivot_df = (
        df.pivot_table(
            index="date",
            columns="parameter",
            values="zscore",
            aggfunc="mean"
        )
        .fillna(0)
    )

    if pivot_df.empty:
        emit_anomaly_markdown([])
        return []

    pivot_df["event_intensity"] = pivot_df.sum(axis=1)

    # This method of grouping discards non-significant days by only selecting days where one of the following happens:
    # (i) Multiple pollutants spike together
    # (ii) A single pollutant spikes significantly in a way that helps us point out the source with confidence.
    # Aside from almost 50% of the year when nothing spikes, the rest of the year has insignificant spikes of individual pollutants which decrease
    # confidence in their source. This helps us get rid of those.
    spike_days = sorted(pivot_df[pivot_df["event_intensity"] > SPIKE_THRESHOLD].index)
    clean_days = sorted(pivot_df[pivot_df["event_intensity"] < CLEAN_THRESHOLD].index)

    events = []

    # Spike Events
    for s, e in group_periods(spike_days):
        # Get the complete period
        sub = pivot_df.loc[s:e]

        # Select the parameters which caused this period to classify as spike period
        contributors = [
            p for p in pivot_df.drop("event_intensity", axis=1).columns
            if sub[p].mean() > 2
        ]

        events.append({
            "event_type": "spike",
            "start_date": s,
            "end_date": e,
            "contributors": contributors
        })

    # Clean Events
    for s, e in group_periods(clean_days):
        events.append({
            "event_type": "clean",
            "start_date": s,
            "end_date": e,
            "contributors": []
        })

    # -----------------------------
    # STEP 3 — Emit Markdown Artifact
    # -----------------------------
    md = emit_anomaly_markdown(events)
    create_markdown_artifact(
        key="cross-parameter-anomalies",
        markdown=md,
        description="Detected multi-sensor anomaly periods"
    )

    return events
