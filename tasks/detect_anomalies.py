from prefect import task
from prefect.artifacts import create_markdown_artifact  # New import
import pandas as pd


def detect_cross_parameter_events(anomalies_list: list):
    """
    Currently uses simple threshold on the sum of mad_zcores of each pollutant to give indications.
    In the future this can be replaced with one of the following approaches:

    1. A Source Apportionment model.
    2. Sentence Transformers trained on historical context
    3. LLMs as reasoning engines; passing data-rich reports to an LLM to synthesize context (wind, location, seasonal trends) for high-level causality analysis.

    :param anomalies_list:
    :return: A clean summary of all cross-pollutant anomalies with clear timeline and probable cause.
    """
    # 1. Correct check for empty data
    if not anomalies_list:
        return "No anomalies recorded to analyze."

    df = pd.DataFrame(anomalies_list)

    # Ensure date is datetime objects
    df['date'] = pd.to_datetime(df['date'])

    # 2. Pivot the data
    # We use 'zscore' to identify the magnitude of the shift
    pivot_df = df.pivot_table(
        index='date',
        columns='parameter',
        values='zscore',
        aggfunc='mean'
    ).fillna(0)

    if pivot_df.empty:
        return "Analysis yielded an empty data matrix."

    # 3. IDENTIFY SPIKES (Fires / Pollution)
    # Focus on the 'Big 3' for fire: aerosol, co, no2
    # We look for days where the COMBINED Z-score is high
    fire_indicators = ['aerosol', 'co', 'no2', 'hcho']
    existing_indicators = [c for c in fire_indicators if c in pivot_df.columns]

    pivot_df['event_intensity'] = pivot_df[existing_indicators].sum(axis=1)

    # Threshold: A sum of Z-scores > 6 suggests at least 2 sensors are spiking hard
    spike_days = pivot_df[pivot_df['event_intensity'] > 6].sort_index()

    # 4. IDENTIFY CLEAN PERIODS
    # These are dates where the Z-score is significantly negative (e.g., < -2)
    # implying values are much lower than the median.
    clean_days = pivot_df[pivot_df['event_intensity'] < -5].sort_index()

    results = []

    # Helper to group dates into periods
    def get_periods(indices):
        if indices.empty: return []
        periods = []
        start = indices[0]
        prev = start
        for curr in indices[1:]:
            if (curr - prev).days > 2:  # Allow 2-day gaps
                periods.append((start, prev))
                start = curr
            prev = curr
        periods.append((start, prev))
        return periods

    # Process Spikes
    for s, e in get_periods(spike_days.index):
        sub = pivot_df.loc[s:e]
        top_contributors = [p for p in existing_indicators if sub[p].mean() > 2]

        msg = f"Significant Spike ({s.strftime('%Y-%m-%d')} to {e.strftime('%Y-%m-%d')}): "
        if 'aerosol' in top_contributors and 'co' in top_contributors:
            msg += f"Likely FIRE/SMOKE event detected (High {' + '.join(top_contributors)})."
        else:
            msg += f"General pollution increase in {' + '.join(top_contributors)}."
        results.append(msg)

    # Process Clean Air
    for s, e in get_periods(clean_days.index):
        results.append(
            f"CLEAN AIR PERIOD ({s.strftime('%Y-%m-%d')} to {e.strftime('%Y-%m-%d')}): "
            f"Pollutant levels were significantly lower than seasonal norms."
        )

    return "\n".join(results) if results else "No cross-parameter events identified."


@task
def detect_anomalies(results: list):
    anomalies = []
    for res in results:
        param = res["stats"]["parameter"]
        df = res["daily"].copy()

        # 1. Calculate the Median and MAD (Median Absolute Deviation)
        # We use the whole series (or a very large window) to establish 'normal'
        median = df["avg"].median()
        ad = (df["avg"] - median).abs()  # Absolute Deviations
        mad = ad.median()  # Median Absolute Deviation

        # 2. Calculate Modified Z-score
        # The constant 0.6745 makes it comparable to standard Z-score
        # We add a tiny epsilon (1e-9) to avoid division by zero if MAD is 0
        df["mod_zscore"] = (0.6745 * (df["avg"] - median)) / (mad + 1e-9)

        # 3. Use a standard threshold (3.5 is the statistical recommendation, but we are not catching enough anomalies with that, so we use 2)
        # This applies regardless of whether values are 0.0001 or 1000.
        threshold = 2
        detected = df[df["mod_zscore"].abs() > threshold]

        for _, row in detected.iterrows():
            anomalies.append({
                "parameter": param,
                "date": row["date"].strftime('%Y-%m-%d'),
                "value": row["avg"],
                "zscore": row["mod_zscore"]
            })

    anomaly_summary = None
    if anomalies:
        anomaly_summary = detect_cross_parameter_events(anomalies)

    return anomaly_summary
