from prefect import flow
from prefect.futures import wait

from tasks.process_sensor import process_sensor
from tasks.detect_anomalies import detect_and_summarize_anomalies
from tasks.compute_sampling_rate import compute_sampling_rate

@flow(name="edge_flow")
def edge_flow(buffers):
    futures = []
    for name, data in buffers.items():
        if data:
            result = process_sensor.submit(name, data)
            futures.append(result)

    wait(futures)
    results = [f.result() for f in futures if f.state.is_completed()]

    anomalies = detect_and_summarize_anomalies.submit(results)
    sampling = compute_sampling_rate.submit(results)

    return {"anomalies": anomalies, "sampling": sampling}


def edge_flow_local(buffers):
    results = []
    for name, data in buffers.items():
        if data:
            result = process_sensor.fn(name, data)
            results.append(result)

    anomalies = detect_and_summarize_anomalies.fn(results)
    sampling = compute_sampling_rate.fn(results)

    return {"anomalies": anomalies, "sampling": sampling}