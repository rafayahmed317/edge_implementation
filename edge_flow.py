import time
from multiprocessing import Manager, Process
import os

from prefect import flow
from prefect.futures import wait
from prefect.deployments import run_deployment

from tasks.process_sensor import process_sensor
from tasks.detect_anomalies import detect_and_summarize_anomalies
from tasks.compute_sampling_rate import compute_sampling_rate

from ingestion.buffer import Buffer
from ingestion.mock_sensor import MockSensor


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


if __name__ == "__main__":
    data_dir = "S5P_Data_Exports/Palisades (2025-2026)/"
    anomaly_detection_interval = 20

    year = str(2025)
    sensors = {
        "aerosol": f"{data_dir}S5P_Aerosol_Stats_{year}.csv",
        "no2": f"{data_dir}S5P_NO2_Stats_{year}.csv",
        "so2": f"{data_dir}S5P_SO2_Stats_{year}.csv",
        "co": f"{data_dir}S5P_CO_Stats_{year}.csv",
        "hcho": f"{data_dir}S5P_HCHO_Stats_{year}.csv",
        "o3": f"{data_dir}S5P_O3_Stats_{year}.csv",
        "ch4": f"{data_dir}S5P_CH4_Stats_{year}.csv"
    }
    active_buffers = {}

    deployment = None

    # Manager handles IPC (Inter-Process Communication)
    with Manager() as manager:
        processes = []

        for param, path in sensors.items():
            if os.path.exists(path):
                # Create a shared buffer for this sensor
                buf = Buffer(manager)
                active_buffers[param] = buf

                # Initialize sensor
                sensor = MockSensor(name=param, path=path, buffer=buf, freq_hz=365)

                # Start sensor in a totally separate process (No GIL)
                p = Process(target=sensor.run_ingest_loop, daemon=True)
                p.start()
                processes.append(p)

        while True:
            time.sleep(anomaly_detection_interval)
            serialized_buffers = {}

            for param, buf in active_buffers.items():
                data = buf.snapshot_and_clear()
                if data:
                    serialized_buffers[param] = data

            if deployment is None:
                edge_flow.from_source(source="https://github.com/rafayahmed317/edge_implementation.git", entrypoint="edge_flow.py:edge_flow").deploy(
                    name="edge_anomaly_detection_pipeline",
                    parameters={"buffers": serialized_buffers},
                    work_pool_name="processor"
                )
                deployment = True

            result = run_deployment(name="edge_flow/edge_anomaly_detection_pipeline", parameters={"buffers": serialized_buffers})
            print(result)
