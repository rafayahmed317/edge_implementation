import time
from multiprocessing import Manager, Process
import os

from edge_flow import edge_flow
from prefect.deployments import run_deployment

from ingestion.buffer import Buffer
from ingestion.mock_sensor import MockSensor

MOCK_DATA_DIR = "S5P_Data_Exports/Palisades (2025-2026)/"
MOCK_DATA_YEAR = 2025
# Set to lower values for slowing down ingestion, higher for making it faster
MOCK_DATA_FREQ = 365

GIT_URL = "https://github.com/rafayahmed317/edge_implementation.git"
DEPLOYMENT_NAME = "edge_anomaly_detection_pipeline"
ANOMALY_DETECTION_INTERVAL = 20


if __name__ == "__main__":
    data_dir = MOCK_DATA_DIR

    year = str(MOCK_DATA_YEAR)
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
                sensor = MockSensor(name=param, path=path, buffer=buf, freq_hz=MOCK_DATA_FREQ)

                # Start sensor in a totally separate process (No GIL)
                p = Process(target=sensor.run_ingest_loop, daemon=True)
                p.start()
                processes.append(p)

        while True:
            time.sleep(ANOMALY_DETECTION_INTERVAL)
            serialized_buffers = {}

            for param, buf in active_buffers.items():
                data = buf.snapshot_and_clear()
                if data:
                    serialized_buffers[param] = data

            if deployment is None:
                edge_flow.from_source(source=os.getcwd(), entrypoint="edge_flow.py:edge_flow").deploy(
                    name=DEPLOYMENT_NAME,
                    parameters={"buffers": serialized_buffers},
                    work_pool_name="processor"
                )
                deployment = True

            result = run_deployment(name=f"edge_flow/{DEPLOYMENT_NAME}", parameters={"buffers": serialized_buffers})
            print(result)
