import os
import json
import time
import threading
from typing import List, Dict

import pandas as pd
from confluent_kafka import Producer, KafkaException

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CSV_FILE = os.environ.get("CSV_FILE", "./data/human_vital_signs_dataset_2024.csv")
TOPIC = "sensor_raw"

patients_info: List[Dict] = [
    {"Patient ID": 1, "Age": 37, "Gender_Male": 0, "Weight (kg)": 91.5, "Height (m)": 1.68},
    {"Patient ID": 2, "Age": 77, "Gender_Male": 1, "Weight (kg)": 50.7, "Height (m)": 1.99},
    {"Patient ID": 3, "Age": 68, "Gender_Male": 0, "Weight (kg)": 90.3, "Height (m)": 1.77},
    {"Patient ID": 4, "Age": 45, "Gender_Male": 1, "Weight (kg)": 75.0, "Height (m)": 1.80},
]

def wait_for_csv(path: str, poll_seconds: int = 2) -> None:
    """Block until the CSV file exists and is readable."""
    while True:
        try:
            if os.path.isfile(path) and os.access(path, os.R_OK):
                return
        except Exception:
            pass
        print(f"CSV file not available at '{path}', retrying in {poll_seconds}s...")
        time.sleep(poll_seconds)

def wait_for_kafka(bootstrap_servers: str, poll_seconds: int = 2) -> None:
    """Block until Kafka metadata can be fetched."""
    while True:
        try:
            producer = Producer({"bootstrap.servers": bootstrap_servers})
            producer.list_topics(timeout=5)
            return
        except KafkaException as e:
            print(f"Kafka not ready ({e}), retrying in {poll_seconds}s...")
            time.sleep(poll_seconds)
        except Exception as e:
            print(f"Waiting for Kafka ({e}), retrying in {poll_seconds}s...")
            time.sleep(poll_seconds)

wait_for_csv(CSV_FILE)
df = pd.read_csv(CSV_FILE)

wait_for_kafka(KAFKA_BOOTSTRAP)

def produce_patient(patient):
    patient_id = patient["Patient ID"]

    patient_df = df[df["Patient ID"] == patient_id]
    if patient_df.empty:
        print(f"No CSV data for patient {patient_id}, skipping...")
        return

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "linger.ms": 10
    })

    while True:
        for _, row in patient_df.iterrows():
            message = {
                "Patient ID": patient_id,
                "Age": patient["Age"],
                "Gender_Male": patient["Gender_Male"],
                "Weight (kg)": patient["Weight (kg)"],
                "Height (m)": patient["Height (m)"],
                "Heart Rate": row["Heart Rate"],
                "Body Temperature": row["Body Temperature"],
                "Oxygen Saturation": row["Oxygen Saturation"],
                "Systolic Blood Pressure": row["Systolic Blood Pressure"],
                "Diastolic Blood Pressure": row["Diastolic Blood Pressure"],
                "Derived_HRV": row["Derived_HRV"],
                "Derived_Pulse_Pressure": row["Derived_Pulse_Pressure"],
                "Derived_BMI": row["Derived_BMI"],
                "Derived_MAP": row["Derived_MAP"],
                "Timestamp": row["Timestamp"]
            }

            try:
                producer.produce(TOPIC, key=str(patient_id), value=json.dumps(message))
                producer.poll(0)
                print(f"Sent for patient {patient_id} at {row['Timestamp']}")
            except BufferError as e:
                print(f"Producer buffer full ({e}), sleeping and retrying...")
                time.sleep(0.5)
                continue
            except KafkaException as e:
                print(f"Kafka error while producing: {e}, will retry...")
                time.sleep(1)
                continue
            except Exception as e:
                print(f"Unexpected error while producing: {e}, will retry...")
                time.sleep(1)
                continue

            time.sleep(1)

        try:
            producer.flush(timeout=10)
        except Exception as e:
            print(f"Flush error (ignored): {e}")

threads: List[threading.Thread] = []
for patient in patients_info:
    t = threading.Thread(target=produce_patient, args=(patient,), daemon=True)
    t.start()
    threads.append(t)

while True:
    time.sleep(60)
