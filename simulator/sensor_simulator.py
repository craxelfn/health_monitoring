import os
import pandas as pd
from confluent_kafka import Producer
import threading
import time
import json

# --- CONFIG ---
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CSV_FILE = os.environ.get("CSV_FILE", "./data/human_vital_signs_dataset_2024.csv")
TOPIC = "sensor_raw"

# --- STATIC PATIENT METADATA ---
patients_info = [
    {"Patient ID": 1, "Age": 37, "Gender_Male": 0, "Weight (kg)": 91.5, "Height (m)": 1.68},
    {"Patient ID": 2, "Age": 77, "Gender_Male": 1, "Weight (kg)": 50.7, "Height (m)": 1.99},
    {"Patient ID": 3, "Age": 68, "Gender_Male": 0, "Weight (kg)": 90.3, "Height (m)": 1.77},
    {"Patient ID": 4, "Age": 45, "Gender_Male": 1, "Weight (kg)": 75.0, "Height (m)": 1.80},
]

# --- READ SENSOR DATA ---
df = pd.read_csv(CSV_FILE)

# --- KAFKA PRODUCER FUNCTION ---
def produce_patient(patient):
    patient_id = patient["Patient ID"]

    # Filter CSV data for that patient if exists
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

    for _, row in patient_df.iterrows():
        # Build message payload
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

        producer.produce(TOPIC, key=str(patient_id), value=json.dumps(message))
        producer.flush()
        print(f"Sent for patient {patient_id} at {row['Timestamp']}")
        time.sleep(1)  # simulate real-time

# --- RUN 4 PATIENTS IN PARALLEL ---
threads = []
for patient in patients_info:
    t = threading.Thread(target=produce_patient, args=(patient,))
    t.start()
    threads.append(t)

for t in threads:
    t.join()

