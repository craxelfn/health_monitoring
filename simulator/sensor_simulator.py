import os
import pandas as pd
from confluent_kafka import Producer
import threading
import time
import uuid

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CSV_FILE = os.environ.get("CSV_FILE", "./data/human_vital_signs_dataset_2024.csv")
TOPIC = "sensor_raw"

# Read CSV
df = pd.read_csv(CSV_FILE)

# Group by Patient ID
patients = df['Patient ID'].unique()

def produce_patient(patient_id):
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP,
                         'acks': 'all',
                         'enable.idempotence': True,
                         'retries': 5,
                         'linger.ms': 10})
    
    patient_df = df[df['Patient ID'] == patient_id]
    for _, row in patient_df.iterrows():
        message = row.to_json()
        producer.produce(TOPIC, key=str(patient_id), value=message)
        producer.flush()
        print(f"Sent for patient {patient_id}: {row['Timestamp']}")
        time.sleep(1)  # simulate real-time

threads = []
for pid in patients[:4]:  # only 4 patients in parallel
    t = threading.Thread(target=produce_patient, args=(pid,))
    t.start()
    threads.append(t)

for t in threads:
    t.join()
