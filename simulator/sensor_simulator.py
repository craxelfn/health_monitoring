import os
import json
import time
import uuid
import threading
from typing import List, Dict
from datetime import datetime

import pandas as pd
from confluent_kafka import Producer, KafkaException

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CSV_FILE = os.environ.get("CSV_FILE", "./data/human_vital_signs_dataset_2024.csv")
TOPIC = "sensor_raw"
SEND_INTERVAL_SECONDS = float(os.environ.get("SEND_INTERVAL_SECONDS", "1"))

patients_info: List[Dict] = [
    {"patient_id": "P001", "age": 37, "gender": "Female", "weight_kg": 91.5, "height_m": 1.68},
    {"patient_id": "P002", "age": 77, "gender": "Male", "weight_kg": 50.7, "height_m": 1.99},
    {"patient_id": "P003", "age": 68, "gender": "Female", "weight_kg": 90.3, "height_m": 1.77},
    {"patient_id": "P004", "age": 45, "gender": "Male", "weight_kg": 75.0, "height_m": 1.80},
]


def wait_for_csv(path: str, poll_seconds: int = 2) -> None:
    """Block until the CSV file exists and is readable."""
    while True:
        try:
            if os.path.isfile(path) and os.access(path, os.R_OK):
                print(f"✓ CSV file found at '{path}'")
                return
        except Exception:
            pass
        print(f"⏳ CSV file not available at '{path}', retrying in {poll_seconds}s...")
        time.sleep(poll_seconds)


def wait_for_kafka(bootstrap_servers: str, poll_seconds: int = 2) -> None:
    """Block until Kafka metadata can be fetched."""
    while True:
        try:
            producer = Producer({"bootstrap.servers": bootstrap_servers})
            producer.list_topics(timeout=5)
            print(f"✓ Kafka connected at {bootstrap_servers}")
            return
        except KafkaException as e:
            print(f"⏳ Kafka not ready ({e}), retrying in {poll_seconds}s...")
            time.sleep(poll_seconds)
        except Exception as e:
            print(f"⏳ Waiting for Kafka ({e}), retrying in {poll_seconds}s...")
            time.sleep(poll_seconds)


def map_csv_to_service_format(row: pd.Series, patient: Dict) -> Dict:
    """
    Maps CSV data to match Java Stream Processor service expectations.
    
    Field mapping:
    - Patient ID -> patient_id (snake_case, string)
    - Heart Rate -> heart_rate
    - Body Temperature -> temperature
    - Oxygen Saturation -> oxygen_saturation
    - Systolic Blood Pressure -> blood_pressure_systolic
    - Diastolic Blood Pressure -> blood_pressure_diastolic
    - Timestamp -> timestamp (ISO-8601 format)
    
    Added fields:
    - message_id (UUID for deduplication)
    - respiratory_rate (sourced from CSV column)
    - activity_level (derived from heart rate)
    """
    
    message_id = str(uuid.uuid4())
    
    timestamp = row.get("Timestamp", datetime.now().isoformat())
    try:
        if isinstance(timestamp, str) and 'T' in timestamp:
            iso_timestamp = timestamp
        else:
            dt = pd.to_datetime(timestamp)
            iso_timestamp = dt.isoformat()
    except:
        iso_timestamp = datetime.now().isoformat()
    
    heart_rate = float(row.get("Heart Rate", 75.0))
    
    csv_respiratory_rate = row.get("Respiratory Rate", 16)
    try:
        respiratory_rate = int(float(csv_respiratory_rate))
    except (TypeError, ValueError):
        respiratory_rate = int(heart_rate / 4.5)
    
    if heart_rate < 60:
        activity_level = "resting"
    elif heart_rate < 100:
        activity_level = "light"
    elif heart_rate < 140:
        activity_level = "moderate"
    else:
        activity_level = "vigorous"
    
    message = {
        "message_id": message_id,
        "patient_id": patient["patient_id"],
        "timestamp": iso_timestamp,
        "heart_rate": heart_rate,
        "blood_pressure_systolic": int(row.get("Systolic Blood Pressure", 120)),
        "blood_pressure_diastolic": int(row.get("Diastolic Blood Pressure", 80)),
        "temperature": float(row.get("Body Temperature", 37.0)),
        "oxygen_saturation": float(row.get("Oxygen Saturation", 98.0)),
        "respiratory_rate": respiratory_rate,
        "activity_level": activity_level,
    }
    
    return message


def produce_patient(patient: Dict) -> None:
    """
    Produces sensor events for a single patient in an infinite loop.
    """
    patient_id = patient["patient_id"]
    
    try:
        csv_patient_id = int(patient_id.replace("P", "").lstrip("0"))
    except:
        print(f"⚠️  Cannot map patient_id '{patient_id}' to CSV Patient ID")
        return
    
    patient_df = df[df["Patient ID"] == csv_patient_id]
    if patient_df.empty:
        print(f"⚠️  No CSV data for patient {patient_id} (CSV ID: {csv_patient_id}), skipping...")
        return
    
    print(f"✓ Starting producer for patient {patient_id} ({len(patient_df)} records)")
    
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "linger.ms": 10,
        "compression.type": "gzip"
    })
    
    record_count = 0
    
    while True:
        for _, row in patient_df.iterrows():
            message = map_csv_to_service_format(row, patient)
            
            try:
                producer.produce(
                    TOPIC,
                    key=patient_id,
                    value=json.dumps(message),
                    callback=lambda err, msg: delivery_callback(err, msg, patient_id)
                )
                producer.poll(0)
                
                record_count += 1
                print(f"📤 [{patient_id}] Sent record #{record_count} | HR: {message['heart_rate']:.1f} | Temp: {message['temperature']:.1f}°C")
                
            except BufferError as e:
                print(f"⚠️  [{patient_id}] Producer buffer full, sleeping...")
                time.sleep(0.5)
                continue
            except KafkaException as e:
                print(f"❌ [{patient_id}] Kafka error: {e}")
                time.sleep(1)
                continue
            except Exception as e:
                print(f"❌ [{patient_id}] Unexpected error: {e}")
                time.sleep(1)
                continue
            
            time.sleep(SEND_INTERVAL_SECONDS)
        
        try:
            producer.flush(timeout=10)
            print(f"✓ [{patient_id}] Completed cycle, restarting...")
        except Exception as e:
            print(f"⚠️  [{patient_id}] Flush error: {e}")


def delivery_callback(err, msg, patient_id: str) -> None:
    """Callback for message delivery confirmation."""
    if err:
        print(f"❌ [{patient_id}] Message delivery failed: {err}")


def main():
    """Main entry point."""
    print("=" * 60)
    print("🏥 Health Sensor Data Simulator")
    print("=" * 60)
    print(f"Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
    print(f"CSV File: {CSV_FILE}")
    print(f"Target Topic: {TOPIC}")
    print(f"Send Interval: {SEND_INTERVAL_SECONDS}s")
    print("=" * 60)
    
    wait_for_csv(CSV_FILE)
    wait_for_kafka(KAFKA_BOOTSTRAP)
    
    global df
    df = pd.read_csv(CSV_FILE)
    print(f"✓ Loaded {len(df)} records from CSV")
    print(f"✓ CSV columns: {list(df.columns)}")
    print(f"✓ Unique patients in CSV: {df['Patient ID'].nunique()}")
    print("=" * 60)
    
    threads: List[threading.Thread] = []
    for patient in patients_info:
        t = threading.Thread(
            target=produce_patient,
            args=(patient,),
            daemon=True,
            name=f"Producer-{patient['patient_id']}"
        )
        t.start()
        threads.append(t)
        time.sleep(0.5)  
    
    print(f"✓ Started {len(threads)} producer threads")
    print("=" * 60)
    print("🚀 Simulator running... Press Ctrl+C to stop")
    print("=" * 60)
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(60)
            # Print periodic health check
            alive_count = sum(1 for t in threads if t.is_alive())
            print(f"💓 Health check: {alive_count}/{len(threads)} producers active")
    except KeyboardInterrupt:
        print("\n⏹️  Shutting down simulator...")


if __name__ == "__main__":
    main()