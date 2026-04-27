from kafka import KafkaConsumer
import json
import hashlib
from datetime import datetime
import time

def buat_hash(data):
    return hashlib.sha256(str(data).encode()).hexdigest()

consumer = KafkaConsumer(
    'weather_topic',  # pastikan sama dengan producer
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='file-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("🚀 CONSUMER FILE AKTIF")

buffer = []
BATCH_SIZE = 10

for msg in consumer:
    data = msg.value

    data["timestamp"] = datetime.utcnow().isoformat() + "Z"
    data["hash"] = buat_hash(data)

    buffer.append(data)
    print("📥 DATA:", data)

    # simpan ke file tiap 10 data
    if len(buffer) >= BATCH_SIZE:
        with open("data.json", "a") as f:
            for item in buffer:
                f.write(json.dumps(item) + "\n")

        print(f"💾 SIMPAN {len(buffer)} DATA KE FILE\n")
        buffer = []