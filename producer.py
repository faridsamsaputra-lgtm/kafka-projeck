import requests
from kafka import KafkaProducer
import json
import time
import certifi
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_KEY = "2eff498e979a3ec5502495d757f81eea"

print("⏳ Menunggu Kafka...")
time.sleep(5)

# 🔥 Kafka Producer (STABIL VERSION)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=10,
    linger_ms=10
)

print("✅ PRODUCER AKTIF")
print("🚀 Mulai kirim data...\n")

# HTTP Session retry
session = requests.Session()
retry = Retry(total=5, backoff_factor=1)
session.mount("https://", HTTPAdapter(max_retries=retry))

while True:
    try:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {"q": "Semarang", "appid": API_KEY, "units": "metric"}

        response = session.get(url, params=params, timeout=10, verify=certifi.where())

        if response.status_code == 200:
            data = response.json()

            payload = {
                "kota": data["name"],
                "suhu": data["main"]["temp"],
                "kelembaban": data["main"]["humidity"],
                "cuaca": data["weather"][0]["description"]
            }

            producer.send('weather_topic', value=payload)
            producer.flush()

            print("📤 KIRIM:", payload)

        else:
            print("❌ API ERROR:", response.status_code)

    except Exception as e:
        print("⚠️ ERROR:", e)

    time.sleep(5)
    