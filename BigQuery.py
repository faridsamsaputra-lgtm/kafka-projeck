import time
import os
from google.cloud import bigquery

client = bigquery.Client.from_service_account_json(
    "myni-project-dewe-0b136d708a75.json"
)

table_id = "myni-project-dewe.weather_data.weather_stream"

def upload():
    try:
        with open("data.json", "rb") as f:
            if os.stat("data.json").st_size == 0:
                print("⚠️ File kosong, skip upload")
                return

            job = client.load_table_from_file(
                f,
                table_id,
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    autodetect=True
                )
            )

        job.result()
        print("✅ Upload sukses ke BigQuery")

        # reset file setelah sukses
        open("data.json", "w").close()

    except Exception as e:
        print("❌ ERROR UPLOAD:", e)

print("🚀 AUTO UPLOAD START")

while True:
    if os.path.exists("data.json"):
        upload()

    time.sleep(60)  # tiap 1 menit