import os
import json
import time
from google.cloud import bigquery

# =============================
# 🔗 INIT BIGQUERY (STREAMLIT SECRET)
# =============================
def init_bq():
    key = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    return bigquery.Client.from_service_account_info(key)

client = init_bq()

table_id = "myni-project-dewe.weather_data.weather_stream"

# =============================
# 🚀 FUNCTION UPLOAD
# =============================
def upload():
    try:
        if not os.path.exists("data.json"):
            print("📂 File belum ada")
            return

        if os.path.getsize("data.json") == 0:
            print("⚠️ File kosong, skip upload")
            return

        with open("data.json", "rb") as f:
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

        # reset file
        open("data.json", "w").close()

    except Exception as e:
        print("❌ ERROR UPLOAD:", e)

# =============================
# 🔄 AUTO LOOP
# =============================
print("🚀 AUTO UPLOAD START")

while True:
    upload()
    time.sleep(60)  # tiap 1 menit
