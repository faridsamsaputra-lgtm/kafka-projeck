import streamlit as st
from google.cloud import bigquery
import pandas as pd
import hashlib
import json
from datetime import datetime
from streamlit_autorefresh import st_autorefresh

# =============================
# 🔐 LOGIN ADMIN
# =============================
def login():
    st.title("🔐 Login Admin")
    user = st.text_input("Username")
    pw = st.text_input("Password", type="password")

    if st.button("Login"):
        if user == "admin" and pw == "123":
            st.session_state["login"] = True
        else:
            st.error("Username / Password salah")

if "login" not in st.session_state:
    st.session_state["login"] = False

if not st.session_state["login"]:
    login()
    st.stop()

# =============================
# 🔗 KONEKSI BIGQUERY
# =============================
@st.cache_resource
def init_bq():
    return bigquery.Client.from_service_account_json(
        "myni-project-dewe-0b136d708a75.json"
    )

client = init_bq()

# =============================
# 📥 QUERY DATA
# =============================
query = """
SELECT *
FROM `myni-project-dewe.weather_data.weather_stream`
ORDER BY timestamp DESC
LIMIT 100
"""

try:
    df = client.query(query).to_dataframe()
except Exception as e:
    st.error(f"❌ Error BigQuery: {e}")
    st.stop()

# =============================
# ❗ HANDLE DATA KOSONG
# =============================
if df.empty:
    st.warning("⚠️ Data belum masuk ke BigQuery")
    st.stop()

st.title("📊 Dashboard Cuaca Real-Time")

# =============================
# 🔍 FILTER KOTA
# =============================
kota = st.selectbox("Pilih Kota", df['kota'].unique())
df = df[df['kota'] == kota]

if df.empty:
    st.warning("Data kota tidak ditemukan")
    st.stop()

# =============================
# 📊 FIX PANDAS WARNING
# =============================
df = df.copy()
df['timestamp'] = pd.to_datetime(df['timestamp'])

# =============================
# 🌡️ STATUS CUACA
# =============================
suhu = df['suhu'].iloc[0]

if suhu > 30:
    status = "🔥 PANAS"
    warna = "red"
elif suhu < 25:
    status = "❄️ DINGIN"
    warna = "blue"
else:
    status = "🌤️ NORMAL"
    warna = "green"

st.markdown(f"<h3 style='color:{warna}'>{status}</h3>", unsafe_allow_html=True)

# =============================
# 📊 METRIC
# =============================
col1, col2, col3 = st.columns(3)

col1.metric("🌡️ Suhu", suhu)
col2.metric("💧 Kelembaban", df['kelembaban'].iloc[0])
col3.metric("☁️ Cuaca", df['cuaca'].iloc[0])

# =============================
# 📈 GRAFIK
# =============================
st.subheader("📈 Grafik Suhu")
st.line_chart(df.set_index('timestamp')['suhu'])

st.subheader("📈 Grafik Kelembaban")
st.line_chart(df.set_index('timestamp')['kelembaban'])

# =============================
# 🔐 VALIDASI HASH (FIX FINAL)
# =============================
def cek_hash(row):
    data = {
        "kota": row["kota"],
        "suhu": row["suhu"],
        "kelembaban": row["kelembaban"],
        "cuaca": row["cuaca"],
        "timestamp": row["timestamp"].isoformat()
    }

    hash_baru = hashlib.sha256(
        json.dumps(data, sort_keys=True).encode()
    ).hexdigest()

    # cek kolom aman
    if "hash" not in row:
        return False

    return hash_baru == row["hash"]

df["valid"] = df.apply(cek_hash, axis=1)

st.subheader("🔐 Validasi Data (Hash Check)")
st.dataframe(df[["kota","suhu","kelembaban","cuaca","valid"]])

# =============================
# 📋 DATA LENGKAP
# =============================
st.subheader("📋 Data Lengkap")
st.dataframe(df)

# =============================
# 🔄 AUTO REFRESH (REALTIME)
# =============================
st_autorefresh(interval=5000, key="refresh")