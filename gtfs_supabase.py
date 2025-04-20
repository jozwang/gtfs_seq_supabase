import pandas as pd
import requests
import zipfile
import io
import streamlit as st
from datetime import datetime, time
import pytz
import psycopg2
from psycopg2.extras import execute_values

# --- PostgreSQL Connection ---
PG_HOST = "eegejlqdgahlmtjniupz.supabase.co"
PG_PORT = 5432
PG_DB = "postgres"
PG_USER = "postgres"
PG_PASSWORD = "Supa1base!"  # Enter your actual DB password here

# --- GTFS Static Data URL ---
GTFS_ZIP_URL = "https://www.data.qld.gov.au/dataset/general-transit-feed-specification-gtfs-translink/resource/e43b6b9f-fc2b-4630-a7c9-86dd5483552b/download"

def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode="require"
    )

def download_gtfs():
    try:
        response = requests.get(GTFS_ZIP_URL, timeout=10)
        response.raise_for_status()
        return zipfile.ZipFile(io.BytesIO(response.content))
    except requests.RequestException as e:
        st.error(f"Error downloading GTFS data: {e}")
        return None

def extract_file(zip_obj, filename):
    try:
        with zip_obj.open(filename) as file:
            return pd.read_csv(file, dtype=str, low_memory=False)
    except Exception as e:
        st.warning(f"Could not read {filename}: {e}")
        return pd.DataFrame()

def classify_region(lat, lon):
    lat, lon = float(lat), float(lon)
    if -28.2 <= lat <= -27.8 and 153.2 <= lon <= 153.5:
        return "Gold Coast"
    elif -27.7 <= lat <= -27.2 and 152.8 <= lon <= 153.5:
        return "Brisbane"
    elif -27.2 <= lat <= -26.3 and 152.8 <= lon <= 153.3:
        return "Sunshine Coast"
    else:
        return "Other"

def store_to_postgres(table_name, df):
    if df.empty:
        return

    conn = get_pg_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"DELETE FROM {table_name};")
        columns = list(df.columns)
        values = df[columns].values.tolist()
        insert_query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES %s"
        execute_values(cursor, insert_query, values)
        conn.commit()
        st.success(f"{table_name} updated.")
    except Exception as e:
        st.error(f"Failed to update {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def load_gtfs_data(force_refresh=False):
    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = None

    brisbane_tz = pytz.timezone("Australia/Brisbane")
    now = datetime.now(brisbane_tz)
    refresh_time = datetime.combine(now.date(), time(1, 0), tzinfo=brisbane_tz)

    if force_refresh or st.session_state.last_refresh is None or (now > refresh_time and st.session_state.last_refresh < refresh_time):
        zip_obj = download_gtfs()
        if not zip_obj:
            return

        routes_df = extract_file(zip_obj, "routes.txt")
        stops_df = extract_file(zip_obj, "stops.txt")
        trips_df = extract_file(zip_obj, "trips.txt")
        stop_times_df = extract_file(zip_obj, "stop_times.txt")
        shapes_df = extract_file(zip_obj, "shapes.txt")

        stops_df["stop_lat"] = stops_df["stop_lat"].astype(float)
        stops_df["stop_lon"] = stops_df["stop_lon"].astype(float)
        stops_df["region"] = stops_df.apply(lambda row: classify_region(row["stop_lat"], row["stop_lon"]), axis=1)

        store_to_postgres("gtfs_routes", routes_df)
        store_to_postgres("gtfs_stops", stops_df)
        store_to_postgres("gtfs_trips", trips_df)
        store_to_postgres("gtfs_stop_times", stop_times_df)
        store_to_postgres("gtfs_shapes", shapes_df)

        st.session_state.last_refresh = now
    else:
        st.info("GTFS data already refreshed today.")

def show_preview_from_postgres(table_name):
    try:
        conn = get_pg_connection()
        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 5", conn)
        conn.close()
        st.subheader(f"{table_name} (latest 5 rows)")
        st.dataframe(df)
    except Exception as e:
        st.error(f"Error fetching preview from {table_name}: {e}")

# --- Streamlit App ---
st.title("GTFS Data Loader")

if st.button("Download Now"):
    load_gtfs_data(force_refresh=True)
else:
    load_gtfs_data(force_refresh=False)

# --- Show Latest Preview from DB ---
for table in ["gtfs_routes", "gtfs_stops", "gtfs_trips", "gtfs_stop_times", "gtfs_shapes"]:
    show_preview_from_postgres(table)
