import pandas as pd
import requests
import zipfile
import io
import psycopg2
from psycopg2.extras import execute_values
import streamlit as st

# --- GTFS Static Data URL ---
GTFS_ZIP_URL = "https://www.data.qld.gov.au/dataset/general-transit-feed-specification-gtfs-translink/resource/e43b6b9f-fc2b-4630-a7c9-86dd5483552b/download"

# --- PostgreSQL Connection ---
SUPABASE_URL = st.secrets.get("SUPABASE_DATABASE_URL")

# --- Map table names to GTFS file names ---
file_mapping = {
    "gtfs_routes": "routes.txt",
    "gtfs_trips": "trips.txt",
    "gtfs_shapes": "shapes.txt"
}

def get_pg_connection():
    try:
        conn = psycopg2.connect(SUPABASE_URL, connect_timeout=5)
        return conn
    except Exception as e:
        st.error(f"Database connection error: {str(e)}")
        return None

def download_and_extract_gtfs():
    try:
        response = requests.get(GTFS_ZIP_URL, timeout=15)
        response.raise_for_status()
        return zipfile.ZipFile(io.BytesIO(response.content))
    except Exception as e:
        st.error(f"Failed to download GTFS ZIP: {e}")
        return None

def load_gtfs_file(zip_obj, filename):
    try:
        with zip_obj.open(filename) as f:
            return pd.read_csv(f, dtype=str)
    except Exception:
        return pd.DataFrame()

def truncate_table(table_name, conn):
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()
        st.info(f"Truncated table {table_name}")
    except Exception as e:
        st.error(f"Failed to truncate {table_name}: {e}")

def store_dataframe_to_db(df, table_name, conn):
    if df.empty:
        st.warning(f"Skipping empty DataFrame for table {table_name}")
        return

    truncate_table(table_name, conn)

    columns = list(df.columns)
    values = df.values.tolist()
    insert_query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES %s
    """
    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, values)
            conn.commit()
        st.success(f"Inserted {len(values)} rows into {table_name}")
    except Exception as e:
        st.error(f"Failed to insert into {table_name}: {e}")

def preview_supabase_tables(conn, table_names):
    for table in table_names:
        try:
            query = f"SELECT * FROM {table} LIMIT 5"
            df = pd.read_sql(query, conn)
            st.subheader(f"Preview of {table}")
            st.dataframe(df)
        except Exception as e:
            st.error(f"Failed to query {table}: {e}")

def main():
    st.title("GTFS Loader and Supabase Preview")

    zip_obj = download_and_extract_gtfs()
    if zip_obj is None:
        return

    conn = get_pg_connection()
    if conn is None:
        return

    st.header("Uploading GTFS data to Supabase (with truncation)...")
    for table_name, file_name in file_mapping.items():
        df = load_gtfs_file(zip_obj, file_name)
        store_dataframe_to_db(df, table_name, conn)

    st.header("Supabase Table Previews (Top 5 rows)")
    preview_supabase_tables(conn, file_mapping.keys())

    conn.close()

if __name__ == "__main__":
    main()
