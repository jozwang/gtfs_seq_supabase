import pandas as pd
import requests
import zipfile
import io
import streamlit as st
import psycopg2
import traceback

# --- Configuration ---
SUPABASE_URL = st.secrets.get("SUPABASE_URL")
GTFS_ZIP_URL = "https://www.data.qld.gov.au/dataset/general-transit-feed-specification-gtfs-translink/resource/e43b6b9f-fc2b-4630-a7c9-86dd5483552b/download"

# --- Database Connection ---
def get_pg_connection():
    try:
        return psycopg2.connect(SUPABASE_URL, connect_timeout=5)
    except psycopg2.OperationalError as e:
        st.error(f"Connection failed: {e}")
        return None
    except Exception as e:
        st.error(f"Unexpected error: {e}")
        st.error(traceback.format_exc())
        return None

# --- GTFS Download ---
@st.cache_data(show_spinner=False)
def download_gtfs():
    try:
        response = requests.get(GTFS_ZIP_URL, timeout=30)
        response.raise_for_status()
        return zipfile.ZipFile(io.BytesIO(response.content))
    except requests.RequestException as e:
        st.error(f"Error downloading GTFS data: {e}")
        return None

# --- Extract a file from GTFS ---
def extract_file(zip_obj, filename):
    try:
        with zip_obj.open(filename) as file:
            return pd.read_csv(file, dtype=str, low_memory=False)
    except KeyError:
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"Could not read {filename}: {e}")
        return pd.DataFrame()

# --- Load list of GTFS files ---
def list_gtfs_files(zip_obj):
    return [f.filename for f in zip_obj.filelist if f.filename.endswith('.txt')]

# --- Load column names from Supabase ---
def get_supabase_table_columns(conn, table_name):
    query = f"""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = 'public' AND table_name = %s;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (table_name,))
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        st.error(f"Error reading Supabase table: {e}")
        return []

# --- Streamlit App ---
st.title("Compare Column Names: Supabase vs GTFS ZIP")

# Load GTFS files
zip_obj = download_gtfs()
gtfs_files = list_gtfs_files(zip_obj) if zip_obj else []

# Supabase connection
conn = get_pg_connection()
pg_tables = []

if conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema='public' AND table_type='BASE TABLE'
            ORDER BY table_name;
        """)
        pg_tables = [row[0] for row in cur.fetchall()]

col1, col2 = st.columns(2)

with col1:
    selected_pg_table = st.selectbox("Select Supabase Table", pg_tables)

with col2:
    selected_gtfs_file = st.selectbox("Select GTFS File", gtfs_files)

# Load and compare
if selected_pg_table and selected_gtfs_file:
    pg_columns = get_supabase_table_columns(conn, selected_pg_table)
    gtfs_df = extract_file(zip_obj, selected_gtfs_file)
    gtfs_columns = gtfs_df.columns.tolist()

    st.markdown("### Column Comparison")
    comparison_df = pd.DataFrame({
        "Supabase Columns": pd.Series(pg_columns),
        "GTFS File Columns": pd.Series(gtfs_columns)
    })

    st.dataframe(comparison_df, use_container_width=True)
