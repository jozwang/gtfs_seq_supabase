import pandas as pd
import requests
import zipfile
import io
import streamlit as st
import psycopg2
import traceback

# --- Config ---
SUPABASE_URL = st.secrets.get("SUPABASE_URL")
GTFS_ZIP_URL = "https://www.data.qld.gov.au/dataset/general-transit-feed-specification-gtfs-translink/resource/e43b6b9f-fc2b-4630-a7c9-86dd5483552b/download"

# --- Supabase Connection ---
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

# --- Download GTFS ZIP as bytes (safe to cache) ---
@st.cache_data(show_spinner=False)
def download_gtfs_bytes():
    try:
        response = requests.get(GTFS_ZIP_URL, timeout=30)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        st.error(f"Error downloading GTFS data: {e}")
        return None

# --- Extract file list from ZIP ---
def list_gtfs_files(zip_obj):
    return [f.filename for f in zip_obj.filelist if f.filename.endswith('.txt')]

# --- Extract file as DataFrame ---
def extract_file(zip_obj, filename):
    try:
        with zip_obj.open(filename) as file:
            return pd.read_csv(file, dtype=str, low_memory=False)
    except KeyError:
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"Could not read {filename}: {e}")
        return pd.DataFrame()

# --- Get Supabase table column names ---
def get_supabase_table_columns(conn, table_name):
    query = """
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

# --- App Layout ---
st.title("Column Comparison: Supabase vs GTFS")

# Load GTFS ZIP content
zip_content = download_gtfs_bytes()
zip_obj = zipfile.ZipFile(io.BytesIO(zip_content)) if zip_content else None
gtfs_files = list_gtfs_files(zip_obj) if zip_obj else []

# Load Supabase table names
conn = get_pg_connection()
pg_tables = []
if conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        pg_tables = [row[0] for row in cur.fetchall()]

# UI selection
col1, col2 = st.columns(2)
with col1:
    selected_pg_table = st.selectbox("Select Supabase Table", pg_tables)

with col2:
    selected_gtfs_file = st.selectbox("Select GTFS File", gtfs_files)

# Compare Columns
if selected_pg_table and selected_gtfs_file:
    pg_columns = get_supabase_table_columns(conn, selected_pg_table)
    gtfs_df = extract_file(zip_obj, selected_gtfs_file)
    gtfs_columns = gtfs_df.columns.tolist()

    # Make DataFrame comparison with highlights
    max_len = max(len(pg_columns), len(gtfs_columns))
    pg_series = pd.Series(pg_columns + [None] * (max_len - len(pg_columns)))
    gtfs_series = pd.Series(gtfs_columns + [None] * (max_len - len(gtfs_columns)))

    comparison_df = pd.DataFrame({
        "Supabase Columns": pg_series,
        "GTFS File Columns": gtfs_series
    })

    def highlight_diff(val1, val2):
        if val1 != val2:
            return "background-color: yellow"
        return ""

    styled_df = comparison_df.style.apply(
        lambda row: [highlight_diff(row["Supabase Columns"], row["GTFS File Columns"])] * 2,
        axis=1
    )

    st.markdown("### Column Comparison Table")
    st.dataframe(styled_df, use_container_width=True)
