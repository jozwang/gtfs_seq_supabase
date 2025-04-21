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
        with st.spinner("Downloading GTFS data..."):
            response = requests.get(GTFS_ZIP_URL, timeout=30)  # Increased timeout
            response.raise_for_status()
            st.success("GTFS data downloaded successfully")
            return zipfile.ZipFile(io.BytesIO(response.content))
    except Exception as e:
        st.error(f"Failed to download GTFS ZIP: {e}")
        return None

def load_gtfs_file(zip_obj, filename):
    try:
        with st.spinner(f"Loading {filename}..."):
            with zip_obj.open(filename) as f:
                df = pd.read_csv(f, dtype=str, low_memory=False)
                st.success(f"Loaded {filename} with {len(df)} rows")
                return df
    except Exception as e:
        st.error(f"Failed to load {filename}: {e}")
        return pd.DataFrame()

def get_table_columns(table_name, conn):
    """Get column names from table in the database"""
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """)
            columns = [row[0] for row in cur.fetchall()]
            return columns
    except Exception as e:
        st.error(f"Failed to get columns for {table_name}: {e}")
        return []

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

    # Get column names from database table
    db_columns = get_table_columns(table_name, conn)
    
    if not db_columns:
        st.error(f"Could not retrieve columns for {table_name}")
        return
    
    # Match columns between DataFrame and database table
    common_columns = [col for col in df.columns if col in db_columns]
    
    if not common_columns:
        st.error(f"No matching columns found between GTFS file and {table_name}")
        st.write("GTFS file columns:", df.columns.tolist())
        st.write("Database table columns:", db_columns)
        return
    
    # Only keep columns that match the database schema
    df_filtered = df[common_columns]
    
    # Check for missing required columns
    missing_columns = set(db_columns) - set(common_columns)
    if missing_columns:
        st.warning(f"Missing columns in {table_name}: {missing_columns}")
    
    truncate_table(table_name, conn)

    values = df_filtered.values.tolist()
    insert_query = f"""
        INSERT INTO {table_name} ({', '.join(common_columns)})
        VALUES %s
    """
    
    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, values)
            conn.commit()
        st.success(f"Inserted {len(values)} rows into {table_name} using {len(common_columns)} columns")
    except Exception as e:
        st.error(f"Failed to insert into {table_name}: {e}")
        st.error(f"Error details: {str(e)}")

def preview_supabase_tables(conn, table_names):
    for table in table_names:
        try:
            query = f"SELECT * FROM {table} LIMIT 5"
            df = pd.read_sql(query, conn)
            
            # Get row count
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cur.fetchone()[0]
            
            st.subheader(f"Preview of {table} ({row_count} total rows)")
            st.dataframe(df)
        except Exception as e:
            st.error(f"Failed to query {table}: {e}")

def main():
    st.title("GTFS Loader and Supabase Preview")

    # Add file upload option for testing with local GTFS files
    uploaded_file = st.file_uploader("Upload GTFS ZIP file (optional)", type="zip")
    
    if uploaded_file is not None:
        # Use uploaded file
        zip_obj = zipfile.ZipFile(uploaded_file)
        st.success("Using uploaded GTFS file")
    else:
        # Download from URL
        zip_obj = download_and_extract_gtfs()
    
    if zip_obj is None:
        return

    # Connect to database
    conn = get_pg_connection()
    if conn is None:
        return

    # Show available files in the ZIP
    with st.expander("Available files in GTFS package"):
        st.write(zip_obj.namelist())

    # Processing section
    st.header("Upload GTFS Data to Supabase")
    
    # Add option to select which tables to process
    selected_tables = st.multiselect(
        "Select tables to process",
        options=list(file_mapping.keys()),
        default=list(file_mapping.keys())
    )
    
    if st.button("Process Selected Tables", type="primary"):
        progress_bar = st.progress(0)
        progress_text = st.empty()
        
        for i, table_name in enumerate(selected_tables):
            progress_text.text(f"Processing {table_name}...")
            progress_bar.progress((i / len(selected_tables)) * 100)
            
            file_name = file_mapping[table_name]
            df = load_gtfs_file(zip_obj, file_name)
            store_dataframe_to_db(df, table_name, conn)
        
        progress_bar.progress(100)
        progress_text.text("Processing complete!")
    
    # Preview section
    st.header("Supabase Table Previews")
    preview_supabase_tables(conn, file_mapping.keys())

    conn.close()

if __name__ == "__main__":
    main()
