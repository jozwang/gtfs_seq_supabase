import pandas as pd
import requests
import zipfile
import io
import streamlit as st
from datetime import datetime, time
import pytz
import psycopg2
from psycopg2.extras import execute_values
import time as time_module  # Renamed to avoid conflict with datetime.time

# --- PostgreSQL Connection ---
SUPABASE_URL = st.secrets.get("SUPABASE_URL")
# --- GTFS Static Data URL ---
GTFS_ZIP_URL = "https://www.data.qld.gov.au/dataset/general-transit-feed-specification-gtfs-translink/resource/e43b6b9f-fc2b-4630-a7c9-86dd5483552b/download"

def get_pg_connection():
    try:
        # Connect using the connection URL
        conn = psycopg2.connect(
            SUPABASE_URL,
            connect_timeout=5
        )
        return conn
    except psycopg2.OperationalError as e:
        st.error(f"Connection failed (operational error): {str(e)}")
        return None
    except Exception as e:
        st.error(f"Connection failed (unexpected error): {str(e)}")
        st.error(traceback.format_exc())
        return None

def download_gtfs():
    try:
        response = requests.get(GTFS_ZIP_URL, timeout=30)  # Increased timeout
        response.raise_for_status()
        return zipfile.ZipFile(io.BytesIO(response.content))
    except requests.RequestException as e:
        st.error(f"Error downloading GTFS data: {e}")
        return None

def extract_file(zip_obj, filename):
    try:
        with zip_obj.open(filename) as file:
            return pd.read_csv(file, dtype=str, low_memory=False)
    except KeyError as e:
        st.warning(f"File {filename} not found in GTFS package: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"Could not read {filename}: {e}")
        return pd.DataFrame()

def classify_region(lat, lon):
    try:
        lat, lon = float(lat), float(lon)
        if -28.2 <= lat <= -27.8 and 153.2 <= lon <= 153.5:
            return "Gold Coast"
        elif -27.7 <= lat <= -27.2 and 152.8 <= lon <= 153.5:
            return "Brisbane"
        elif -27.2 <= lat <= -26.3 and 152.8 <= lon <= 153.3:
            return "Sunshine Coast"
        else:
            return "Other"
    except (ValueError, TypeError):
        return "Unknown"

def store_to_postgres(table_name, df):
    if df.empty:
        st.warning(f"No data to store in {table_name}")
        return

    conn = get_pg_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {table_name};")  # Use TRUNCATE instead of DELETE for better performance
        
        # Filter columns to match the table schema
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position;")
        db_columns = [col[0] for col in cursor.fetchall()]
        
        # Only use columns that exist in the database
        valid_columns = [col for col in df.columns if col in db_columns]
        
        if not valid_columns:
            st.error(f"No matching columns found for {table_name}")
            return
            
        # Insert data
        values = df[valid_columns].values.tolist()
        insert_query = f"INSERT INTO {table_name} ({','.join(valid_columns)}) VALUES %s"
        execute_values(cursor, insert_query, values)
        conn.commit()
        st.success(f"{table_name} updated with {len(df)} records.")
    except Exception as e:
        st.error(f"Failed to update {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def load_specific_gtfs_table(table_name):
    """
    Load and update a specific GTFS table
    """
    # Create a progress bar
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # Step 1: Download GTFS data (30%)
    status_text.text(f"Downloading GTFS data for {table_name}...")
    zip_obj = download_gtfs()
    if not zip_obj:
        progress_bar.empty()
        status_text.empty()
        return
    progress_bar.progress(30)
    
    # Step 2: Extract specific data file (60%)
    status_text.text(f"Extracting {table_name} data...")
    
    # Map table name to corresponding file
    file_mapping = {
        "gtfs_routes": "routes.txt",
        "gtfs_stops": "stops.txt",
        "gtfs_trips": "trips.txt",
        "gtfs_stop_times": "stop_times.txt",
        "gtfs_shapes": "shapes.txt"
    }
    
    if table_name not in file_mapping:
        st.error(f"Unknown table: {table_name}")
        progress_bar.empty()
        status_text.empty()
        return
        
    file_name = file_mapping[table_name]
    df = extract_file(zip_obj, file_name)
    
    # Special processing for stops table
    if table_name == "gtfs_stops" and not df.empty and 'stop_lat' in df.columns and 'stop_lon' in df.columns:
        # Apply region classification
        df["region"] = df.apply(
            lambda row: classify_region(row["stop_lat"], row["stop_lon"]), 
            axis=1
        )
    
    progress_bar.progress(60)
    
    # Step 3: Store data to PostgreSQL (100%)
    status_text.text(f"Storing {table_name} data to PostgreSQL...")
    store_to_postgres(table_name, df)
    progress_bar.progress(100)
    
    # Display completion message
    status_text.text(f"{table_name} update completed successfully!")
    
    # Clear progress bar after 3 seconds
    time_module.sleep(3)
    progress_bar.empty()
    status_text.empty()
    
    # Show success message
    brisbane_tz = pytz.timezone("Australia/Brisbane")
    now = datetime.now(brisbane_tz)
    st.success(f"{table_name} successfully updated at {now.strftime('%Y-%m-%d %H:%M:%S')}")

def load_all_gtfs_data(force_refresh=False):
    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = None

    brisbane_tz = pytz.timezone("Australia/Brisbane")
    now = datetime.now(brisbane_tz)
    
    # Set refresh time to 1 AM
    refresh_time = datetime.combine(now.date(), time(1, 0)).replace(tzinfo=brisbane_tz)

    if force_refresh or st.session_state.last_refresh is None or (now > refresh_time and (st.session_state.last_refresh is None or st.session_state.last_refresh < refresh_time)):
        # Create a progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Step 1: Download GTFS data (10%)
        status_text.text("Downloading GTFS data...")
        zip_obj = download_gtfs()
        if not zip_obj:
            progress_bar.empty()
            status_text.empty()
            return
        progress_bar.progress(10)
        
        # Step 2: Extract data files (50%)
        status_text.text("Extracting GTFS files...")
        routes_df = extract_file(zip_obj, "routes.txt")
        progress_bar.progress(20)
        
        stops_df = extract_file(zip_obj, "stops.txt")
        progress_bar.progress(30)
        
        trips_df = extract_file(zip_obj, "trips.txt")
        progress_bar.progress(40)
        
        stop_times_df = extract_file(zip_obj, "stop_times.txt")
        progress_bar.progress(50)
        
        shapes_df = extract_file(zip_obj, "shapes.txt")
        progress_bar.progress(60)

        # Step 3: Process data (70%)
        status_text.text("Processing GTFS data...")
        if not stops_df.empty and 'stop_lat' in stops_df.columns and 'stop_lon' in stops_df.columns:
            # Apply region classification
            stops_df["region"] = stops_df.apply(
                lambda row: classify_region(row["stop_lat"], row["stop_lon"]), 
                axis=1
            )
        progress_bar.progress(70)

        # Step 4: Store data to PostgreSQL (100%)
        status_text.text("Storing data to PostgreSQL...")
        store_to_postgres("gtfs_routes", routes_df)
        progress_bar.progress(75)
        
        store_to_postgres("gtfs_stops", stops_df)
        progress_bar.progress(80)
        
        store_to_postgres("gtfs_trips", trips_df)
        progress_bar.progress(85)
        
        # Uncommented this line to restore stop_times loading
        store_to_postgres("gtfs_stop_times", stop_times_df)
        progress_bar.progress(95)
        
        store_to_postgres("gtfs_shapes", shapes_df)
        progress_bar.progress(100)
        
        # Update last refresh time
        st.session_state.last_refresh = now
        
        # Display completion message
        status_text.text("Data refresh completed successfully!")
        
        # Clear progress bar after 3 seconds
        time_module.sleep(3)
        progress_bar.empty()
        status_text.empty()
        
        # Show success message
        st.success(f"GTFS data successfully refreshed at {now.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        st.info(f"GTFS data already refreshed today at {st.session_state.last_refresh.strftime('%Y-%m-%d %H:%M:%S')}.")

def check_table_exists(table_name):
    conn = get_pg_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table_name}'
            );
        """)
        exists = cursor.fetchone()[0]
        return exists
    except Exception as e:
        st.error(f"Error checking if table exists: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def show_preview_from_postgres(table_name):
    if not check_table_exists(table_name):
        st.warning(f"Table {table_name} does not exist yet. Please run the data loader first.")
        return
        
    try:
        conn = get_pg_connection()
        query = f"SELECT * FROM {table_name} LIMIT 5"
        df = pd.read_sql(query, conn)
        conn.close()
        
        if df.empty:
            st.info(f"No data in {table_name}")
        else:
            st.subheader(f"{table_name} (latest 5 rows)")
            st.dataframe(df)
            
            # Show record count
            conn = get_pg_connection()
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            conn.close()
            st.text(f"Total records: {count}")
    except Exception as e:
        st.error(f"Error fetching preview from {table_name}: {e}")

# --- Streamlit App ---
st.title("GTFS Data Loader")
st.write("This app downloads and loads TransLink GTFS data into PostgreSQL database.")

# Define available tables
tables = ["gtfs_routes", "gtfs_stops", "gtfs_trips", "gtfs_stop_times", "gtfs_shapes"]

# Create tabs for different functionalities
tab1, tab2 = st.tabs(["Update Tables", "View Data"])

with tab1:
    st.subheader("Update GTFS Data")
    
    # Option 1: Update all tables
    st.markdown("### Option 1: Update All Tables")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Download & Refresh All", type="primary"):
            load_all_gtfs_data(force_refresh=True)
    with col2:
        if st.button("Check for Updates (All)"):
            load_all_gtfs_data(force_refresh=False)
    
    # Option 2: Update specific table
    st.markdown("### Option 2: Update Specific Table")
    selected_table = st.selectbox(
        "Select table to update:",
        tables,
        format_func=lambda x: x.replace("gtfs_", "").capitalize()
    )
    
    if st.button(f"Update {selected_table.replace('gtfs_', '').capitalize()} Only", type="primary"):
        load_specific_gtfs_table(selected_table)

with tab2:
    st.subheader("Database Preview")
    
    # Use tabs for better organization
    preview_tabs = st.tabs([table.replace("gtfs_", "").capitalize() for table in tables])
    for i, tab in enumerate(preview_tabs):
        with tab:
            show_preview_from_postgres(tables[i])
