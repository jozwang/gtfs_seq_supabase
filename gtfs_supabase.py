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
SUPABASE_URL = st.secrets.get("SUPABASE_DATABASE_URL")
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

def create_tables_if_not_exist():
    conn = get_pg_connection()
    cursor = conn.cursor()
    try:
        # Create tables if they don't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gtfs_routes (
                route_id TEXT PRIMARY KEY,
                agency_id TEXT,
                route_short_name TEXT,
                route_long_name TEXT,
                route_desc TEXT,
                route_type TEXT,
                route_url TEXT,
                route_color TEXT,
                route_text_color TEXT
            );
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gtfs_stops (
                stop_id TEXT PRIMARY KEY,
                stop_code TEXT,
                stop_name TEXT,
                stop_desc TEXT,
                stop_lat TEXT,
                stop_lon TEXT,
                zone_id TEXT,
                stop_url TEXT,
                location_type TEXT,
                parent_station TEXT,
                stop_timezone TEXT,
                wheelchair_boarding TEXT,
                platform_code TEXT,
                region TEXT
            );
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gtfs_trips (
                route_id TEXT,
                service_id TEXT,
                trip_id TEXT PRIMARY KEY,
                trip_headsign TEXT,
                trip_short_name TEXT,
                direction_id TEXT,
                block_id TEXT,
                shape_id TEXT,
                wheelchair_accessible TEXT,
                bikes_allowed TEXT
            );
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gtfs_stop_times (
                trip_id TEXT,
                arrival_time TEXT,
                departure_time TEXT,
                stop_id TEXT,
                stop_sequence TEXT,
                stop_headsign TEXT,
                pickup_type TEXT,
                drop_off_type TEXT,
                shape_dist_traveled TEXT,
                timepoint TEXT,
                PRIMARY KEY (trip_id, stop_sequence)
            );
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gtfs_shapes (
                shape_id TEXT,
                shape_pt_lat TEXT,
                shape_pt_lon TEXT,
                shape_pt_sequence TEXT,
                shape_dist_traveled TEXT,
                PRIMARY KEY (shape_id, shape_pt_sequence)
            );
        """)
        
        conn.commit()
    except Exception as e:
        st.error(f"Error creating tables: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

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

def load_gtfs_data(force_refresh=False):
    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = None

    brisbane_tz = pytz.timezone("Australia/Brisbane")
    now = datetime.now(brisbane_tz)
    
    # Set refresh time to 1 AM
    refresh_time = datetime.combine(now.date(), time(1, 0)).replace(tzinfo=brisbane_tz)

    # Create tables first
    create_tables_if_not_exist()

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
        
        store_to_postgres("gtfs_stop_times", stop_times_df)
        progress_bar.progress(95)
        
        store_to_postgres("gtfs_shapes", shapes_df)
        progress_bar.progress(100)
        
        # Update last refresh time
        st.session_state.last_refresh = now
        
        # Display completion message
        status_text.text("Data refresh completed successfully!")
        
        # Clear progress bar after 3 seconds
        time.sleep(3)
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

col1, col2 = st.columns(2)
with col1:
    if st.button("Download & Refresh Now", type="primary"):
        load_gtfs_data(force_refresh=True)
with col2:
    if st.button("Check for Updates"):
        load_gtfs_data(force_refresh=False)

# --- Show Latest Preview from DB ---
st.subheader("Database Preview")
tables = ["gtfs_routes", "gtfs_stops", "gtfs_trips", "gtfs_stop_times", "gtfs_shapes"]

# Use tabs for better organization
tabs = st.tabs([table.replace("gtfs_", "").capitalize() for table in tables])
for i, tab in enumerate(tabs):
    with tab:
        show_preview_from_postgres(tables[i])
