import pandas as pd
import requests
import zipfile
import io
import psycopg2
from psycopg2.extras import execute_values
import streamlit as st
import time
from datetime import datetime
import traceback

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

    db_columns = get_table_columns(table_name, conn)
    if not db_columns:
        st.error(f"Could not retrieve columns for {table_name}")
        return

    # Truncate the table before inserting
    truncate_table(table_name, conn)

    # Identify which columns are missing from the GTFS DataFrame
    missing_columns = set(db_columns) - set(df.columns)
    if missing_columns:
        st.info(f"Skipping these missing columns (defaulted in DB): {missing_columns}")

    # Use only the available columns for insert
    insert_columns = [col for col in db_columns if col not in missing_columns]

    # Prepare insert DataFrame with only matching columns
    insert_df = df[insert_columns].copy()

    # Replace NaN with None for SQL compatibility
    insert_df = insert_df.where(pd.notnull(insert_df), None)

    # Convert to list of tuples
    values = insert_df.values.tolist()

    # Build SQL insert statement
    insert_query = f"""
        INSERT INTO {table_name} ({', '.join(insert_columns)})
        VALUES %s
    """

    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, values)
            conn.commit()
        st.success(f"Inserted {len(values)} rows into {table_name}")
        st.success(f"({len(insert_columns)} columns inserted, {len(missing_columns)} defaulted in DB)")
    except Exception as e:
        st.error(f"Failed to insert into {table_name}: {e}")

def preview_supabase_tables(conn, table_names):
    for table in table_names:
        try:
            # Get column information to identify NULL values
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = '{table}'
                    ORDER BY ordinal_position
                """)
                column_info = cur.fetchall()
                
                # Get list of nullable columns
                nullable_columns = [col[0] for col in column_info if col[2] == 'YES']
                
                # Get row count
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cur.fetchone()[0]
                
                # Get NULL counts for each column
                null_counts = {}
                for col in nullable_columns:
                    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
                    null_count = cur.fetchone()[0]
                    if null_count > 0:
                        null_counts[col] = null_count
            
            # Fetch preview data
            query = f"SELECT * FROM {table} LIMIT 5"
            df = pd.read_sql(query, conn)
            
            # Display information
            st.subheader(f"Preview of {table} ({row_count} total rows)")
            st.dataframe(df)
            
            # Show NULL value statistics if any exist
            if null_counts:
                with st.expander(f"NULL value statistics for {table}"):
                    for col, count in null_counts.items():
                        percentage = (count / row_count) * 100
                        st.text(f"{col}: {count} NULL values ({percentage:.1f}%)")
            
        except Exception as e:
            st.error(f"Failed to query {table}: {e}")

def main():
    st.title("GTFS Loader and Supabase Preview")
    
    # Create tabs for better organization
    tab1, tab2 = st.tabs(["Load Data", "View Tables"])
    
    with tab1:
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
            st.error("No GTFS data available. Please upload a file or check the download URL.")
            return

        # Connect to database
        conn = get_pg_connection()
        if conn is None:
            return

        # Show available files in the ZIP
        with st.expander("Available files in GTFS package"):
            files = zip_obj.namelist()
            st.write(files)
            
            # Show warnings if expected files are missing
            missing_files = [file_mapping[table] for table in file_mapping if file_mapping[table] not in files]
            if missing_files:
                st.warning(f"The following expected files are missing from the GTFS package: {missing_files}")

        # Processing section
        st.header("Upload GTFS Data to Supabase")
        
        # Check table structure in database
        with st.expander("Database Table Structure"):
            for table_name in file_mapping.keys():
                try:
                    columns = get_table_columns(table_name, conn)
                    st.write(f"**{table_name}**: {columns}")
                except Exception as e:
                    st.error(f"Could not fetch structure for {table_name}: {e}")
        
        # Add option to select which tables to process
        selected_tables = st.multiselect(
            "Select tables to process",
            options=list(file_mapping.keys()),
            default=list(file_mapping.keys())
        )
        
        col1, col2 = st.columns(2)
        with col1:
            process_button = st.button("Process Selected Tables", type="primary")
        with col2:
            preview_before_upload = st.checkbox("Preview data before upload", value=True)
        
        if process_button:
            if preview_before_upload:
                # Show preview of data before uploading
                for table_name in selected_tables:
                    file_name = file_mapping[table_name]
                    df = load_gtfs_file(zip_obj, file_name)
                    
                    if not df.empty:
                        with st.expander(f"Preview of {file_name} data"):
                            st.write(f"First 5 rows of {len(df)} total:")
                            st.dataframe(df.head())
                            
                            # Show column matching information
                            db_columns = get_table_columns(table_name, conn)
                            common_cols = [col for col in df.columns if col in db_columns]
                            missing_cols = set(db_columns) - set(common_cols)
                            
                            st.write(f"**Column Matching for {table_name}:**")
                            st.write(f"- GTFS columns: {len(df.columns)}")
                            st.write(f"- Database columns: {len(db_columns)}")
                            st.write(f"- Matched columns: {len(common_cols)}")
                            st.write(f"- Missing Columns: {len(missing_cols)}")
                            
                            if missing_cols:
                                st.write("**Columns that are not in db:**", list(missing_cols))
                    
                # Add confirmation button
                if st.button("Confirm and Upload to Database"):
                    process_tables(selected_tables, zip_obj, conn)
            else:
                # Process immediately without preview
                process_tables(selected_tables, zip_obj, conn)
    
    with tab2:
        # Connect to database if not already connected
        if 'conn' not in locals() or conn is None:
            conn = get_pg_connection()
            if conn is None:
                st.error("Cannot connect to database")
                return
        
        # Preview section
        st.header("Supabase Table Previews")
        preview_supabase_tables(conn, file_mapping.keys())
        
        # Add option to download table data
        with st.expander("Download table data"):
            download_table = st.selectbox("Select table to download", options=list(file_mapping.keys()))
            if st.button("Generate CSV"):
                query = f"SELECT * FROM {download_table}"
                df = pd.read_sql(query, conn)
                
                if not df.empty:
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name=f"{download_table}.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("No data to download")

    # Always close connection
    if 'conn' in locals() and conn is not None:
        conn.close()

def process_tables(selected_tables, zip_obj, conn):
    """Process selected tables and show progress"""
    progress_bar = st.progress(0)
    progress_text = st.empty()
    
    for i, table_name in enumerate(selected_tables):
        progress_text.text(f"Processing {table_name}...")
        progress_bar.progress(int((i / len(selected_tables)) * 100))
        
        file_name = file_mapping[table_name]
        df = load_gtfs_file(zip_obj, file_name)
        store_dataframe_to_db(df, table_name, conn)
    
    progress_bar.progress(100)
    progress_text.text("Processing complete!")
    
    # Show success message with timestamp
    st.success(f"All tables processed successfully at {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Add option to view results
    if st.button("View Results"):
        st.experimental_rerun()

if __name__ == "__main__":
    main()
