import streamlit as st
import psycopg2
import traceback
import time

# --- Supabase PostgreSQL Credentials ---
PG_HOST = "eegejlqdgahlmtjniupz.supabase.co"
PG_PORT = 5432
PG_DB = "postgres"
PG_USER = "postgres"
PG_PASSWORD = "Supa1base!"  # Replace with your actual password

# --- Function to get connection with timeout ---
def get_pg_connection(timeout=5):
    st.info(f"Attempting to connect to {PG_HOST}...")
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
            sslmode="require",
            connect_timeout=timeout
        )
        return conn
    except psycopg2.OperationalError as e:
        st.error(f"Connection failed (operational error): {str(e)}")
        return None
    except Exception as e:
        st.error(f"Connection failed (unexpected error): {str(e)}")
        st.error(traceback.format_exc())
        return None

# --- Initialize session state ---
if 'connection_status' not in st.session_state:
    st.session_state.connection_status = None
    st.session_state.connection_time = None
    st.session_state.connection_message = None

# --- Streamlit App ---
st.title("Supabase PostgreSQL Connection Check")

# Add a connection timeout input
timeout = st.slider("Connection Timeout (seconds)", 3, 30, 10)

col1, col2 = st.columns(2)
with col1:
    if st.button("Check Connection", type="primary"):
        # Clear previous status
        st.session_state.connection_status = None
        st.session_state.connection_message = None
        
        # Show spinner while connecting
        with st.spinner("Connecting to database..."):
            start_time = time.time()
            try:
                conn = get_pg_connection(timeout)
                if conn:
                    # Test the connection by executing a simple query
                    cursor = conn.cursor()
                    cursor.execute("SELECT version();")
                    version = cursor.fetchone()[0]
                    cursor.close()
                    conn.close()
                    
                    # Store success information in session state
                    elapsed = time.time() - start_time
                    st.session_state.connection_status = "success"
                    st.session_state.connection_time = elapsed
                    st.session_state.connection_message = f"Connected successfully in {elapsed:.2f} seconds.\nPostgreSQL version: {version}"
                else:
                    st.session_state.connection_status = "error"
                    st.session_state.connection_message = "Connection failed. See error message above."
            except Exception as e:
                # Handle any unexpected exceptions
                st.session_state.connection_status = "error"
                st.session_state.connection_message = f"Unexpected error: {str(e)}\n{traceback.format_exc()}"

with col2:
    # Clear results button
    if st.button("Clear Results"):
        st.session_state.connection_status = None
        st.session_state.connection_message = None
        st.rerun()

# Display connection results
if st.session_state.connection_status == "success":
    st.success(st.session_state.connection_message)
elif st.session_state.connection_status == "error":
    st.error(st.session_state.connection_message)

# Show connection information
with st.expander("Connection Details"):
    st.write(f"**Host:** {PG_HOST}")
    st.write(f"**Port:** {PG_PORT}")
    st.write(f"**Database:** {PG_DB}")
    st.write(f"**User:** {PG_USER}")
    st.write("**Password:** ********")
    st.write(f"**SSL Mode:** require")
    
    # Show current status
    if st.session_state.connection_status is not None:
        st.write(f"**Status:** {st.session_state.connection_status}")
        if st.session_state.connection_time is not None:
            st.write(f"**Connection Time:** {st.session_state.connection_time:.2f} seconds")

# Troubleshooting section
st.subheader("Troubleshooting Tips")
st.markdown("""
If the connection fails, check the following:
1. **Network Connectivity**: Make sure your network allows connections to the database host
2. **Credentials**: Verify your username and password are correct
3. **IP Restrictions**: Check if your database has IP restrictions enabled
4. **Firewall Rules**: Ensure your firewall allows outgoing connections to port 5432
5. **SSL Requirements**: Some databases require SSL connections
""")
