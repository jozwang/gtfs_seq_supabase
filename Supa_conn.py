import streamlit as st
import psycopg2
import traceback
import time
import os
from urllib.parse import urlparse

# --- Supabase Connection Options ---
# Option 1: Connection URL with service role (preferred)
# SUPABASE_URL = "postgresql://postgres.eegejlqdgahlmtjniupz:Supa1base!@aws-0-us-west-1.pooler.supabase.com:5432/postgres"
# Note: Replace the password in the URL with your actual service role key or use environment variables

# Option 2: Connection string with service role from environment variables
SUPABASE_URL = st.secrets.get("SUPABASE_DATABASE_URL")
#os.environ.get("SUPABASE_DATABASE_URL", "")

# --- Function to get connection with timeout ---
def get_pg_connection(timeout=5):
    st.info(f"Attempting to connect to Supabase database...")
    try:
        # Parse the connection URL to display host without sensitive info
        parsed_url = urlparse(SUPABASE_URL)
        display_host = f"{parsed_url.scheme}://{parsed_url.username}@{parsed_url.hostname}"
        st.info(f"Connecting to {display_host}...")
        
        # Connect using the connection URL
        conn = psycopg2.connect(
            SUPABASE_URL,
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
    # Parse and display connection info from URL
    try:
        parsed_url = urlparse(SUPABASE_URL)
        st.write(f"**Connection Type:** URL-based with Service Role")
        st.write(f"**Host:** {parsed_url.hostname}")
        st.write(f"**Port:** {parsed_url.port or 5432}")
        st.write(f"**Database:** {parsed_url.path.strip('/')}")
        st.write(f"**User:** {parsed_url.username}")
        st.write("**Password:** ********")
    except Exception:
        st.write("Error parsing connection URL")
    
    # Show current status
    if st.session_state.connection_status is not None:
        st.write(f"**Status:** {st.session_state.connection_status}")
        if st.session_state.connection_time is not None:
            st.write(f"**Connection Time:** {st.session_state.connection_time:.2f} seconds")

# Troubleshooting section
st.subheader("Troubleshooting Tips")
st.markdown("""
If the connection fails, check the following:
1. **Service Role API Key**: Make sure you're using the correct service role API key (not the anon public key)
2. **Connection URL Format**: Verify your connection URL is formatted correctly
3. **Network Connectivity**: Ensure your network allows connections to the Supabase database
4. **IP Restrictions**: Check if your database has IP allow lists enabled in the Supabase dashboard
5. **Environment Variables**: If using environment variables, confirm they're properly set
6. **SSL Requirements**: Supabase requires SSL connections (this is handled automatically in the connection URL)

**For Supabase Service Roles:**
- Service roles have higher privileges than anonymous access
- Service role keys should be kept secure and not exposed in client-side code
- Consider storing the service role key in environment variables
""")

# Add a section for environment variable setup
with st.expander("Using Environment Variables (Recommended)"):
    st.markdown("""
    For better security, store your connection URL in environment variables:
    
    **Local Development:**
    ```bash
    # Linux/MacOS
    export SUPABASE_DATABASE_URL="postgresql://postgres.yourprojectref:yourpassword@aws-0-region.pooler.supabase.com:5432/postgres"
    
    # Windows (Command Prompt)
    set SUPABASE_DATABASE_URL=postgresql://postgres.yourprojectref:yourpassword@aws-0-region.pooler.supabase.com:5432/postgres
    
    # Windows (PowerShell)
    $env:SUPABASE_DATABASE_URL="postgresql://postgres.yourprojectref:yourpassword@aws-0-region.pooler.supabase.com:5432/postgres"
    ```
    
    **Streamlit Cloud:**
    Add the environment variable in your app settings.
    
    **Code Changes:**
    ```python
    import os
    SUPABASE_URL = os.environ.get("SUPABASE_DATABASE_URL", "")
    ```
    """)

# Add a section explaining how to get the connection URL from Supabase
with st.expander("How to get your Supabase Connection URL"):
    st.markdown("""
    To find your Supabase connection URL with service role privileges:

    1. Log in to your Supabase dashboard
    2. Select your project
    3. Go to Project Settings > Database
    4. Look for "Connection string" or "URI" section
    5. Choose "URI" format and select "Service role" (not the public anon key)
    6. Copy the entire connection string
    
    Example format:
    ```
    postgresql://postgres.[project-ref]:[service-role-password]@aws-0-[region].pooler.supabase.com:5432/postgres
    ```
    
    Note: Using the pooler URL (with aws-0-region.pooler.supabase.com) is recommended for better connection management.
    """)
