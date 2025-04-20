import streamlit as st
import psycopg2

# --- Supabase PostgreSQL Credentials ---
PG_HOST = "eegejlqdgahlmtjniupz.supabase.co"
PG_PORT = 5432
PG_DB = "postgres"
PG_USER = "postgres"
PG_PASSWORD = "Supa1base!"  # Replace with your actual password

# --- Function to get connection ---
def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode="require"
    )

# --- Streamlit App ---
st.title("Supabase PostgreSQL Connection Check")

if st.button("Check Connection"):
    try:
        conn = get_pg_connection()
        conn.close()
        st.success("Successfully connected to Supabase PostgreSQL database.")
    except Exception as e:
        st.error(f"Connection failed: {e}")
