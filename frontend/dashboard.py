import streamlit as st
import requests
import pandas as pd
import time
import os
from datetime import datetime

# --- Configuration ---
FASTAPI_BASE_URL = os.getenv("FASTAPI_BASE_URL", "http://localhost:8000")
REFRESH_INTERVAL_SECONDS = 10 # How often the dashboard will refresh its data

st.set_page_config(
    page_title="Solana Liquidity Sniper Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Helper Functions for API Calls ---
@st.cache_data(ttl=REFRESH_INTERVAL_SECONDS)
def get_recent_transactions(limit: int = 100):
    try:
        response = requests.get(f"{FASTAPI_BASE_URL}/transactions/recent", params={"limit": limit})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching recent transactions: {e}")
        return []

@st.cache_data(ttl=REFRESH_INTERVAL_SECONDS)
def get_event_type_counts():
    try:
        response = requests.get(f"{FASTAPI_BASE_URL}/analytics/event_type_counts")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching event type counts: {e}")
        return []

@st.cache_data(ttl=REFRESH_INTERVAL_SECONDS)
def get_program_id_counts():
    try:
        response = requests.get(f"{FASTAPI_BASE_URL}/analytics/program_id_counts")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching program ID counts: {e}")
        return []

# --- Dashboard Layout ---
st.title("📈 Solana Liquidity Sniper Dashboard")

# Auto-refresh mechanism
st_autorefresh_runner = st.empty()
if st_autorefresh_runner.button("Refresh Now"):
    st.cache_data.clear() # Clear cache on manual refresh
    st_autorefresh_runner.write(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}")

st.sidebar.header("Configuration")
st.sidebar.text(f"FastAPI URL: {FASTAPI_BASE_URL}")
st.sidebar.text(f"Refresh Interval: {REFRESH_INTERVAL_SECONDS} seconds")

# --- Display Recent Transactions ---
st.header("Recent Transactions")
transactions_data = get_recent_transactions(limit=50)
if transactions_data:
    df_transactions = pd.DataFrame(transactions_data)
    # Convert block_time to datetime objects for better display/sorting if needed
    df_transactions['block_time'] = pd.to_datetime(df_transactions['block_time'])
    # Select and reorder columns for display
    display_columns = [
        "block_time", "transaction_signature", "program_id", "event_type",
        "token_mint_address_in", "amount_in", "token_mint_address_out", "amount_out",
        "signer_address"
    ]
    # Handle cases where columns might be missing or in different order
    present_columns = [col for col in df_transactions.columns if col in display_columns]
    df_transactions = df_transactions[present_columns]

    st.dataframe(df_transactions, use_container_width=True)
else:
    st.info("No recent transactions to display. Ensure FastAPI and data pipeline are running.")

st.markdown("---")

# --- Display Analytics ---
col1, col2 = st.columns(2)

with col1:
    st.header("Event Type Distribution")
    event_counts = get_event_type_counts()
    if event_counts:
        df_event_counts = pd.DataFrame(event_counts)
        st.bar_chart(df_event_counts, x="name", y="count")
    else:
        st.info("No event type data to display.")

with col2:
    st.header("Program ID Distribution")
    program_counts = get_program_id_counts()
    if program_counts:
        df_program_counts = pd.DataFrame(program_counts)
        st.bar_chart(df_program_counts, x="name", y="count")
    else:
        st.info("No program ID data to display.")

# Footer
st.markdown("---")
st.caption(f"Data refreshed every {REFRESH_INTERVAL_SECONDS} seconds. Built with Streamlit, FastAPI, Kafka, PostgreSQL, and Solana.")

# Force rerun for auto-refresh
time.sleep(REFRESH_INTERVAL_SECONDS)
st.rerun()
