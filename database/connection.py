import os
import streamlit as st
from supabase import create_client, Client
import logging

logger = logging.getLogger(__name__)

@st.cache_resource
def init_connection():
    """Initialize connection to Supabase database."""
    # Try environment variables first
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_API_KEY")
    
    # Fall back to Streamlit secrets if env vars not available
    if not url or not key:
        url = st.secrets.get("SUPABASE_URL", "")
        key = st.secrets.get("SUPABASE_API_KEY", "")
    
    if not url or not key:
        st.error("Supabase credentials not found! Please set SUPABASE_URL and SUPABASE_API_KEY in .env file or Streamlit secrets.")
        st.stop()
    
    return create_client(url, key)