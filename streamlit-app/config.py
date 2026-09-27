"""Configuration du dashboard Streamlit — aucun secret en dur."""
import os

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
REQUEST_TIMEOUT = float(os.getenv("API_TIMEOUT_SECONDS", "10"))
