"""
Scottish Equity Risk Pipeline - Streamlit Dashboard
File: dashboard/streamilit_app.py
"""

import streamlit as st
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

