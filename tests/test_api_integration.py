"""Integration tests for the SMARD API functions.

These tests make actual API calls and should be run with caution.
They may fail if the API is down or has changed.
"""

import pytest
from datetime import datetime, timedelta
import pandas as pd
from smard_data.api import download_smard_data
from smard_data.config import Resolution, Region, Variable

def test_download_recent_solar_data():
    """Test downloading recent solar generation data."""
    df = download_smard_data(
        region=Region.DE.value,
        resolution=Resolution.HOUR.value,
        variable=Variable.SOLAR.value,
        variable_name="solar"
    )
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "solar" in df.columns
    assert all(isinstance(idx, datetime) for idx in df.index)
    
    # Verify data values are reasonable for solar
    assert all(df["solar"] >= 0)  # Solar generation can't be negative
    assert all(df["solar"] < 50000)  # Typical German solar never exceeds 50GW

def test_download_historical_data():
    """Test downloading historical data with start_time."""
    start_time = datetime.now() - timedelta(days=30)  # Get last 30 days
    
    df = download_smard_data(
        region=Region.DE.value,
        resolution=Resolution.DAY.value,
        variable=Variable.SOLAR.value,
        variable_name="solar",
        start_time=start_time
    )
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    
    # Verify we got data starting from our start_time
    assert min(df.index) >= start_time
    assert max(df.index) <= datetime.now()

def test_download_different_resolutions():
    """Test downloading data at different time resolutions."""
    start_time = datetime.now() - timedelta(days=7)  # Last week
    
    # Test hourly data
    df_hour = download_smard_data(
        region=Region.DE.value,
        resolution=Resolution.HOUR.value,
        variable=Variable.SOLAR.value,
        variable_name="solar",
        start_time=start_time
    )
    
    # Test daily data
    df_day = download_smard_data(
        region=Region.DE.value,
        resolution=Resolution.DAY.value,
        variable=Variable.SOLAR.value,
        variable_name="solar",
        start_time=start_time
    )
    
    # Hourly data should have ~24 times more entries than daily
    assert len(df_hour) > len(df_day)
    assert abs(len(df_hour) / len(df_day) - 24) < 5  # Allow some flexibility

def test_download_different_variables():
    """Test downloading different types of power data."""
    start_time = datetime.now() - timedelta(days=7)
    
    # Test solar
    df_solar = download_smard_data(
        region=Region.DE.value,
        resolution=Resolution.HOUR.value,
        variable=Variable.SOLAR.value,
        variable_name="solar",
        start_time=start_time
    )
    
    # Test wind offshore
    df_wind = download_smard_data(
        region=Region.DE.value,
        resolution=Resolution.HOUR.value,
        variable=Variable.WIND_OFFSHORE.value,
        variable_name="wind_offshore",
        start_time=start_time
    )
    
    assert len(df_solar) > 0
    assert len(df_wind) > 0
    assert "solar" in df_solar.columns
    assert "wind_offshore" in df_wind.columns

def test_download_different_regions():
    """Test downloading data for different regions."""
    start_time = datetime.now() - timedelta(days=7)
    
    # Test Germany
    df_de = download_smard_data(
        region=Region.DE.value,
        resolution=Resolution.HOUR.value,
        variable=Variable.SOLAR.value,
        variable_name="solar",
        start_time=start_time
    )
    
    # Test Austria
    df_at = download_smard_data(
        region=Region.AT.value,
        resolution=Resolution.HOUR.value,
        variable=Variable.SOLAR.value,
        variable_name="solar",
        start_time=start_time
    )
    
    assert len(df_de) > 0
    assert len(df_at) > 0
    # Data should be different for different regions
    assert not df_de["solar"].equals(df_at["solar"]) 