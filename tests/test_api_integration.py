"""Integration tests for the SMARD API functions.

These tests make actual API calls and should be run with caution.
They may fail if the API is down or has changed.
"""

from datetime import datetime, timedelta
import pandas as pd
from smard_data.api import download_smard_data
from smard_data.config import Resolution, Region, Variable

def test_download_data():
    """Test downloading data with different parameters."""
    start_time = datetime.now() - timedelta(days=200)

    # Test combinations of parameters
    test_params = [
        # First combination
        {
            "region": Region.DE.value,
            "resolution": Resolution.HOUR.value,
            "variable": Variable.SOLAR.value,
            "variable_name": "solar"
        },
        # Second combination 
        {
            "region": Region.AT.value,
            "resolution": Resolution.DAY.value,
            "variable": Variable.BIOMASS.value,
            "variable_name": "biomass"
        }
    ]

    for params in test_params:
        df = download_smard_data(
            start_time=start_time,
            **params
        )

        # Basic data validation
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert params["variable_name"] in df.columns
        assert all(isinstance(idx, datetime) for idx in df.index)
        assert min(df.index) >= start_time
        assert max(df.index) <= datetime.now()

        # Data range validation
        values = df[params["variable_name"]]
        assert all(values >= 0)  # Power generation can't be negative
        assert all(values < 50000)  # Typical generation never exceeds 50GW

        if params["resolution"] == Resolution.HOUR.value:
            # Hourly data should have more granular entries
            assert len(df) > 24  # At least a day's worth of hourly data