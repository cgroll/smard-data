"""Tests for the SMARD API functions."""

import pytest
from datetime import datetime
import pandas as pd
from unittest.mock import patch, Mock
from smard_data.api import download_smard_data
from smard_data.config import Resolution, Region, Variable

@pytest.fixture
def mock_timestamps_response():
    return {
        "timestamps": [
            1640995200000,  # 2022-01-01 00:00:00
            1641081600000,  # 2022-01-02 00:00:00
            1641168000000   # 2022-01-03 00:00:00
        ]
    }

@pytest.fixture
def mock_data_response():
    return {
        "series": [
            [1641024000000, 100.5],  # 2022-01-01 00:00:00
            [1641027600000, 200.7],  # 2022-01-01 01:00:00
        ]
    }

def test_download_smard_data_success(mock_timestamps_response, mock_data_response):
    """Test successful data download and processing."""
    with patch('requests.get') as mock_get:
        # Configure mock responses for multiple timestamp requests
        mock_get.side_effect = [
            Mock(status_code=200, json=lambda: mock_timestamps_response),
            Mock(status_code=200, json=lambda: mock_data_response),
            Mock(status_code=200, json=lambda: mock_data_response),
            Mock(status_code=200, json=lambda: mock_data_response)
        ]
        
        # Call function with test parameters
        df = download_smard_data(
            region=Region.DE.value,
            resolution=Resolution.HOUR.value,
            variable=Variable.SOLAR.value,
            variable_name="solar"
        )
        
        # Verify the result
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2  # After deduplication
        assert list(df.columns) == ["solar"]
        assert df.index.name == "timestamp"
        assert all(isinstance(idx, datetime) for idx in df.index)

def test_download_smard_data_with_start_time(mock_timestamps_response, mock_data_response):
    """Test data download with start_time parameter."""
    with patch('requests.get') as mock_get:
        mock_get.side_effect = [
            Mock(status_code=200, json=lambda: mock_timestamps_response),
            Mock(status_code=200, json=lambda: mock_data_response)
        ]
        
        start_time = datetime(2022, 1, 2)  # Should only get data from second timestamp
        df = download_smard_data(
            region=Region.DE.value,
            resolution=Resolution.HOUR.value,
            variable=Variable.SOLAR.value,
            variable_name="solar",
            start_time=start_time
        )
        
        # Verify only one timestamp request was made after filtering
        assert mock_get.call_count == 2
        assert isinstance(df, pd.DataFrame)

def test_download_smard_data_timestamp_error():
    """Test error handling when timestamp request fails."""
    with patch('requests.get') as mock_get:
        mock_get.return_value = Mock(status_code=404)
        
        with pytest.raises(RuntimeError) as exc_info:
            download_smard_data(
                region=Region.DE.value,
                resolution=Resolution.HOUR.value,
                variable=Variable.SOLAR.value,
                variable_name="solar"
            )
        
        assert "Error fetching timestamps: 404" in str(exc_info.value)

def test_download_smard_data_empty_timestamps(mock_timestamps_response):
    """Test error handling when no timestamps are available."""
    mock_timestamps_response["timestamps"] = []
    
    with patch('requests.get') as mock_get:
        mock_get.return_value = Mock(status_code=200, json=lambda: mock_timestamps_response)
        
        with pytest.raises(RuntimeError) as exc_info:
            download_smard_data(
                region=Region.DE.value,
                resolution=Resolution.HOUR.value,
                variable=Variable.SOLAR.value,
                variable_name="solar"
            )
        
        assert "No timestamps available" in str(exc_info.value)

def test_download_smard_data_partial_data_success(mock_timestamps_response, mock_data_response):
    """Test successful data collection when some requests fail."""
    with patch('requests.get') as mock_get:
        mock_get.side_effect = [
            Mock(status_code=200, json=lambda: mock_timestamps_response),
            Mock(status_code=200, json=lambda: mock_data_response),
            Mock(status_code=500),  # This request fails
            Mock(status_code=200, json=lambda: mock_data_response)
        ]
        
        df = download_smard_data(
            region=Region.DE.value,
            resolution=Resolution.HOUR.value,
            variable=Variable.SOLAR.value,
            variable_name="solar"
        )
        
        # Should still get data from successful requests
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

def test_download_smard_data_start_time_no_data(mock_timestamps_response):
    """Test error handling when no data is available after start_time."""
    with patch('requests.get') as mock_get:
        mock_get.return_value = Mock(status_code=200, json=lambda: mock_timestamps_response)
        
        future_date = datetime(2025, 1, 1)
        with pytest.raises(RuntimeError) as exc_info:
            download_smard_data(
                region=Region.DE.value,
                resolution=Resolution.HOUR.value,
                variable=Variable.SOLAR.value,
                variable_name="solar",
                start_time=future_date
            )
        
        assert "No data available after" in str(exc_info.value) 