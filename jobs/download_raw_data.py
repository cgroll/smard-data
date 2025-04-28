"""Download raw data from SMARD API.

This script downloads specified variables from the SMARD API and saves them as
parquet files in the raw data directory.
"""

from smard_data.paths import ProjPaths
from smard_data.config import Resolution, Region, Variable
from smard_data.api import download_smard_data
from datetime import datetime
from tqdm import tqdm
from typing import List


def download_variables(variables: List[int], output_path):
    """Download data for specified variables.
    
    Note: The SMARD API uses a block-based data retrieval system where we first get
    timestamps marking the start of data blocks (e.g. weekly chunks), then fetch
    the actual observations for each block in separate requests.
    
    Args:
        variables: List of variable IDs to download
        output_path: Path to the output directory for saving files
    """
    start_time = datetime(2000, 1, 1)

    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)

    # Iterate over specified variables with progress bar
    for variable_value in tqdm(variables):
        variable_name = Variable.get_name(variable_value)
        print(f"\nDownloading {variable_name} data...")
        df = download_smard_data(
            region=Region.DE.value,
            resolution=Resolution.QUARTER_HOUR.value,
            variable=variable_value,
            variable_name=variable_name,
            start_time=start_time
        )
        # Save to parquet file
        filename = f"{variable_name}_{Resolution.QUARTER_HOUR.value}.parquet"
        file_path = output_path / filename
        df.to_parquet(file_path)
        print(f"Saved data to {file_path}")


def download_generation():
    """Download all power generation variables."""
    paths = ProjPaths()
    download_variables(Variable.get_generation_variables(), paths.generation_raw_data_path)


def download_consumption():
    """Download all power consumption variables."""
    paths = ProjPaths()
    download_variables(Variable.get_consumption_variables(), paths.consumption_raw_data_path)


def download_prices():
    """Download all market price variables."""
    paths = ProjPaths()
    download_variables(Variable.get_price_variables(), paths.prices_raw_data_path)


def download_forecasts():
    """Download all forecast variables."""
    paths = ProjPaths()
    download_variables(Variable.get_forecast_variables(), paths.forecasts_raw_data_path)


def download_all():
    """Download all available variables."""
    paths = ProjPaths()
    download_variables([v.value for v in Variable], paths.raw_data_path)


def main():
    import sys
    if len(sys.argv) == 2:
        arg = sys.argv[1].lower()
        if arg == "generation":
            download_generation()
        elif arg == "consumption":
            download_consumption()
        elif arg == "prices":
            download_prices()
        elif arg == "forecasts":
            download_forecasts()
        elif arg == "all":
            download_all()
        else:
            print(f"Unknown argument: {arg}")
    else:
        download_all()


if __name__ == "__main__":
    main() 