"""Download raw data from SMARD API.

This script downloads all available variables from the SMARD API and saves them as
parquet files in the raw data directory. It is the first step in the data
pipeline.
"""

from smard_data.paths import ProjPaths
from smard_data.config import Resolution, Region, Variable
from smard_data.api import download_smard_data
from datetime import datetime
from tqdm import tqdm


def main():
    """Run the data download job.    
    """
    paths = ProjPaths()
    start_time = datetime(2000, 1, 1)

    # Create raw data directory if it doesn't exist
    paths.raw_data_path.mkdir(parents=True, exist_ok=True)

    # Iterate over all variables with progress bar
    for variable in tqdm(Variable):
        variable_value = variable.value
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
        output_path = paths.raw_data_path / filename
        df.to_parquet(output_path)
        print(f"Saved data to {output_path}")


if __name__ == "__main__":
    main() 