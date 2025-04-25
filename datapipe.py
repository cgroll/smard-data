"""Command line interface for SMARD data pipeline.

This module provides a CLI to run the data pipeline steps in sequence.
Each step checks if its output already exists before running.
"""

import click
from pathlib import Path
from typing import List
from smard_data.config import Variable, Resolution
from smard_data.paths import ProjPaths


def get_output_files(job_name: str) -> List[Path]:
    """Get expected output files for a job."""
    paths = ProjPaths()
    
    if job_name == "download-generation":
        return [
            paths.raw_data_path / f"{Variable.get_name(var)}_{Resolution.QUARTER_HOUR.value}.parquet"
            for var in Variable.get_generation_variables()
        ]
    elif job_name == "download-consumption":
        return [
            paths.raw_data_path / f"{Variable.get_name(var)}_{Resolution.QUARTER_HOUR.value}.parquet"
            for var in Variable.get_consumption_variables()
        ]
    elif job_name == "download-prices":
        return [
            paths.raw_data_path / f"{Variable.get_name(var)}_{Resolution.QUARTER_HOUR.value}.parquet"
            for var in Variable.get_price_variables()
        ]
    elif job_name == "download-forecasts":
        return [
            paths.raw_data_path / f"{Variable.get_name(var)}_{Resolution.QUARTER_HOUR.value}.parquet"
            for var in Variable.get_forecast_variables()
        ]
    elif job_name == "download-all":
        return [
            paths.raw_data_path / f"{Variable.get_name(var.value)}_{Resolution.QUARTER_HOUR.value}.parquet"
            for var in Variable
        ]
    return []


def check_outputs(job_name: str) -> bool:
    """Check if all output files for a job exist."""
    return all(f.exists() for f in get_output_files(job_name))


@click.group()
def cli():
    """SMARD data pipeline CLI.
    
    Run individual steps or all steps in sequence.
    Steps will be skipped if their output files already exist.
    """
    pass


@cli.group()
def download():
    """Download raw data from SMARD API."""
    pass


@download.command()
@click.option('--force', is_flag=True, help='Force run even if outputs exist')
def generation(force: bool):
    """Download power generation data."""
    if not force and check_outputs("download-generation"):
        click.echo("Generation data files already exist. Use --force to redownload.")
        return
        
    click.echo("Downloading power generation data...")
    from jobs.download_raw_data import download_generation
    download_generation()


@download.command()
@click.option('--force', is_flag=True, help='Force run even if outputs exist')
def consumption(force: bool):
    """Download power consumption data."""
    if not force and check_outputs("download-consumption"):
        click.echo("Consumption data files already exist. Use --force to redownload.")
        return
        
    click.echo("Downloading power consumption data...")
    from jobs.download_raw_data import download_consumption
    download_consumption()


@download.command()
@click.option('--force', is_flag=True, help='Force run even if outputs exist')
def prices(force: bool):
    """Download market price data."""
    if not force and check_outputs("download-prices"):
        click.echo("Price data files already exist. Use --force to redownload.")
        return
        
    click.echo("Downloading market price data...")
    from jobs.download_raw_data import download_prices
    download_prices()


@download.command()
@click.option('--force', is_flag=True, help='Force run even if outputs exist')
def forecasts(force: bool):
    """Download forecast data."""
    if not force and check_outputs("download-forecasts"):
        click.echo("Forecast data files already exist. Use --force to redownload.")
        return
        
    click.echo("Downloading forecast data...")
    from jobs.download_raw_data import download_forecasts
    download_forecasts()


@download.command()
@click.option('--force', is_flag=True, help='Force run even if outputs exist')
def all(force: bool):
    """Download all available data."""
    if not force and check_outputs("download-all"):
        click.echo("All data files already exist. Use --force to redownload.")
        return
        
    click.echo("Downloading all data...")
    from jobs.download_raw_data import download_all
    download_all()


@cli.command()
@click.option('--force', is_flag=True, help='Force run all steps')
def pipeline(force: bool):
    """Run all pipeline steps in sequence."""
    # Run full download
    all(force=force)
    # Future steps would be added here


if __name__ == "__main__":
    cli() 