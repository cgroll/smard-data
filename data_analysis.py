# %%
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from smard_data.config import Variable, Resolution
from smard_data.paths import ProjPaths

# Set consistent style for all plots
plt.style.use('seaborn-v0_8')
FIGSIZE_SINGLE = (12, 6)
FIGSIZE_WIDE = (15, 6)

# Color schemes
GENERATION_COLORS = {
    'SOLAR': '#FFD700',
    'WIND_OFFSHORE': '#4682B4',
    'WIND_ONSHORE': '#87CEEB',
    'BIOMASS': '#228B22',
    'HYDRO': '#00CED1',
    'OTHER_RENEWABLE': '#98FB98',
    'NUCLEAR': '#FFB6C1',
    'BROWN_COAL': '#8B4513',
    'HARD_COAL': '#A0522D',
    'NATURAL_GAS': '#DEB887',
    'OTHER_CONVENTIONAL': '#D2B48C',
    'PUMPED_STORAGE': '#9370DB'
}

# Category colors
CATEGORY_COLORS = {
    'Renewable': '#2ECC71',
    'Conventional': '#8B4513',
    'Storage': '#3498DB'
}

def load_generation_data():
    """Load and prepare generation data from all sources."""
    paths = ProjPaths()
    generation_vars = [Variable.get_name(var) for var in Variable.get_generation_variables()]
    
    # Get paths to generation data files
    data_files = []
    for var in generation_vars:
        file = paths.raw_data_path / f"{var}_quarterhour.parquet"
        if file.exists():
            data_files.append(file)

    if not data_files:
        raise RuntimeError("No generation data files found")

    # Load and concatenate generation files
    dfs = []
    for file in data_files:
        df_temp = pd.read_parquet(file)
        dfs.append(df_temp)

    # Combine all dataframes
    df_combined = pd.concat(dfs, axis=1)
    df_combined = df_combined * 4  # normalize to hourly frequency
    
    # Sort and clean data
    df_combined = df_combined.sort_index()
    df_combined = df_combined[~df_combined.index.duplicated(keep='first')]
    
    # Group columns by energy type
    renewable_cols = ['SOLAR', 'WIND_OFFSHORE', 'WIND_ONSHORE', 'BIOMASS', 'HYDRO', 'OTHER_RENEWABLE']
    conventional_cols = ['NUCLEAR', 'BROWN_COAL', 'HARD_COAL', 'NATURAL_GAS', 'OTHER_CONVENTIONAL']
    storage_cols = ['PUMPED_STORAGE']
    
    sorted_cols = renewable_cols + conventional_cols + storage_cols
    assert set(sorted_cols) == set(df_combined.columns), "Missing or extra columns detected"
    
    return df_combined[sorted_cols]

def load_consumption_data():
    """Load and prepare consumption/load data."""
    paths = ProjPaths()
    load_vars = [Variable.get_name(var) for var in Variable.get_consumption_variables()]
    
    load_files = []
    for var in load_vars:
        file = paths.raw_data_path / f"{var}_quarterhour.parquet"
        if file.exists():
            load_files.append(file)

    if not load_files:
        raise RuntimeError("No load data files found")

    load_dfs = []
    for file in load_files:
        df_temp = pd.read_parquet(file)
        load_dfs.append(df_temp)

    df_load = pd.concat(load_dfs, axis=1)
    df_load = df_load.sort_index()
    df_load = df_load[~df_load.index.duplicated(keep='first')]
    
    return df_load

def style_plot(fig, ax, title, xlabel, ylabel, annotation=None):
    """Apply consistent styling to plots."""
    ax.set_title(title, pad=20, fontsize=14)
    ax.set_xlabel(xlabel, fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.grid(True, alpha=0.3)
    
    if annotation:
        ax.text(0.02, 0.98, annotation, 
                transform=ax.transAxes,
                fontsize=10,
                verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

# %% [markdown]
# # Power Demand Analysis for Germany
# Analysis of power demand patterns and generation mix in Germany.

# %% [markdown]
# ## 1. Power Demand Overview

# %%
# Load the data
df_load = load_consumption_data()

def plot_demand_overview():
    """Create overview plots of power demand."""
    # Yearly total load trend
    fig, ax = plt.subplots(figsize=FIGSIZE_SINGLE)
    monthly_load = df_load['TOTAL_LOAD'].resample('M').mean()
    monthly_load.plot(ax=ax)
    
    annotation = (f"Average monthly load ranges from {monthly_load.min():,.0f} MW to {monthly_load.max():,.0f} MW\n"
                 f"Overall trend shows {'increasing' if monthly_load.iloc[-1] > monthly_load.iloc[0] else 'decreasing'} demand")
    
    style_plot(fig, ax, 
              'Monthly Average Power Demand in Germany',
              'Year', 
              'Average Load (MW)',
              annotation)
    plt.show()
    
    # Monthly pattern
    fig, ax = plt.subplots(figsize=FIGSIZE_SINGLE)
    monthly_load = df_load.groupby(df_load.index.month)['TOTAL_LOAD'].mean()
    
    ax.plot(range(1,13), monthly_load.values, marker='o')
    ax.set_xticks(range(1,13))
    ax.set_xticklabels(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
    
    annotation = ("Clear seasonal pattern:\n"
                 "Peak average demand in winter months\n" 
                 "Lower average demand during summer months")
    
    style_plot(fig, ax,
              'Average Monthly Power Demand Pattern',
              'Month',
              'Average Load (MW)',
              annotation)
    plt.show()
    
    # Daily pattern
    fig, ax = plt.subplots(figsize=FIGSIZE_SINGLE)
    hourly_load = df_load.groupby(df_load.index.hour)['TOTAL_LOAD'].mean()
    
    ax.plot(hourly_load.index, hourly_load.values, marker='o')
    ax.set_xticks(range(0, 24, 2))
    ax.set_xticklabels([f"{hour:02d}:00" for hour in range(0, 24, 2)])
    
    annotation = ("Daily demand pattern:\n"
                 "Peak demand periods around noon and early evening (after office hours)\n"
                 "Lowest demand in the middle of the night when most people are sleeping")
    
    style_plot(fig, ax,
              'Average Daily Power Demand Pattern',
              'Hour of Day',
              'Average Load (MW)',
              annotation)
    plt.show()

plot_demand_overview()

# %% [markdown]
# ## 2. Power Generation Mix Evolution

# %%
# Load generation data
df_combined = load_generation_data()

def plot_generation_mix_evolution():
    """Plot the evolution of the generation mix over time."""
    # Calculate monthly averages
    monthly_gen = df_combined.resample('M').mean()
    
    # Absolute generation
    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)
    monthly_gen.plot(kind='area', stacked=True, ax=ax, color=[GENERATION_COLORS[col] for col in monthly_gen.columns])
    
    annotation = ("Key observations:\n"
                 "- Increasing renewable contribution\n"
                 "- Seasonal patterns in solar and wind\n"
                 "- Declining nuclear generation")
    
    style_plot(fig, ax,
              'Evolution of Power Generation Mix',
              'Year',
              'Power Generation (MW)',
              annotation)
    
    plt.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=4)
    plt.tight_layout()
    plt.show()
    
    # Percentage contribution
    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)
    monthly_gen_pct = monthly_gen.div(monthly_gen.sum(axis=1), axis=0) * 100
    monthly_gen_pct.plot(kind='area', stacked=True, ax=ax, color=[GENERATION_COLORS[col] for col in monthly_gen_pct.columns])
    
    annotation = ("Relative contribution changes:\n"
                 "Steady increase in renewable share over time, particularly from wind and solar sources")
    style_plot(fig, ax,
              'Relative Share of Power Generation Sources',
              'Year',
              'Percentage of Total Generation',
              annotation)
    
    plt.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=4)
    plt.tight_layout()
    plt.show()

plot_generation_mix_evolution()

# %% [markdown]
# ## 3. Renewable Generation Patterns

# %%
def plot_renewable_patterns():
    """Analyze and plot renewable generation patterns."""
    # Solar generation patterns
    fig, axes = plt.subplots(2, 1, figsize=(12, 12))
    
    # Monthly solar pattern
    monthly_solar = df_combined['SOLAR'].groupby(df_combined.index.month).mean()
    axes[0].plot(range(1,13), monthly_solar.values, marker='o', color=GENERATION_COLORS['SOLAR'])
    axes[0].set_xticks(range(1,13))
    axes[0].set_xticklabels(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                            'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
    axes[0].set_ylim(bottom=0)  # Set y-axis to start at 0
    
    annotation_solar = ("Peak solar generation during summer months\n"
                       "Minimum generation during winter months")
    
    style_plot(fig, axes[0],
              'Average Solar Generation by Month',
              'Month',
              'Power (MW)',
              annotation_solar)
    
    # Hourly solar pattern
    hourly_solar = df_combined['SOLAR'].groupby(df_combined.index.hour).mean()
    axes[1].plot(hourly_solar.index, hourly_solar.values, marker='o', color=GENERATION_COLORS['SOLAR'])
    axes[1].set_xticks(range(0, 24, 2))
    axes[1].set_xticklabels([f"{hour:02d}:00" for hour in range(0, 24, 2)])
    axes[1].set_ylim(bottom=0)  # Set y-axis to start at 0
    
    annotation_hourly = ("Peak generation around noon\n"
                        "Zero generation during night hours")
    
    style_plot(fig, axes[1],
              'Average Solar Generation by Hour',
              'Hour of Day',
              'Power (MW)',
              annotation_hourly)
    
    plt.tight_layout()
    plt.show()
    
    # Wind generation patterns
    df_combined['WIND_TOTAL'] = df_combined['WIND_OFFSHORE'] + df_combined['WIND_ONSHORE']
    
    fig, axes = plt.subplots(2, 1, figsize=(12, 12))
    
    # Monthly wind pattern
    monthly_wind = df_combined['WIND_TOTAL'].groupby(df_combined.index.month).mean()
    axes[0].plot(range(1,13), monthly_wind.values, marker='o', color='blue')
    axes[0].set_xticks(range(1,13))
    axes[0].set_xticklabels(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                            'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
    axes[0].set_ylim(bottom=0)  # Set y-axis to start at 0
    annotation_wind = ("Highest wind generation in winter months\n"
                      "Lowest in summer months")
    
    style_plot(fig, axes[0],
              'Average Wind Generation by Month',
              'Month',
              'Power (MW)',
              annotation_wind)
    
    # Hourly wind pattern
    hourly_wind = df_combined['WIND_TOTAL'].groupby(df_combined.index.hour).mean()
    axes[1].plot(hourly_wind.index, hourly_wind.values, marker='o', color='blue')
    axes[1].set_xticks(range(0, 24, 2))
    axes[1].set_xticklabels([f"{hour:02d}:00" for hour in range(0, 24, 2)])
    axes[1].set_ylim(bottom=0)  # Set y-axis to start at 0
    
    annotation_hourly = ("Slightly lower generation before/around noon\n"
                        "Relatively stable throughout the day with only slight variations")
    style_plot(fig, axes[1],
              'Average Wind Generation by Hour',
              'Hour of Day',
              'Power (MW)',
              annotation_hourly)
    
    plt.tight_layout()
    plt.show()

plot_renewable_patterns()

# %% [markdown]
# ## 4. Grid Stability Analysis

# %%
def plot_grid_stability():
    """Analyze and plot grid stability metrics."""
    # Calculate residual load percentage
    residual_pct = (df_load['RESIDUAL_LOAD'] / df_load['TOTAL_LOAD'] * 100).clip(lower=0)
    
    # Monthly maximum residual load
    fig, ax = plt.subplots(figsize=FIGSIZE_SINGLE)
    monthly_max_residual = residual_pct.resample('M').max()
    
    ax.plot(monthly_max_residual.index, monthly_max_residual.values)
    
    annotation = (f"Average maximum residual load: {monthly_max_residual.mean():.1f}%\n"
                 f"Highest recorded: {monthly_max_residual.max():.1f}%\n"
                 "Trend shows decreasing reliance on conventional sources")
    
    style_plot(fig, ax,
              'Monthly Maximum Residual Load Percentage',
              'Date',
              'Residual Load Percentage',
              annotation)
    plt.show()
    
    # Distribution of residual load
    fig, ax = plt.subplots(figsize=FIGSIZE_SINGLE)
    ax.hist(residual_pct, bins=50, edgecolor='black')
    
    annotation = (f"Mean: {residual_pct.mean():.1f}%\n"
                 f"Median: {residual_pct.median():.1f}%\n"
                 f"Std Dev: {residual_pct.std():.1f}%")
    
    style_plot(fig, ax,
              'Distribution of Residual Load Percentage',
              'Residual Load Percentage',
              'Frequency',
              annotation)
    plt.show()

plot_grid_stability()

# %%
