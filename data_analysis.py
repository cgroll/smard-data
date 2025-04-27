# %%
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from smard_data.config import Variable
from smard_data.paths import ProjPaths

# Set consistent style for all plots
plt.style.use('seaborn-v0_8')
FIGSIZE = (12, 6)  # Single standard figure size for all plots

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
    fig, ax = plt.subplots(figsize=FIGSIZE)
    monthly_load = df_load['TOTAL_LOAD'].resample('ME').sum() / 1000  # Convert to GWh
    monthly_load = monthly_load.iloc[:-1]
    monthly_load.plot(ax=ax)
    
    annotation = (f"Aggregated monthly load ranges from {monthly_load.min():,.0f} GWh to {monthly_load.max():,.0f} GWh\n"
                 f"Overall trend shows {'increasing' if monthly_load.iloc[-1] > monthly_load.iloc[0] else 'decreasing'} demand")
    
    style_plot(fig, ax, 
              'Monthly Power Demand in Germany',
              'Year', 
              'Total Load (GWh)',
              annotation)
    plt.show()
    
    # Monthly pattern
    fig, ax = plt.subplots(figsize=FIGSIZE)
    monthly_load = df_load.groupby(df_load.index.month)['TOTAL_LOAD'].mean()
    
    ax.plot(range(1,13), monthly_load.values, marker='o')
    ax.set_xticks(range(1,13))
    ax.set_xticklabels(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
    
    annotation = ("Clear seasonal pattern:\n"
                 "Peak average demand in winter months\n" 
                 "Lower average demand during summer months")
    
    style_plot(fig, ax,
              'Average Monthly Power Demand Pattern (hourly)',
              'Month',
              'Average Load (MWh)',
              annotation)
    plt.show()
    
    # Daily pattern
    fig, ax = plt.subplots(figsize=FIGSIZE)
    hourly_load = df_load.groupby(df_load.index.hour)['TOTAL_LOAD'].mean()
    
    ax.plot(hourly_load.index, hourly_load.values, marker='o')
    ax.set_xticks(range(0, 24, 2))
    ax.set_xticklabels([f"{hour:02d}:00" for hour in range(0, 24, 2)])
    
    annotation = ("Daily demand pattern:\n"
                 "Peak demand periods around noon and early evening (after office hours)\n"
                 "Lowest demand in the middle of the night when most people are sleeping")
    
    style_plot(fig, ax,
              'Average Daily Power Demand Pattern (hourly)',
              'Hour of Day',
              'Average Load (MWh)',
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
    # Calculate monthly averages (scaling 15-min values to hourly MWh)
    monthly_gen = df_combined.resample('ME').mean()
    
    # Absolute generation
    fig, ax = plt.subplots(figsize=FIGSIZE)
    monthly_gen.plot(kind='area', stacked=True, ax=ax, color=[GENERATION_COLORS[col] for col in monthly_gen.columns])
    
    annotation = ("Key observations:\n"
                 "- Increasing renewable contribution\n"
                 "- Seasonal patterns in solar and wind\n"
                 "- Declining nuclear generation\n"
                 "Note: Values shown are hourly averages calculated from 15-minute intervals")
    
    style_plot(fig, ax,
              'Evolution of Power Generation Mix',
              'Year',
              'Power Generation (MWh)',
              annotation)
    
    plt.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=4)
    plt.show()
    
    # Percentage contribution
    fig, ax = plt.subplots(figsize=FIGSIZE)
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
    plt.show()

plot_generation_mix_evolution()

# %% [markdown]
# ## 3. Renewable Generation Patterns

# %%
def plot_renewable_patterns():
    """Analyze and plot renewable generation patterns."""
    # Solar generation patterns
    fig, axes = plt.subplots(2, 1, figsize=FIGSIZE)
    
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
              'Power (MWh)',
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
              'Power (MWh)',
              annotation_hourly)
    
    plt.show()
    
    # Wind generation patterns
    df_combined['WIND_TOTAL'] = df_combined['WIND_OFFSHORE'] + df_combined['WIND_ONSHORE']
    
    fig, axes = plt.subplots(2, 1, figsize=FIGSIZE)
    
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
              'Power (MWh)',
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
              'Power (MWh)',
              annotation_hourly)
    
    plt.show()

plot_renewable_patterns()

# %% [markdown]
# ## 4. Grid Stability Analysis

# %%
def plot_grid_stability():
    """Analyze and plot grid stability metrics."""
    # Calculate residual load percentage
    residual_pct = (df_load['RESIDUAL_LOAD'] / df_load['TOTAL_LOAD'] * 100).clip(lower=0)
    
    # Monthly maximum and mean residual load
    fig, ax = plt.subplots(figsize=FIGSIZE)
    monthly_max_residual = residual_pct.resample('ME').max()
    monthly_mean_residual = residual_pct.resample('ME').mean()
    
    ax.plot(monthly_max_residual.index, monthly_max_residual.values, label='Maximum')
    ax.plot(monthly_mean_residual.index, monthly_mean_residual.values, label='Mean')
    ax.legend()
    ax.set_ylim(bottom=0)  # Set y-axis to start at 0
    
    annotation = ("Despite decreasing average reliance on conventional sources,\n"
                 "each month still has peak periods where nearly all power\n"
                 "must come from conventional generation.\n"
                 "The overall trend shows gradual improvement in renewable integration")
    style_plot(fig, ax,
              'Monthly Maximum and Mean Residual Load Percentage',
              'Date',
              'Residual Load Percentage',
              annotation)
    plt.show()
    
    # Get second and second last full years
    years = residual_pct.index.year.unique()
    second_year = years[1]
    second_last_year = years[-2]
    
    # Plot distribution for second year
    fig, ax = plt.subplots(figsize=FIGSIZE)
    year_data = residual_pct[residual_pct.index.year == second_year]
    ax.hist(year_data, bins=50, edgecolor='black')
    
    annotation = (f"Year {second_year}\n"
                 "Distribution shows how often different levels of conventional\n"
                 "generation are needed to meet demand throughout the year")
    
    style_plot(fig, ax,
              f'Distribution of Residual Load Percentage ({second_year})',
              'Residual Load Percentage',
              'Frequency',
              annotation)
    plt.show()
    
    # Plot distribution for second last year
    fig, ax = plt.subplots(figsize=FIGSIZE)
    year_data = residual_pct[residual_pct.index.year == second_last_year]
    ax.hist(year_data, bins=50, edgecolor='black')
    
    annotation = (f"Year {second_last_year}\n"
                 "Distribution shows how often different levels of conventional\n"
                 "generation are needed to meet demand throughout the year")
    
    style_plot(fig, ax,
              f'Distribution of Residual Load Percentage ({second_last_year})',
              'Residual Load Percentage', 
              'Frequency',
              annotation)
    plt.show()
    
    # Add ECDFs for both years
    fig, ax = plt.subplots(figsize=FIGSIZE)
    
    # Calculate and plot ECDF for second year
    year_data = residual_pct[residual_pct.index.year == second_year]
    sorted_data = np.sort(year_data)
    ecdf = np.arange(1, len(sorted_data) + 1) / len(sorted_data)
    ax.plot(sorted_data, ecdf, label=str(second_year))
    
    # Calculate and plot ECDF for second last year  
    year_data = residual_pct[residual_pct.index.year == second_last_year]
    sorted_data = np.sort(year_data)
    ecdf = np.arange(1, len(sorted_data) + 1) / len(sorted_data)
    ax.plot(sorted_data, ecdf, label=str(second_last_year))
    
    annotation = ("Empirical Cumulative Distribution Function shows\n"
                 "the probability of residual load being below a certain percentage.\n"
                 "Steeper sections indicate more frequent values.")
    
    style_plot(fig, ax,
              'ECDF of Residual Load Percentage',
              'Residual Load Percentage',
              'Cumulative Probability',
              annotation)
    
    plt.legend()
    plt.show()
    
plot_grid_stability()

# %%
