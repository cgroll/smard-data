# %%
import pandas as pd
import matplotlib.pyplot as plt

# %%
from pathlib import Path
from smard_data.config import Variable, Resolution
from smard_data.paths import ProjPaths

# Get path to downloaded data files
paths = ProjPaths()
data_path = paths.raw_data_path / f"{Variable.get_name(Variable.SOLAR.value)}_{Resolution.QUARTER_HOUR.value}.parquet"

# Load the solar generation data
df = pd.read_parquet(data_path)
print(f"Loaded {len(df):,} rows of data")
print("\nFirst few rows:")
print(df.head())


# Load and combine all data files
from pathlib import Path
import pandas as pd

# Get generation variable names
from smard_data.config import Variable
generation_vars = [Variable.get_name(var) for var in Variable.get_generation_variables()]

# Get paths to generation data files
from smard_data.paths import ProjPaths
paths = ProjPaths()
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
    # Read the file
    df_temp = pd.read_parquet(file)
    dfs.append(df_temp)

# Combine all dataframes
df_combined = pd.concat(dfs, axis=1)

# normalize the unit of the data to hourly frequency
df_combined = df_combined * 4

# Sort index and handle any duplicate timestamps
df_combined = df_combined.sort_index()
df_combined = df_combined[~df_combined.index.duplicated(keep='first')]

# Group columns by energy type
renewable_cols = ['SOLAR', 'WIND_OFFSHORE', 'WIND_ONSHORE', 'BIOMASS', 'HYDRO', 'OTHER_RENEWABLE']
conventional_cols = ['NUCLEAR', 'BROWN_COAL', 'HARD_COAL', 'NATURAL_GAS', 'OTHER_CONVENTIONAL']
storage_cols = ['PUMPED_STORAGE']

# Create sorted column list and verify all columns are included
sorted_cols = renewable_cols + conventional_cols + storage_cols
assert set(sorted_cols) == set(df_combined.columns), "Missing or extra columns detected"

# Display sorted columns
df_combined = df_combined[sorted_cols]

print("\nCombined dataframe shape:", df_combined.shape)
print("\nColumns:", df_combined.columns.tolist())
print("\nFirst few rows:")
print(df_combined.head())

# %%

df_combined.sum(axis=1).resample('M').mean().plot()

# %%

# Load total and residual load data
load_vars = [Variable.get_name(var) for var in Variable.get_consumption_variables()]
load_files = []
for var in load_vars:
    file = paths.raw_data_path / f"{var}_quarterhour.parquet"
    if file.exists():
        load_files.append(file)

if not load_files:
    raise RuntimeError("No load data files found")

# Load and concatenate load files 
load_dfs = []
for file in load_files:
    df_temp = pd.read_parquet(file)
    load_dfs.append(df_temp)

# Combine load dataframes
df_load = pd.concat(load_dfs, axis=1)


# Sort index and handle duplicates
df_load = df_load.sort_index()
df_load = df_load[~df_load.index.duplicated(keep='first')]

print("\nLoad dataframe shape:", df_load.shape)
print("\nColumns:", df_load.columns.tolist())
print("\nFirst few rows:")
print(df_load.head())

# %%

# Plot yearly total load
plt.figure(figsize=(10, 6))
df_load['TOTAL_LOAD'].resample('Y').sum().plot()
plt.title('Yearly Total Load')
plt.ylabel('Total Load (MWh)')
plt.show()

# %%

# Plot monthly total load
plt.figure(figsize=(10, 6))
df_load['TOTAL_LOAD'].resample('M').mean().plot()
plt.title('Monthly Total Load')
plt.ylabel('Total Load (MW)')
plt.show()


# %%

# Monthly data with trend
import matplotlib.pyplot as plt

# Monthly averages with trend line
plt.figure(figsize=(10, 6))
monthly = df_combined['SOLAR'].resample('M').mean()
trend = monthly.rolling(window=12, center=True).mean()

plt.plot(monthly.index, monthly, alpha=0.5, label='Monthly average')
plt.plot(monthly.index, trend, 'r-', label='12-month rolling average')
plt.title('Monthly Solar Generation with Trend')
plt.ylabel('Power (MW)')
plt.legend()
plt.show()

# Month of year averages (seasonal pattern)
plt.figure(figsize=(10, 6))
monthly_grouped = df_combined['SOLAR'].groupby(df_combined.index.month).mean()
plt.plot(monthly_grouped.index, monthly_grouped, marker='o')
plt.title('Average Solar Generation by Month')
plt.xlabel('Month')
plt.ylabel('Power (MW)')
plt.xticks(range(1,13))
plt.show()

# Hour of day averages
plt.figure(figsize=(10, 6))
hourly_grouped = df_combined['SOLAR'].groupby(df_combined.index.hour).mean()
plt.plot(hourly_grouped.index, hourly_grouped, marker='o')
plt.title('Average Solar Generation by Hour')
plt.xlabel('Hour')
plt.ylabel('Power (MW)')
plt.xticks(range(0,24,2))
plt.show()

# Heatmap of hour vs month
plt.figure(figsize=(10, 6))
pivot = df_combined['SOLAR'].groupby([df_combined.index.month, df_combined.index.hour]).mean().unstack()
im = plt.imshow(pivot, aspect='auto', cmap='YlOrRd')
plt.title('Solar Generation: Hour vs Month')
plt.xlabel('Hour')
plt.ylabel('Month')
plt.colorbar(label='Power (MW)')
plt.show()

# %%

# Calculate total wind generation
df_combined['WIND_TOTAL'] = df_combined['WIND_OFFSHORE'] + df_combined['WIND_ONSHORE']

# Monthly averages with trend line
plt.figure(figsize=(10, 6))
monthly = df_combined['WIND_TOTAL'].resample('M').mean()
trend = monthly.rolling(window=12, center=True).mean()

plt.plot(monthly.index, monthly, alpha=0.5, label='Monthly average')
plt.plot(monthly.index, trend, 'r-', label='12-month rolling average')
plt.title('Monthly Wind Generation with Trend')
plt.ylabel('Power (MW)')
plt.legend()
plt.show()

# Month of year averages (seasonal pattern)
plt.figure(figsize=(10, 6))
monthly_grouped = df_combined['WIND_TOTAL'].groupby(df_combined.index.month).mean()
plt.plot(monthly_grouped.index, monthly_grouped, marker='o')
plt.title('Average Wind Generation by Month')
plt.xlabel('Month')
plt.ylabel('Power (MW)')
plt.xticks(range(1,13))
plt.show()

# Hour of day averages
plt.figure(figsize=(10, 6))
hourly_grouped = df_combined['WIND_TOTAL'].groupby(df_combined.index.hour).mean()
plt.plot(hourly_grouped.index, hourly_grouped, marker='o')
plt.title('Average Wind Generation by Hour')
plt.xlabel('Hour')
plt.ylabel('Power (MW)')
plt.xticks(range(0,24,2))
plt.show()

# Heatmap of hour vs month
plt.figure(figsize=(10, 6))
pivot = df_combined['WIND_TOTAL'].groupby([df_combined.index.month, df_combined.index.hour]).mean().unstack()
im = plt.imshow(pivot, aspect='auto', cmap='YlOrRd')
plt.title('Wind Generation: Hour vs Month')
plt.xlabel('Hour')
plt.ylabel('Month')
plt.colorbar(label='Power (MW)')
plt.show()


# %%

# Stacked area chart of all power sources by month
plt.figure(figsize=(15, 6))  # Made wider to accommodate legend

monthly_gen = df_combined.resample('M').mean()
monthly_gen.plot.area(stacked=True)
plt.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=4)

# %%

# Calculate percentage contribution of each source to total generation
monthly_gen_pct = monthly_gen.copy()
monthly_total = monthly_gen.sum(axis=1)
for col in monthly_gen.columns:
    monthly_gen_pct[col] = monthly_gen[col] / monthly_total * 100

# Plot stacked area chart of percentage contributions
plt.figure(figsize=(15, 6))
monthly_gen_pct.plot.area(stacked=True)
plt.title('Relative Share of Power Generation Sources')
plt.ylabel('Percentage of Total Generation')
plt.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=4)
plt.show()

# %%


import seaborn as sns
# Create heatmap showing annual percentage contribution by source
plt.figure(figsize=(12, 8))

# Calculate annual averages as percentages
annual_gen = df_combined.resample('Y').mean()
annual_total = annual_gen.sum(axis=1)
annual_pct = annual_gen.div(annual_total, axis=0) * 100

# Format year labels to YYYY
annual_pct.index = annual_pct.index.strftime('%Y')

# Create heatmap with flipped axes
sns.heatmap(annual_pct, annot=True, fmt='.1f', cmap='YlOrRd',
            cbar_kws={'label': 'Percentage of Total Generation'})

plt.title('Annual Generation Mix')
plt.ylabel('Year')
plt.xlabel('Power Source')
plt.show()

# %%

# Get second last complete year of data
last_complete_year = annual_pct.index[-2]

# Create bar plot for second last year
plt.figure(figsize=(12, 6))
annual_pct.loc[last_complete_year].plot(kind='bar')
plt.title(f'Generation Mix in {last_complete_year}')
plt.ylabel('Percentage of Total Generation')
plt.xlabel('Power Source')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()


# %%

# Aggregate sources into three categories
renewable_sources = ['WIND_OFFSHORE', 'WIND_ONSHORE', 'SOLAR', 'BIOMASS', 'HYDRO', 'OTHER_RENEWABLE']
conventional_sources = ['NUCLEAR', 'BROWN_COAL', 'HARD_COAL', 'NATURAL_GAS', 'OTHER_CONVENTIONAL'] 
storage_sources = ['PUMPED_STORAGE']

# Create new dataframe with aggregated categories
df_agg = pd.DataFrame()
df_agg['Renewable'] = df_combined[renewable_sources].sum(axis=1)
df_agg['Conventional'] = df_combined[conventional_sources].sum(axis=1)
df_agg['Storage'] = df_combined[storage_sources].sum(axis=1)

# Monthly stacked area chart (absolute values)
plt.figure(figsize=(15, 6))
monthly_agg = df_agg.resample('M').mean()
monthly_agg.plot.area(stacked=True)
plt.title('Power Generation by Category')
plt.ylabel('Power Generation (MW)')
plt.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=3)
plt.show()

# Monthly stacked area chart (percentages)
monthly_agg_pct = monthly_agg.copy()
monthly_total_agg = monthly_agg.sum(axis=1)
for col in monthly_agg.columns:
    monthly_agg_pct[col] = monthly_agg[col] / monthly_total_agg * 100

plt.figure(figsize=(15, 6))
monthly_agg_pct.plot.area(stacked=True)
plt.title('Relative Share of Power Generation Categories')
plt.ylabel('Percentage of Total Generation')
plt.legend(bbox_to_anchor=(0.5, -0.15), loc='upper center', ncol=3)
plt.show()

# Annual heatmap
plt.figure(figsize=(10, 8))

# Calculate annual averages as percentages
annual_agg = df_agg.resample('Y').mean()
annual_total_agg = annual_agg.sum(axis=1)
annual_pct_agg = annual_agg.div(annual_total_agg, axis=0) * 100

# Format year labels to YYYY
annual_pct_agg.index = annual_pct_agg.index.strftime('%Y')

# Create heatmap
sns.heatmap(annual_pct_agg, annot=True, fmt='.1f', cmap='YlOrRd',
            cbar_kws={'label': 'Percentage of Total Generation'})

plt.title('Annual Generation Mix by Category')
plt.ylabel('Year')
plt.xlabel('Generation Category')
plt.show()


# %%

df_agg['Conventional'].resample('D').max().plot()
df_agg.sum(axis=1).resample('D').max().plot()
df_agg.sum(axis=1).resample('D').mean().plot()

# %%

# Calculate percentages relative to total generation
df_agg_pct = df_agg.div(df_agg.sum(axis=1), axis=0) * 100
df_agg_pct.resample('D').max().plot()

df_agg_pct.resample('D').max().rolling(window=30).mean().plot()

# %%

# Calculate residual load percentage
df_load = df_load.dropna()
residual_pct = (df_load['RESIDUAL_LOAD'] / df_load['TOTAL_LOAD'] * 100)
residual_pct = residual_pct.clip(lower=0)

residual_pct.resample('D').max().plot()
residual_pct.resample('M').max().plot()
residual_pct.resample('Y').max().plot()

# %%

df_max_monthly_residual_pct = residual_pct.resample('M').max()
df_max_monthly_residual_pct.plot()

# %%
# Create boxplot of monthly residual load percentages
plt.figure(figsize=(12, 6))

# Extract month from index and create boxplot
df_max_monthly_residual_pct.index = df_max_monthly_residual_pct.index.month
monthly_boxplot = plt.boxplot([df_max_monthly_residual_pct[df_max_monthly_residual_pct.index == month] 
                             for month in range(1, 13)],
                             labels=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])

plt.title('Monthly Distribution of Maximum Residual Load Percentage')
plt.ylabel('Residual Load Percentage')
plt.xlabel('Month')
plt.grid(True, alpha=0.3)
plt.show()

# %%

# Create boxplot of hourly residual load percentages
plt.figure(figsize=(15, 6))

# Extract hour from index and create boxplot
residual_pct.index = residual_pct.index.hour
hourly_boxplot = plt.boxplot([residual_pct[residual_pct.index == hour] 
                            for hour in range(24)],
                            labels=[f"{hour:02d}:00" for hour in range(24)])

plt.title('Hourly Distribution of Residual Load Percentage')
plt.ylabel('Residual Load Percentage')
plt.xlabel('Hour of Day')
plt.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# %%

# Create monthly line plot of total load
plt.figure(figsize=(12, 6))

# Group by month and calculate mean total load
monthly_total_load = df_load.groupby(df_load.index.month)['TOTAL_LOAD'].mean()
plt.plot(range(1,13), monthly_total_load.values)

plt.title('Average Total Load by Month')
plt.ylabel('Total Load (MW)')
plt.xlabel('Month') 
plt.grid(True, alpha=0.3)
plt.xticks(range(1,13), ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                         'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'], rotation=45)
plt.show()

# Create hourly line plot of total load
plt.figure(figsize=(15, 6))

# Calculate mean total load by hour
hourly_total_load = df_load.groupby(df_load.index.hour)['TOTAL_LOAD'].mean()
plt.plot(hourly_total_load.index, hourly_total_load.values)

plt.title('Average Hourly Total Load')
plt.ylabel('Total Load (MW)')
plt.xlabel('Hour of Day')
plt.grid(True, alpha=0.3)
plt.xticks(range(24), [f"{hour:02d}:00" for hour in range(24)], rotation=45)
plt.tight_layout()
plt.show()


# %%

# Create histogram of residual load percentage distribution
plt.figure(figsize=(10, 6))
plt.hist(residual_pct, bins=50, edgecolor='black')
plt.title('Distribution of Residual Load Percentage')
plt.xlabel('Residual Load Percentage')
plt.ylabel('Frequency')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

# Add descriptive statistics
print("\nDescriptive Statistics of Residual Load Percentage:")
print(f"Mean: {residual_pct.mean():.2f}%")
print(f"Median: {residual_pct.median():.2f}%")
print(f"Standard Deviation: {residual_pct.std():.2f}%")
print(f"Minimum: {residual_pct.min():.2f}%")
print(f"Maximum: {residual_pct.max():.2f}%")
