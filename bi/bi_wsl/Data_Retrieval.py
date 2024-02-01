from hdfs import InsecureClient
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
import os

# Connect to HDFS
client = InsecureClient('http://localhost:9870/', user='hadoop')

# Define the local path to save the CSV files
local_path = '/mnt/c/Users/Youcode/Desktop/8 Months/Fil Rouge/bi/data_from_hdfs/'

# Function to read data for a specific season from HDFS
def read_season_data(season):
    hdfs_path = f'/seasons_players/season{season}/players_data_{season}.parquet'
    
    # Read Parquet file content into BytesIO buffer
    with client.read(hdfs_path) as reader:
        parquet_content = BytesIO(reader.read())
    
    # Read Parquet file using pyarrow
    parquet_file = pq.read_table(parquet_content)
    
    # Convert Parquet file to Pandas DataFrame
    df = parquet_file.to_pandas()
    return df

# Function to perform data transformations
def transform_data(df):
    # Use the str.contains() method to filter rows
    filtered_rows = df[df['team_title'].str.contains(',')]

    # Use the drop method to delete the filtered rows from the original DataFrame
    df = df.drop(filtered_rows.index)
    # Drop unnecessary columns
    columns_to_drop = ['xGChain', 'xGBuildup']
    df = df.drop(columns=columns_to_drop, axis=1)

    # Calculate and create new percentage columns
    percentage_columns = ['xG', 'xA', 'npxG']
    new_percentage_columns = [col + '_percentage' for col in percentage_columns]

    for col, new_col in zip(percentage_columns, new_percentage_columns):
        # Calculate percentage and assign to a new column
        df[new_col] = (df[col] / df[percentage_columns].sum(axis=1)) * 100

    # Remove the original percentage columns
    df = df.drop(columns=percentage_columns)

    # Replace NaN values with 0 in specific columns
    columns_to_fill_zero = ['xG_percentage', 'xA_percentage', 'npxG_percentage']
    df[columns_to_fill_zero] = df[columns_to_fill_zero].fillna(0)

    # Calculate and add a new column for missed shots
    df['missed_shots'] = df['shots'] - df['goals']

    # Reorder columns for better readability
    desired_column_order = ['league', 'season', 'player_id', 'player_name', 'team_title', 'position',
                             'games', 'time', 'goals', 'assists', 'npg', 'xG_percentage', 'xA_percentage', 'npxG_percentage',
                             'shots', 'missed_shots', 'key_passes', 'yellow_cards', 'red_cards']

    df = df[desired_column_order]

    return df

def save_season_data(df, season):
    # Construct the local path to save the CSV file
    file_path = os.path.join(local_path, f'season_{season}.csv')
    
    # Save the DataFrame as CSV
    df.to_csv(file_path, index=False)
    print(f"Saved data for season {season} to {file_path}")

def get_unique_seasons():
    all_files = client.list('/seasons_players/')
    unique_seasons = set()
    for file in all_files:
        season = file.split('_')[-1].split('.')[0]
        if season.startswith('season'):
            season = season[len('season'):]
        unique_seasons.add(season)
    return sorted(list(unique_seasons))

# Get unique seasons available in the HDFS directory
unique_seasons = get_unique_seasons()

# Display and save data for each season available in HDFS
for season in unique_seasons:
    season_data = read_season_data(season)
    season_data_transformed = transform_data(season_data)
    # Save the transformed data
    save_season_data(season_data_transformed, season)