import pandas as pd
import json
import pyodbc
from hdfs import InsecureClient
from io import BytesIO
import time
import pyarrow as pa
import pyarrow.parquet as pq

# Read data from CSV   
csv_data = pd.read_csv('Data_Splitter/datasets/csv_players.csv')

# Read data from JSON file
json_data = []
with open('Data_Splitter/datasets/json_players.json', 'r') as json_file:
    for line in json_file:
        try:
            data = json.loads(line)
            json_data.append(data)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")

# Convert JSON data to DataFrame
json_df = pd.DataFrame(json_data)

import requests
import pandas as pd

# Function to get all players from the API
def get_all_players():
    url = 'http://127.0.0.1:5001/api/players'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # Convert the list of dictionaries to a DataFrame
        df = pd.DataFrame(data)
        # Select and reorder columns
        selected_data = df[desired_columns]
        return selected_data
    else:
        print(f"Error: {response.status_code}")
        return None

# Define the desired column order
desired_columns = ['league', 'season', 'player_id', 'player_name', 'team_title', 'position',
                   'games', 'time', 'goals', 'assists', 'xG', 'xA', 'shots', 'key_passes',
                   'yellow_cards', 'red_cards', 'npg', 'npxG', 'xGChain', 'xGBuildup']    

# Get all players and display the data
all_players_data = get_all_players()
all_players_data

# Merge all DataFrames
merged_data = pd.concat([csv_data, json_df, all_players_data], ignore_index=True)

# Connect to HDFS
client = InsecureClient('http://localhost:9870/', user='hadoop')

# Get unique seasons from the dataset
unique_seasons = merged_data['season'].unique()

# Loop through each unique season and insert data into HDFS
for season in unique_seasons:
    # Filter data for the current season
    season_data = merged_data[merged_data['season'] == season]

    # Convert DataFrame to Parquet in memory
    parquet_data = BytesIO()
    parquet_table = pa.Table.from_pandas(season_data)
    pq.write_table(parquet_table, parquet_data)

    # Set HDFS path for the current season
    hdfs_path = f'/seasons_players/season{season}/players_data_{season}.parquet'

    # Check if the HDFS path already exists
    if client.status(hdfs_path, strict=False):
        print(f"Path '{hdfs_path}' already exists. Overwriting...")

    # Write data to HDFS for the current season with overwrite option
    with client.write(hdfs_path, overwrite=True) as writer:
        writer.write(parquet_data.getvalue())

    # Add a delay before moving to the next season
    print(f"Waiting few seconds before moving to the next season...")
    time.sleep(30)  # Sleep for 30 seconds

    # Add a statement indicating completion for the current season
    print(f"Data insertion for season {season} completed.")

# Add a final completion message after all seasons are processed
print("All data insertion operations completed successfully.")