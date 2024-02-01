import pandas as pd
import os

# Set the path to the folder containing CSV files
folder_path = 'data_from_hdfs'

# List all files in the folder
csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

# Initialize an empty DataFrame to store the merged data
df = pd.DataFrame()

# Loop through each CSV file and merge its content into the main DataFrame
for csv_file in csv_files:
    # Construct the full path to the CSV file
    csv_path = os.path.join(folder_path, csv_file)
    
    # Read the CSV file into a DataFrame
    current_df = pd.read_csv(csv_path)
    
    # Merge the current DataFrame into the main DataFrame
    df = pd.concat([df, current_df], ignore_index=True)

# Data Warehouse Preparation :
 
# Extracting necessary columns for each table
league = df[['league']]
season = df[['season']]
player = df[['player_id', 'player_name']]
team = df[['team_title']]
position = df[['position']]
playerstatistics = df[['games', 'time', 'goals', 'assists', 'npg', 'xG_percentage', 'xA_percentage', 'npxG_percentage', 'shots', 'missed_shots', 'key_passes', 'yellow_cards', 'red_cards']]

# Encoding categorical columns and creating unique IDs
league['league_id'] = league['league'].astype('category').cat.codes
season['season_id'] = season['season'].astype('category').cat.codes
team['team_id'] = team['team_title'].astype('category').cat.codes
position['position_id'] = position['position'].astype('category').cat.codes

# Generating factorized ID for playerstatistics
playerstatistics['pstatistics_id'] = pd.factorize(
    playerstatistics[
        ['games', 'time', 'goals', 'assists', 'npg', 'xG_percentage', 'xA_percentage',
         'npxG_percentage', 'shots', 'missed_shots', 'key_passes', 'yellow_cards', 'red_cards']
    ].apply(tuple, axis=1)
)[0]

# Merging IDs from other tables
playerstatistics['league_id'] = league['league_id']
playerstatistics['season_id'] = season['season_id']
playerstatistics['player_id'] = player['player_id']
playerstatistics['team_id'] = team['team_id']
playerstatistics['position_id'] = position['position_id']

# Change the column order using indexing on each DataFrame
league = league[['league_id', 'league']]
season = season[['season_id', 'season']]
team = team[['team_id', 'team_title']]
position = position[['position_id', 'position']]

desired_column_order = ['pstatistics_id', 'league_id', 'season_id', 'player_id', 'team_id', 'position_id',
                        'games', 'time', 'goals', 'assists', 'npg', 'xG_percentage', 'xA_percentage',
                         'npxG_percentage', 'shots', 'missed_shots', 'key_passes', 'yellow_cards', 'red_cards']
playerstatistics = playerstatistics[desired_column_order]

import pyodbc

# Establish a connection
connection = pyodbc.connect('DRIVER=SQL SERVER;SERVER=LAPTOP-1US3GU3J\SQLEXPRESS,1433;DATABASE=season_players')

# Create a cursor to execute SQL queries
cursor = connection.cursor()

# Create the league Dimension
cursor.execute('''
    CREATE TABLE [DimLeague] (
      [league_id] INT,
      [league] VARCHAR(255),
      PRIMARY KEY ([league_Id])
    );
''')

# Create the season Dimension
cursor.execute('''
    CREATE TABLE [DimSeason] (
      [season_id] INT,
      [season] VARCHAR(255),
      PRIMARY KEY ([season_id])
    );
''')

# Create the player Dimension
cursor.execute('''
    CREATE TABLE [DimPlayer] (
      [player_id] INT,
      [player_name] VARCHAR(255),
      PRIMARY KEY ([player_id])
    );
''')

# Create the team Dimension
cursor.execute('''
    CREATE TABLE [DimTeam] (
      [team_id] INT,
      [team_title] VARCHAR(255),
      PRIMARY KEY ([team_id])
    );
''')

# Create the position Dimension
cursor.execute('''
    CREATE TABLE [DimPosition] (
      [position_id] INT,
      [position] VARCHAR(255),
      PRIMARY KEY ([position_id])
    );
''')

# Create the FactPlayerStatistics table with foreign key constraints
cursor.execute('''
    CREATE TABLE [FactPlayerStatistics] (
      [pstatistics_id] INT PRIMARY KEY,
      [league_id] INT,
      [season_id] INT,
      [player_id] INT,
      [team_id] INT,
      [position_id] INT,
      [games] INT,
      [time] INT,
      [goals] INT,
      [assists] INT,
      [npg] INT,
      [xG_percentage] FLOAT,
      [xA_percentage] FLOAT,
      [npxG_percentage] FLOAT,
      [shots] INT,
      [missed_shots] INT,
      [key_passes] INT,
      [yellow_cards] INT,
      [red_cards] INT,
      FOREIGN KEY ([league_id]) REFERENCES [DimLeague]([league_id]),
      FOREIGN KEY ([season_id]) REFERENCES [DimSeason]([season_id]),
      FOREIGN KEY ([player_id]) REFERENCES [DimPlayer]([player_id]),
      FOREIGN KEY ([team_id]) REFERENCES [DimTeam]([team_id]),
      FOREIGN KEY ([position_id]) REFERENCES [DimPosition]([position_id])
    );
''')

# Commit the changes to the database
connection.commit()

# Indexes for FactPlayerStatistics table
cursor.execute('CREATE INDEX idx_goals ON FactPlayerStatistics(goals);')
cursor.execute('CREATE INDEX idx_time ON FactPlayerStatistics(time);')
cursor.execute('CREATE INDEX idx_assists ON FactPlayerStatistics(assists);')
cursor.execute('CREATE INDEX idx_npg ON FactPlayerStatistics(npg);')
cursor.execute('CREATE INDEX idx_shots ON FactPlayerStatistics(shots);')
cursor.execute('CREATE INDEX idx_missed_shots ON FactPlayerStatistics(missed_shots);')
cursor.execute('CREATE INDEX idx_key_passes ON FactPlayerStatistics(key_passes);')
cursor.execute('CREATE INDEX idx_yellow_cards ON FactPlayerStatistics(yellow_cards);')
cursor.execute('CREATE INDEX idx_red_cards ON FactPlayerStatistics(red_cards);')

# Indexes for DimPlayer table
cursor.execute('CREATE INDEX idx_player_name ON DimPlayer(player_name);')

# Indexes for DimTeam table
cursor.execute('CREATE INDEX idx_team_title ON DimTeam(team_title);')


# Commit the changes to the database
connection.commit()

# Insert data into DimLeague table
for row in league.itertuples():
    # Check if the league_id already exists in the table
    cursor.execute('SELECT COUNT(*) FROM DimLeague WHERE league_id = ?', (row.league_id,))
    count = cursor.fetchone()[0]
    
    if count == 0:
        # Insert the record if it doesn't exist
        cursor.execute('INSERT INTO DimLeague (league_id, league) VALUES (?, ?)', (row.league_id, row.league))        

# Insert data into DimSeason table
for row in season.itertuples():
    cursor.execute('SELECT COUNT(*) FROM DimSeason WHERE season_id = ?', (row.season_id,))
    count = cursor.fetchone()[0]
    
    if count == 0:
        cursor.execute('INSERT INTO DimSeason (season_id, season) VALUES (?, ?)', (row.season_id, row.season))

# Insert data into DimPlayer table
for row in player.itertuples():
    cursor.execute('SELECT COUNT(*) FROM DimPlayer WHERE player_id = ?', (row.player_id,))
    count = cursor.fetchone()[0]
    
    if count == 0:
        cursor.execute('INSERT INTO DimPlayer (player_id, player_name) VALUES (?, ?)', (row.player_id, row.player_name))

# Insert data into DimTeam table
for row in team.itertuples():
    cursor.execute('SELECT COUNT(*) FROM DimTeam WHERE team_id = ?', (row.team_id,))
    count = cursor.fetchone()[0]
    
    if count == 0:
        cursor.execute('INSERT INTO DimTeam (team_id, team_title) VALUES (?, ?)', (row.team_id, row.team_title))

# Insert data into DimPosition table
for row in position.itertuples():
    cursor.execute('SELECT COUNT(*) FROM DimPosition WHERE position_id = ?', (row.position_id,))
    count = cursor.fetchone()[0]
    
    if count == 0:
        cursor.execute('INSERT INTO DimPosition (position_id, position) VALUES (?, ?)', (row.position_id, row.position))

# Insert data into FactPlayerStatistics table
for row in playerstatistics.itertuples():
    cursor.execute('SELECT COUNT(*) FROM FactPlayerStatistics WHERE pstatistics_id = ?', (row.pstatistics_id,))
    count = cursor.fetchone()[0]
    
    if count == 0:
        cursor.execute('''
            INSERT INTO FactPlayerStatistics (
                pstatistics_id, league_id, season_id, player_id, team_id, position_id, games, time, goals, assists, npg,
                xG_percentage, xA_percentage, npxG_percentage, shots, missed_shots, key_passes, yellow_cards, red_cards
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row.pstatistics_id, row.league_id, row.season_id, row.player_id, row.team_id, row.position_id,
            row.games, row.time, row.goals, row.assists, row.npg, row.xG_percentage, row.xA_percentage,
            row.npxG_percentage, row.shots, row.missed_shots, row.key_passes, row.yellow_cards, row.red_cards
        ))

# Commit the changes to the database
connection.commit()

# Define the SQL query for Player Performance Data Mart View
player_performance_view_query = '''
CREATE VIEW PlayerPerformanceDataMart AS
SELECT
    fps.pstatistics_id,
    l.league,
    s.season,
    p.player_name,
    t.team_title,
    pos.position,
    fps.games,
    fps.time,
    fps.goals,
    fps.assists,
    fps.npg,
    fps.xG_percentage,
    fps.xA_percentage,
    fps.npxG_percentage,
    fps.shots,
    fps.missed_shots,
    fps.key_passes,
    fps.yellow_cards,
    fps.red_cards
FROM
    FactPlayerStatistics fps
    JOIN DimLeague l ON fps.league_id = l.league_id
    JOIN DimSeason s ON fps.season_id = s.season_id
    JOIN DimPlayer p ON fps.player_id = p.player_id
    JOIN DimTeam t ON fps.team_id = t.team_id
    JOIN DimPosition pos ON fps.position_id = pos.position_id;
'''

# Execute the query to create Player Performance Data Mart View
cursor.execute(player_performance_view_query)

# Commit the changes to the database
connection.commit()

# Define the SQL query for Team Statistics Data Mart View without grouping by fps.pstatistics_id
team_statistics_view_query = '''
CREATE VIEW TeamStatisticsDataMart AS
SELECT
    l.league,
    s.season,
    t.team_title,
    SUM(fps.goals) AS total_goals,
    SUM(fps.assists) AS total_assists,
    AVG(fps.xG_percentage) AS avg_xG_percentage,
    AVG(fps.xA_percentage) AS avg_xA_percentage,
    SUM(fps.shots) AS total_shots,
    SUM(fps.missed_shots) AS total_missed_shots,
    SUM(fps.yellow_cards) AS total_yellow_cards,
    SUM(fps.red_cards) AS total_red_cards
FROM
    FactPlayerStatistics fps
    JOIN DimLeague l ON fps.league_id = l.league_id
    JOIN DimSeason s ON fps.season_id = s.season_id
    JOIN DimTeam t ON fps.team_id = t.team_id
GROUP BY
    l.league,
    s.season,
    t.team_title;
'''

# Execute the query to create Team Statistics Data Mart View
cursor.execute(team_statistics_view_query)

# Commit the changes to the database
connection.commit()

# # Create server-level logins with passwords
# cursor.execute("CREATE LOGIN DataEngineerLogin WITH PASSWORD = 'DataEngineer2004';")
# cursor.execute("CREATE LOGIN DataAnalystLogin WITH PASSWORD = 'DataAnalyst2004';")

# Create database users
cursor.execute("CREATE USER DataEngineerUser FOR LOGIN DataEngineerLogin;")
cursor.execute("CREATE USER DataAnalystUser FOR LOGIN DataAnalystLogin;")

# Create database roles for Data Engineer and Data Analyst
cursor.execute("CREATE ROLE DataEngineerRole;")
cursor.execute("CREATE ROLE DataAnalystRole;")

# Add users to their respective roles
cursor.execute("ALTER ROLE DataEngineerRole ADD MEMBER DataEngineerUser;")
cursor.execute("ALTER ROLE DataAnalystRole ADD MEMBER DataAnalystUser;")

# Grant permissions to roles
# Adjust permissions based on your specific requirements

# DimLeague
cursor.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON [DimLeague] TO DataEngineerRole;")
cursor.execute("GRANT SELECT ON [DimLeague] TO DataAnalystRole;")

# DimSeason
cursor.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON [DimSeason] TO DataEngineerRole;")
cursor.execute("GRANT SELECT ON [DimSeason] TO DataAnalystRole;")

# DimPlayer
cursor.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON [DimPlayer] TO DataEngineerRole;")
cursor.execute("GRANT SELECT ON [DimPlayer] TO DataAnalystRole;")

# DimTeam
cursor.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON [DimTeam] TO DataEngineerRole;")
cursor.execute("GRANT SELECT ON [DimTeam] TO DataAnalystRole;")

# DimPosition
cursor.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON [DimPosition] TO DataEngineerRole;")
cursor.execute("GRANT SELECT ON [DimPosition] TO DataAnalystRole;")

# FactPlayerStatistics
cursor.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON [FactPlayerStatistics] TO DataEngineerRole;")
cursor.execute("GRANT SELECT ON [FactPlayerStatistics] TO DataAnalystRole;")

# Commit the changes to the database
connection.commit()

cursor.close()
connection.close()