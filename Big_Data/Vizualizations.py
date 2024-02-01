import matplotlib
matplotlib.use('Agg')

import dash
from dash import dcc, html
from mplsoccer import Pitch
from dash.dependencies import Input, Output, State
import pandas as pd
from io import BytesIO
import base64
import mplsoccer
import matplotlib.pyplot as plt
import pymongo
import tkinter as tk
tk.Tk().withdraw()

# Replace these values with your MongoDB connection details
mongo_uri = "mongodb://localhost:27017"
database_name = "events_database"
collection_name = "events"

# Connect to MongoDB
client = pymongo.MongoClient(mongo_uri)
database = client[database_name]
collection = database[collection_name]

# Retrieve all data from MongoDB
cursor = collection.find({})
data = list(cursor)
df = pd.DataFrame(data)

# Initialize Dash app
app = dash.Dash(__name__)

# Load pitch image
pitch = mplsoccer.Pitch(pitch_color='green', pitch_type='statsbomb')

# Create a unique identifier for each match
df['match_identifier'] = df['h_team'] + ' vs ' + df['a_team'] + ' (' + df['date'].str.split(' ').str[0] + ')'

# Define a color dictionary for each event type
event_colors = {
    'MissedShots': 'red',
    'BlockedShot': 'blue',
    'Goal': 'orange',  # Change the color for the 'Goal' event
    'SavedShot': 'yellow'
}

# Define layout
app.layout = html.Div([
    html.Div([
        html.H1("Football Match Events", style={'text-align': 'center', 'color': 'white'}),
        html.Label("Select League:", style={'color': 'white'}),
        dcc.Dropdown(
            id='league-dropdown',
            options=[{'label': league, 'value': league} for league in df['league'].unique()],
            value=df['league'].unique()[0],
            style={'width': '50%', 'margin': 'auto'}
        ),
        html.Label("Select Season:", style={'color': 'white'}),
        dcc.Dropdown(
            id='season-dropdown',
            options=[{'label': season, 'value': season} for season in df['season'].unique()],
            value=df['season'].unique()[0],
            style={'width': '50%', 'margin': 'auto'}
        ),
        html.Label("Select Match:", style={'color': 'white'}),
        dcc.Dropdown(
            id='match-dropdown',
            options=[
                {'label': f"{row['h_team']} vs {row['a_team']} ({row['date'].split()[0]})", 'value': row['match_id']} 
                for _, row in df.drop_duplicates(subset=['match_id']).iterrows()
            ],
            value=df['match_id'].iloc[0],
            style={'width': '50%', 'margin': 'auto'}
        ),
        html.Button('Confirm', id='confirm-button', n_clicks=0, style={'margin-top': '10px'}),
        html.Div(id='match-outcome', style={'font-size': '36px', 'text-align': 'center', 'margin-top': '20px', 'color': 'white'}),
    ], style={'background-color': '#343a40', 'padding': '20px', 'background-image': 'url("https://www.scisports.com/wp-content/uploads/2018/08/Passes_Header-1920-1280x720.png")', 'background-size': 'cover'}),
    html.Div([
        html.Img(id='mplsoccer-pitch', src='', style={'width': '60%', 'margin': 'auto'}),
    ], id='viz-container', style={'background-color': '#343a40', 'padding': '20px', 'text-align': 'center'}),
], style={'background-color': '#282c34', 'height': '100vh'})


# Define callback to update the pitch and match outcome based on dropdown selection
@app.callback(
    [Output('mplsoccer-pitch', 'src'),
     Output('match-outcome', 'children')],
    [Input('confirm-button', 'n_clicks')],
    [State('match-dropdown', 'value'),
     State('season-dropdown', 'value'),
     State('league-dropdown', 'value')]
)
def update_pitch(n_clicks, selected_match_id, selected_season, selected_league):
    if n_clicks > 0:
        match_df = df[(df['match_id'] == selected_match_id) & (df['season'] == selected_season) & (df['league'] == selected_league)]

        # Check if match_df is not empty
        if not match_df.empty:
            title_text = f"{match_df['h_team'].iloc[0]} vs {match_df['a_team'].iloc[0]}"
            match_outcome = f"{int(match_df['h_goals'].iloc[0])} - {int(match_df['a_goals'].iloc[0])}"

            fig, ax = pitch.draw(figsize=(12, 7))

            for i, event in match_df.iterrows():
                pitch.scatter(event['X'], event['Y'], s=300, ax=ax, label=event['result'], zorder=3, color=event_colors.get(event['result'], 'gray'))
                ax.text(event['X'], event['Y'], event['result'], fontsize=10, ha='center', va='center', color='white')

            ax.set_title(title_text, fontsize=18)
            img_bytes = BytesIO()
            plt.savefig(img_bytes, format='png')
            plt.close(fig)

            return (f'data:image/png;base64,{base64.b64encode(img_bytes.getvalue()).decode()}', match_outcome)
        else:
            return ('', 'No data for the selected match. Please choose a different match.')
    else:
        return ('', '')

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)