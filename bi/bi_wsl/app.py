from flask import Flask, jsonify
import pandas as pd

app = Flask(__name__)

# Load the CSV data
api_data = pd.read_csv('Data_Splitter/datasets/api_players.csv')

# Endpoint to get all data
@app.route('/api/players', methods=['GET'])
def get_all_players():
    return jsonify(api_data.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True, port=5001)