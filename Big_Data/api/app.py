from flask import Flask, jsonify
import pandas as pd

app = Flask(__name__)

# Load the CSV data
try:
    api_data = pd.read_csv('shots_of_leagues_dataset.csv')
except Exception as e:
    print(f"Error loading CSV file: {str(e)}")
    api_data = pd.DataFrame()  # Empty DataFrame in case of an error

# Endpoint to get all data
@app.route('/api/matches', methods=['GET'])
def get_all_matches():
    return jsonify(api_data.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(debug=True)