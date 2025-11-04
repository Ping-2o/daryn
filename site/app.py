# app.py
from flask import Flask, render_template, request, jsonify
import requests
import json
import logging
from datetime import datetime

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    filename='app.log',  # Log file name
    level=logging.INFO,  # Log level (INFO, DEBUG, ERROR, etc.)
    format='%(asctime)s - %(levelname)s - %(message)s', # Log format
    datefmt='%Y-%m-%d %H:%M:%S' # Date format for logs
)

# Define the URL of your local AI model
AI_MODEL_URL = "http://127.0.0.1:1234" # Ensure your model is running here

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        try:
            # Collect data from the form
            form_data = {
                "annual_production_volume": float(request.form['annual_production_volume']),
                "mining_depth": float(request.form['mining_depth']),
                "operation_years": float(request.form['operation_years']),
                "mine_type": request.form['mine_type'],
                "primary_mineral_type": request.form['primary_mineral_type'],
                "distance_to_water_body": float(request.form['distance_to_water_body']),
                "elevation": float(request.form['elevation']),
                "annual_precipitation": float(request.form['annual_precipitation']),
                "soil_type": request.form['soil_type'],
                "baseline_water_quality": float(request.form['baseline_water_quality']),
                "baseline_air_quality": float(request.form['baseline_air_quality']),
                "baseline_vegetation_ndvi": float(request.form['baseline_vegetation_ndvi'])
            }

            # Log the received form data
            logging.info(f"Form submitted: {form_data}")

            # Send data to the local AI model
            print(f"Sending data to AI model: {form_data}") # Debug print
            response = requests.post(AI_MODEL_URL, json=form_data) # Send JSON data

            if response.status_code == 200:
                # Assuming the AI model returns JSON
                result = response.json()
                print(f"AI Model Response: {result}") # Debug print
                # Log the successful result
                logging.info(f"AI Model Result: {result}")
                return render_template('index.html', result=result, form_data=form_data)
            else:
                error_message = f"AI Model Error: Status Code {response.status_code}, Message: {response.text}"
                print(error_message) # Debug print
                # Log the error
                logging.error(error_message)
                return render_template('index.html', error=error_message, form_data=form_data)

        except ValueError as e:
            # Handle errors related to converting strings to numbers
            error_message = f"Invalid input format: {str(e)}"
            print(error_message) # Debug print
            logging.error(error_message)
            return render_template('index.html', error=error_message)
        except requests.exceptions.RequestException as e:
            # Handle errors related to the HTTP request to the AI model
            error_message = f"Error communicating with the AI model: {str(e)}"
            print(error_message) # Debug print
            logging.error(error_message)
            return render_template('index.html', error=error_message, form_data=form_data)
        except Exception as e:
            # Handle other potential errors
            error_message = f"An unexpected error occurred: {str(e)}"
            print(error_message) # Debug print
            logging.error(error_message)
            return render_template('index.html', error=error_message)

    # If it's a GET request, just render the empty form
    return render_template('index.html')

# New route to display logs
@app.route('/logs')
def show_logs():
    try:
        with open('app.log', 'r') as log_file:
            logs = log_file.read()
        return f"<pre>{logs}</pre>" # Display logs in a pre-formatted block
    except FileNotFoundError:
        return "<p>No log file found.</p>"

if __name__ == '__main__':
    app.run(debug=True) # Run the Flask app in debug mode