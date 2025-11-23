from flask import Flask, render_template, request, Response
import requests
import json
import logging
import datetime  # <-- ADD THIS LINE
from weasyprint import HTML

# --- 1. CORE FLASK SETUP ---
app = Flask(__name__)

# Configure logging to save events to a file named 'app.log'
logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# The local URL where your AI model is being served
AI_MODEL_URL = "http://127.0.0.1:1234/v1/chat/completions"


# --- 2. MAIN APPLICATION ROUTE ---
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        try:
            form_data = {k: v for k, v in request.form.items()}
            logging.info(f"Form submitted: {json.dumps(form_data)}")

            prompt_text = f"""
            As an expert environmental analyst, evaluate the following mining operation data.
            Provide a comprehensive environmental impact assessment structured as a JSON object.
            
            The JSON object must have the following exact structure:
            {{
              "overall_risk_score": <A single number from 0.0 to 10.0 representing the total environmental risk. Higher is worse.>,
              "summary": "<A 2-3 sentence executive summary of the primary risks.>",
              "risks": [
                {{ "category": "Water Contamination", "score": <A number from 0 to 10>, "details": "<A concise explanation...>" }},
                {{ "category": "Air Quality Degradation", "score": <A number from 0 to 10>, "details": "<A concise explanation...>" }},
                {{ "category": "Land & Biodiversity Impact", "score": <A number from 0 to 10>, "details": "<A concise explanation...>" }}
              ]
            }}

            Here is the data to analyze:
            {json.dumps(form_data, indent=2)}
            
            Now, provide ONLY the JSON object as your response.
            """

            api_payload = {
              "model": "local-model",
              "messages": [
                {"role": "system", "content": "You are a helpful assistant that only responds with valid JSON objects."},
                {"role": "user", "content": prompt_text}
              ],
              "temperature": 0.7,
            }

            response = requests.post(AI_MODEL_URL, json=api_payload)

            if response.status_code == 200:
                api_result = response.json()
                result_content_str = api_result['choices'][0]['message']['content']
                logging.info(f"AI Model Raw Response: {result_content_str}")

                try:
                    json_start = result_content_str.find('{')
                    json_end = result_content_str.rfind('}') + 1
                    if json_start != -1 and json_end != -1:
                        clean_json_str = result_content_str[json_start:json_end]
                        result_data = json.loads(clean_json_str)
                        return render_template('index.html', result_data=result_data)
                    else:
                        raise ValueError("No valid JSON object found in the AI response.")
                except (json.JSONDecodeError, ValueError) as e:
                    logging.error(f"Failed to parse AI response as JSON: {e}")
                    return render_template('index.html', result_raw=result_content_str)
            else:
                error_message = f"AI Model Error: Status Code {response.status_code}, Message: {response.text}"
                logging.error(error_message)
                return render_template('index.html', error=error_message)

        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}", exc_info=True)
            return render_template('index.html', error=str(e))

    return render_template('index.html')


# --- 3. PDF GENERATION ROUTE (CORRECTED) ---
@app.route('/report', methods=['POST'])
def generate_report():
    try:
        report_data_str = request.form.get('report_data')
        if not report_data_str:
            return "Error: No report data provided.", 400
        
        report_data = json.loads(report_data_str)
        
        # Get the current time to pass to the template
        current_time = datetime.datetime.now()
        
        # Render the HTML template, passing both the report data and the current time
        rendered_html = render_template('report.html', data=report_data, now=current_time)
        
        pdf = HTML(string=rendered_html).write_pdf()
        
        return Response(
            pdf,
            mimetype='application/pdf',
            headers={'Content-Disposition': 'attachment;filename=EcoImpact_AI_Report.pdf'}
        )
        
    except Exception as e:
        logging.error(f"Failed to generate PDF report: {e}", exc_info=True)
        return "An error occurred while generating the report.", 500


# --- 4. HELPER ROUTE FOR LOGS ---
@app.route('/logs')
def show_logs():
    try:
        with open('app.log', 'r') as log_file:
            logs = log_file.read()
        return f"<pre>{logs}</pre>"
    except FileNotFoundError:
        return "<p>No log file found.</p>"


# --- 5. RUN THE APPLICATION ---
if __name__ == '__main__':
    host = '127.0.0.1'
    port = 5000
    print("===================================================")
    print(f"Flask server running at: http://{host}:{port}/")
    print("===================================================")
    app.run(host=host, port=port, debug=True)