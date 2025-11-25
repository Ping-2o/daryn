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
            
            CRITICAL: You must respond with ONLY a valid JSON object. Do not include any explanatory text, markdown formatting, or code blocks.
            
            The JSON object must have this EXACT structure:
            {{
              "overall_risk_score": <number from 0.0 to 10.0>,
              "summary": "<2-3 sentence executive summary>",
              "risks": [
                {{ "category": "Water Contamination", "score": <number 0-10>, "details": "<explanation>" }},
                {{ "category": "Air Quality Degradation", "score": <number 0-10>, "details": "<explanation>" }},
                {{ "category": "Land & Biodiversity Impact", "score": <number 0-10>, "details": "<explanation>" }}
              ]
            }}

            Mining operation data to analyze:
            {json.dumps(form_data, indent=2)}
            
            Response (JSON only, no markdown, no explanation):
            """

            api_payload = {
              "model": "local-model",
              "messages": [
                {"role": "system", "content": "You are an environmental analyst AI that responds ONLY with valid JSON objects. Never include markdown code blocks, explanations, or any text outside the JSON object."},
                {"role": "user", "content": prompt_text}
              ],
              "temperature": 0.3
            }

            response = requests.post(AI_MODEL_URL, json=api_payload)

            if response.status_code == 200:
                api_result = response.json()
                result_content_str = api_result['choices'][0]['message']['content'].strip()
                logging.info(f"AI Model Raw Response: {result_content_str}")

                try:
                    # Try to extract JSON from the response
                    json_start = result_content_str.find('{')
                    json_end = result_content_str.rfind('}') + 1
                    
                    if json_start != -1 and json_end > json_start:
                        clean_json_str = result_content_str[json_start:json_end]
                        result_data = json.loads(clean_json_str)
                        
                        # Validate that the JSON has the expected structure
                        if not isinstance(result_data, dict):
                            raise ValueError("Response is not a JSON object")
                        if 'overall_risk_score' not in result_data:
                            raise ValueError("Missing 'overall_risk_score' field")
                        if 'summary' not in result_data:
                            raise ValueError("Missing 'summary' field")
                        if 'risks' not in result_data:
                            raise ValueError("Missing 'risks' field")
                        
                        logging.info(f"Successfully parsed AI response with risk score: {result_data.get('overall_risk_score')}")
                        return render_template('index.html', result_data=result_data, form_data=form_data)
                    else:
                        raise ValueError("No valid JSON object found in the AI response.")
                        
                except (json.JSONDecodeError, ValueError) as e:
                    logging.error(f"Failed to parse AI response as JSON: {e}")
                    logging.error(f"Problematic content: {result_content_str}")
                    return render_template('index.html', 
                                         error=f"The AI returned an invalid response. Please try again. Error: {str(e)}",
                                         result_raw=result_content_str)
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