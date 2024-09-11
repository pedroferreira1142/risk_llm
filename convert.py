import uuid
import base64
import vertexai
from vertexai.generative_models import GenerativeModel, Part, SafetySetting, FinishReason, GenerationConfig
import vertexai.generative_models as generative_models
from google.cloud import storage
import json
from google.cloud import bigquery
import requests
from email_sender import send_error_email

# Define your bucket name and PDF file path
bucket_name = 'formula_automator_bucket'

safety_settings = [
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_ONLY_HIGH
    ),
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_ONLY_HIGH
    ),
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_ONLY_HIGH
    ),
    SafetySetting(
        category=SafetySetting.HarmCategory.HARM_CATEGORY_HARASSMENT,
        threshold=SafetySetting.HarmBlockThreshold.BLOCK_ONLY_HIGH
    )
]

response_schema = {
  "type": "OBJECT",
  "properties": {
    "Project Data": {
      "type": "OBJECT",
      "properties": {
        "Country": {"type": "STRING"},
        "Project Title": {"type": "STRING"},
        "Project Description": {"type": "STRING"},
        "Practice Area (Lead)": {"type": "STRING"},
        "Proposed Development Objective(s)": {"type": "STRING"},
        "Financing (in US$, Millions)": {"type": "NUMBER"},
        "Start date": {"type": "STRING", "format": "date"},
        "Completion date": {"type": "STRING", "format": "date"}
      }
    },
    "Business Need": {"type": "STRING"},
    "Introduction and Context": {"type": "STRING"},
    "Proposed Development Objective(s)": {"type": "STRING"},
    "Goals": {"type": "STRING"},
    "Scope": {"type": "STRING"},
    "Opportunity": {"type": "STRING"},
    "Constraints": {"type": "STRING"},
    "Assumptions": {"type": "STRING"},
    "Deliverables": {"type": "STRING"},
    "Concept Description": {"type": "STRING"}
  }
}

text1 = """Convert the data on the file into the following fields

    • Project Data
        ○ Country
        ○ Project Title
        ○ Project Description
        ○ Practice Area (Lead)
        ○ Proposed Development Objective(s) 
        ○ Financing (in US$, Millions)
        ○ Start date
        ○ Completion date
    • Business Need
    • Introduction and Context
    • Proposed Development Objective(s)
    • Goals
    • Scope
    • Opportunity
    • Constraints
    • Assumptions
    • Deliverables
    • Concept Description."""

generation_config= GenerationConfig(
        response_mime_type="application/json", 
        response_schema=response_schema,
        max_output_tokens=8192,
        temperature=1,
    )

def insert_into_bigquery(data, document_id):
    # Initialize the BigQuery client
    client = bigquery.Client()

    # Set the table ID (project_id.dataset_id.table_id)
    table_id = 'globis-code-wizard.project_data.project_data_pid'
    
    # Extract the 'Project Data' from 'data' (key name includes space)
    project_data = data.get("Project Data", {})
    
    # Generate a unique UUID for the 'id' field
    row_id = str(uuid.uuid4())  # Generate a random UUID

    # Convert the parsed JSON response into a dictionary
    rows_to_insert = [
        {
            "uid": row_id,
            "document_id": document_id,
            "country": project_data.get("Country", ""),
            "project_title": project_data.get("Project Title", ""),
            "project_description": project_data.get("Project Description", ""),
            "practice_area_lead": project_data.get("Practice Area (Lead)", ""),
            "proposed_development_objective": project_data.get("Proposed Development Objective(s)", ""),
            "financing_usd_millions": project_data.get("Financing (in US$, Millions)", 0),
            "start_date": project_data.get("Start date", ""),
            "completion_date": project_data.get("Completion date", ""),
            "business_need": data.get("Business Need", ""),
            "introduction_and_context": data.get("Introduction and Context", ""),
            "goals": data.get("Goals", ""),
            "scope": data.get("Scope", ""),
            "opportunity": data.get("Opportunity", ""),
            "constraints": data.get("Constraints", ""),
            "assumptions": data.get("Assumptions", ""),
            "deliverables": data.get("Deliverables", ""),
            "concept_description": data.get("Concept Description", "")
        }
    ]

    # Insert data into the BigQuery table
    errors = client.insert_rows_json(table_id, rows_to_insert)

    # Check for errors during the insertion
    if errors == []:
        print(f"Data inserted successfully into {table_id}")
    else:
        print(f"Encountered errors while inserting rows: {errors}")
        

def generate(document_text, document_id):
    #document = Part.from_data(mime_type="application/pdf", data=base64.b64decode(document_base64))
    vertexai.init(project="globis-code-wizard", location="us-central1")
    model = GenerativeModel("gemini-flash-experimental")
    
    responses = model.generate_content(
        [text1, document_text],
        generation_config=generation_config,
        safety_settings=safety_settings,
        stream=True,
    )

    combined_response_text = ""

    # Iterate through all response texts and concatenate them
    for response in responses:
        try:
            # Try to get the text from the response
            combined_response_text += response.text
        except ValueError as e:
            # Handle specific ValueError when content is blocked by safety filters
            print(f"Warning: Skipping a response due to safety filters: {e}")
            continue

    response_data = []
    try:
        response_data = json.loads(combined_response_text) # Now parse the combined string as a JSON object
    except json.JSONDecodeError as e:
        print(f"Error decoding combined response: {e}")
        print(f"Problematic combined response:")
        print(combined_response_text)
        # Send an email with the error details
        #send_error_email(error_message, combined_response_text)
        pass

    if response_data:
        insert_into_bigquery(response_data, document_id) # Insert the parsed data into the BigQuery table
            

def fetch_and_process_text_from_bigquery(start_row_index = 0):
    # Initialize the BigQuery client
    client = bigquery.Client()

    # SQL query to select all records from the pid_raw_data table
    query = """
        SELECT text, document_key
        FROM `globis-code-wizard.project_data.pid_raw_data`
    """

    # Run the query and fetch results
    query_job = client.query(query)

    # Initialize a counter to track the current row index
    current_row_index = 0

    # Iterate over the result set
    for row in query_job:
        if current_row_index >= start_row_index:
            # Check if a record with this document_key already exists in the table
            check_query = f"""
                SELECT COUNT(1) AS count
                FROM `globis-code-wizard.project_data.project_data_pid`
                WHERE document_id = '{row.document_key}'
            """
            print("document_key", row.document_key)
            check_job = client.query(check_query)
            check_result = check_job.result()
            count = next(check_result).count

            if count == 0:
                # If no record exists, call the generate function
                generate(row.text, row.document_key)
            else:
                print(f"Record with document_key {row.document_key} already exists.")
        current_row_index += 1



fetch_and_process_text_from_bigquery(1000)
