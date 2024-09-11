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
   "type":"ARRAY",
   "items":{
      "type":"OBJECT",
      "properties":{
         "impact":{
            "type":"INTEGER"
         },
         "probability":{
            "type":"INTEGER"
         },
         "risk":{
            "type":"STRING"
         },
         "mitigation_strategy":{
            "type":"STRING"
         }
      }
   }
}

text1 = "Based on this project information, create a dataset of possible possible risks (short risks), and for each risk calculate the impact (1 to 5), probability (1 to 5) and mitigation strategy."

generation_config= GenerationConfig(
        response_mime_type="application/json", 
        response_schema=response_schema,
        max_output_tokens=8192,
        temperature=1,
    )

def insert_into_bigquery(data, document_id, project_uid):
    # Initialize the BigQuery client
    client = bigquery.Client()

    # Set the table ID (project_id.dataset_id.table_id)
    table_id = 'globis-code-wizard.project_data.risk_data'
    for row in data:
        # Convert the parsed JSON response into a dictionary
        rows_to_insert = [
            {
                "project_uid": project_uid,
                "document_id": document_id,
                "risk": row.get("risk", ""),
                "impact": row.get("impact", ""),
                "probability": row.get("probability", ""),
                "mitigation_strategy": row.get("mitigation_strategy", "")
            }
        ]

        # Insert data into the BigQuery table
        errors = client.insert_rows_json(table_id, rows_to_insert)

        # Check for errors during the insertion
        if errors == []:
            print(f"Data inserted successfully into {table_id}")
        else:
            print(f"Encountered errors while inserting rows: {errors}")
        

def generate(row, document_id, project_uid):
    vertexai.init(project="globis-code-wizard", location="us-central1")
    model = GenerativeModel("gemini-1.5-pro-001")
    
    responses = model.generate_content(
        [text1, row],
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
        insert_into_bigquery(response_data, document_id, project_uid) # Insert the parsed data into the BigQuery table
            

def fetch_and_process_text_from_bigquery(start_row_index = 0):
    # Initialize the BigQuery client
    client = bigquery.Client()

    # SQL query to select all records from the pid_raw_data table
    query = """
        SELECT *
        FROM `globis-code-wizard.project_data.project_data_pid`
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
                FROM `globis-code-wizard.project_data.risk_data`
                WHERE document_id = '{row.document_id}'
            """
            print("document_id", row.document_id)
            check_job = client.query(check_query)
            check_result = check_job.result()
            count = next(check_result).count

            if count == 0:
                # Concatenate all columns into one string
                concatenated_string = ' '.join(str(value) for value in row.values())
                # If no record exists, call the generate function
                generate(concatenated_string, row.document_id, row.uid)
            else:
                print(f"Record with document_key {row.document_id} already exists.")
        current_row_index += 1



fetch_and_process_text_from_bigquery()