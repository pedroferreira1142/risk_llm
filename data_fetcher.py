import requests
from google.cloud import bigquery

def insert_into_pid_raw_data(document_key, text, url):
    # Initialize the BigQuery client
    client = bigquery.Client()

    # Set the table ID (project_id.dataset_id.table_id)
    table_id = 'globis-code-wizard.project_data.pid_raw_data'

    # Convert the parsed JSON response into a dictionary
    rows_to_insert = [
        {
            "document_key": document_key,
            "text": text,
            "url": url
        }
    ]

    # Insert data into the BigQuery table
    errors = client.insert_rows_json(table_id, rows_to_insert)

    # Check for errors during the insertion
    if errors == []:
        print(f"Data inserted successfully into {table_id}")
    else:
        print(f"Encountered errors while inserting rows: {errors}")

# Function to fetch text of all documents from the World Bank API with pagination
def fetch_all_world_bank_document_text():
    base_url = "https://search.worldbank.org/api/v2/wds"
    page_size = 100  # Number of rows per page (fixed at 100 as per your request)
    current_offset = 625  # Start with the first page (offset 0)

    while True:
        # Build the URL with pagination (os parameter controls the offset)
        api_url = f"{base_url}?format=json&docty=Project%20Information%20Document&rows={page_size}&os={current_offset}"

        # Make the GET request to the API
        response = requests.get(api_url)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            
            # If there are no documents or we've reached the end, break the loop
            if 'documents' not in data or len(data['documents']) == 0:
                print("No more documents to fetch.")
                break

            # Loop through each document and fetch the text
            for document_key, document_data in data['documents'].items():
                # Check if the 'txturl' (URL to the text version) exists in the document data
                if 'txturl' in document_data:
                    txt_url = document_data['txturl']
                    print(f"Fetching text for document {document_key} from {txt_url}...")
                    
                    # Fetch the text of the document using the txturl
                    txt_response = requests.get(txt_url)

                    if txt_response.status_code == 200:
                        # Print or store the text content of the document
                        print(f"Text content for document {document_key}:\n{txt_response.text[:500]}...\n")  # Displaying only the first 
                        insert_into_pid_raw_data(document_key, txt_response.text, txt_url)

                    else:
                        print(f"Failed to fetch the document text for {document_key}. Status code: {txt_response.status_code}")
                else:
                    print(f"Text URL not found for document {document_key}.")
        
        else:
            print(f"API request failed with status code {response.status_code}")
            break

        # Move to the next page by incrementing the offset
        current_offset += page_size

# Call the function to fetch and display the document text from all pages
fetch_all_world_bank_document_text()
