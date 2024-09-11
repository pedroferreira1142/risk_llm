def download_pdf_as_base64(bucket_name, blob):
    """Downloads a PDF from GCS and converts it to a Base64-encoded string."""
    # Download the PDF content as bytes
    pdf_bytes = blob.download_as_bytes()

    # Convert the PDF bytes to Base64-encoded string
    pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')

    return pdf_base64

def process_pdfs_in_bucket(bucket_name):
    """Finds all PDF files in the bucket, converts them to Base64, and prints the Base64 string."""
    # Get a list of all PDF blobs
    pdf_blobs = list_pdfs_in_bucket(bucket_name)
    
    # Loop through each PDF blob and print its Base64-encoded content
    for blob in pdf_blobs:
        print(f"Processing PDF file: {blob.name}")
        pdf_base64 = download_pdf_as_base64(bucket_name, blob)
        #print(f"Base64 of {blob.name}:\n{pdf_base64}\n")
        generate(pdf_base64)


def list_pdfs_in_bucket(bucket_name):
    """Lists all PDF files in the given bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # List all blobs (files) in the bucket
    blobs = storage_client.list_blobs(bucket_name)
    
    # Filter blobs that end with '.pdf'
    pdf_blobs = [blob for blob in blobs if blob.name.endswith('.pdf')]
    
    return pdf_blobs