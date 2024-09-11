import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_error_email(error_message, combined_response_text):
    sender_email = "pedroferreira1142@gmail.com"  # Replace with your email
    receiver_email = "pedroferreira1142@gmail.com"
    password = "ouui uovk ijxt hznd"  # Replace with your email account password or app password

    # Create the email content
    subject = "Error decoding combined response"
    body = f"An error occurred while decoding the JSON response.\n\nError message: {error_message}\n\nProblematic response:\n{combined_response_text}"

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    # Sending the email
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, password)
        text = msg.as_string()
        server.sendmail(sender_email, receiver_email, text)
        server.quit()
        print("Error email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")