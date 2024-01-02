import sys
import pandas as pd
import requests
from requests.exceptions import ConnectionError, ReadTimeout
import json
from bs4 import BeautifulSoup
from lxml import html
import time
import re
from datetime import date
from datetime import datetime
from datetime import timedelta
import multiprocessing as mp
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import time
import threading

from io import StringIO
import json

import os
import glob

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.message import EmailMessage
import ssl

import boto3
from botocore.exceptions import ClientError
import botocore
import s3fs as s3

# ##### email new jobs

# Specify the path to your JSON file
file_path = r"/home/ec2-user/tern-scraping-code/email_credentials.json"

# Read the JSON file and convert it to a dictionary
with open(file_path, 'r') as json_file:
    email_cred = json.load(json_file)


# Define a function to apply color and style formatting to the cells
def format_row(row):
    return f"""
        <tr>
            <td style="padding: 12px 15px; background-color: {'#D6EEEE' if row['New Jobs Count'] % 2 == 0 else '#D6EEEE'}; border: 2px solid #ddd; font-weight: {'bold' if row.name == 'Keywords' else 'normal'}; text-align: center;">{row['Keywords']}</td>
            <td style="padding: 12px 15px; background-color: {'#D6EEEE' if row['New Jobs Count'] % 2 == 0 else '#D6EEEE'}; border: 2px solid #ddd; font-weight: {'bold' if row.name == 'Keywords' else 'normal'}; text-align: center;">{row['New Jobs Count']}</td>
        </tr>
    """

df = pd.read_csv(r"/home/ec2-user/scrape_data/master_data/Active_Jobs_with_categorisation.csv")

def email_people(email_cred,df):
    # Load your DataFrame from the CSV file
    new_csv_file_path = r"/home/ec2-user/scrape_data/master_data/Active_Jobs_with_categorisation.csv"
    # Email configuration
    sender_email = email_cred['email']
    sender_password = email_cred['password']
    # receiver_email = ["shikharrajput@gmail.com","safal.verma@tern-group.com"]
    receiver_email = ["shikharrajput@gmail.com","safal.verma@tern-group.com","ashita@tern-group.com","akshay.rao@tern-group.com"]

    subject = f"New Job on {str(date.today())}"
    
    # Create the email body with a formatted table
    body = f"""
    <html>
    <body>
      <p>Hi,</p>

      <p> As of today {str(date.today())} there are {df.shape[0]} active jobs on NHS </p>

      <p>PFA.</p>

      <p>Best regards,<br>Your Name</p>
    </body>
    </html>
    """
    
    # Create the email message
    message = MIMEMultipart()
    message.attach(MIMEText(body, 'html'))
    
    # Attach the new CSV file
    with open(new_csv_file_path, 'rb') as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename=Active_Jobs_with_categorisation')
        message.attach(part)
    
    # Create the email message
    message['From'] = sender_email
    message['To'] = ", ".join(receiver_email)
    message['Subject'] = subject

    context = ssl.create_default_context()

    # Connect to the SMTP server and send the email
    smtp_server = "smtp.gmail.com"
    smtp_port = 465

    with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
        server.login(sender_email, sender_password)
        server.send_message(message)

    print("Email sent successfully!")

email_people(email_cred,df)
