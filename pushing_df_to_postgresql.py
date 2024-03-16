# sudo -u postgres psql -> CONNECT TO POSTGRESQL ON SERVER
import pandas as pd
import psycopg2
import json

# Specify the path to your JSON file
file_path = r"/home/ec2-user/tern-scraping-code/postgresql_credentials.json"

# Read the JSON file and convert it to a dictionary
with open(file_path, 'r') as json_file:
    conn_cred = json.load(json_file)


def establising_connection(x,conn_cred):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        database=x,
        user=conn_cred['user'],
        password=conn_cred['password'],
        host=conn_cred['host'],
        port=conn_cred['port']
    )
    print("Connection Established")

    print("Going to Close Connection")
    # Close the cursor and connection
    conn.close()

establising_connection('scrappydb',conn_cred)