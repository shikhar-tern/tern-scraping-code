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
    try:
        # Connect to your postgres DB
        conn = psycopg2.connect(
            dbname=x,
            user=conn_cred['user'],
            password=conn_cred['password'],
            host=conn_cred['host'],
            port=conn_cred['port']
        )
        # # Open a cursor to perform database operations
        # with conn.cursor() as cursor:
        #     # Execute a query
        #     cursor.execute("SELECT version();")

        #     # Retrieve query results
        #     records = cursor.fetchone()
        #     print(f"Connected to - {records}")
        print("Connection Established")
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        print("Close Connection")
        # Close connection
        if conn:
            conn.close()
            
establising_connection('scrappydb',conn_cred)

