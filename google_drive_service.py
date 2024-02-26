from google.oauth2 import service_account
from googleapiclient.discovery import build


def Create_Service(client_secret_file, api_name, api_version, scopes):
    credentials = service_account.Credentials.from_service_account_file(client_secret_file, scopes=scopes)
    service = build(api_name, api_version, credentials=credentials)
    return service