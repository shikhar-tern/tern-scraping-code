from googleapiclient.http import MediaFileUpload
from .google import Create_Service
from google_drive_service import Create_Service

CLIENT_SECRET_FILE = 'credentials.json'
API_NAME = 'drive'
API_VERSION = 'v3'
SCOPES = ['https://www.googleapis.com/auth/drive']

service = Create_Service(CLIENT_SECRET_FILE,API_NAME,API_VERSION,SCOPES)

folder_id = '11PyImlmzGi2FeMxhDZC8c5uA3LKxab13'
file_name = 'Active_Jobs_with_categorisation.xlsx'
mine_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
# Upload a file
file_metadata = {
    'name': file_name,
    'parents': [folder_id]
}

media_content = MediaFileUpload(r"/home/ec2-user/scrape_data/master_data/{0}".format(file_name), mimetype=mine_type)

file = service.files().create(
    body=file_metadata,

    media_body=media_content
).execute()

print(file)

