from googleapiclient.http import MediaFileUpload
from google_drive_service import Create_Service
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import botocore

CLIENT_SECRET_FILE = 'CLIENT_SECRET_FILE.json'
API_NAME = 'drive'
API_VERSION = 'v3'
SCOPES = ['https://www.googleapis.com/auth/drive']

def pulling_list_from_s3(x,y):
    s3 = boto3.resource("s3")
    s3_bucket = s3.Bucket("nhs-dataset")
    dir = x
    files_in_s3 = [f.key.split(dir + "/") for f in s3_bucket.objects.filter(Prefix=dir).all()]
    # Remove the 0th element
    files_in_s3.pop(0)  
    # Flatten the remaining nested lists
    flat_list = [item for sublist in files_in_s3 for item in sublist]
    filtered_list = [item for item in flat_list if item != '']
    #Only picking listing_file
    filt_list = [item for item in filtered_list if item == f'{y}.csv']
    prefixed_list = [f'{x}/' + item for item in filt_list]
    return prefixed_list

def fetching_df(x,y):
    old_listing_data = pd.DataFrame()
    specific_files = pulling_list_from_s3(x,y)
    for file in specific_files:
        #load from bucket
        s3 = boto3.resource("s3")
        obj = s3.Bucket('nhs-dataset').Object(file).get()
        dd = pd.read_csv(obj['Body'], encoding='unicode_escape')
        old_listing_data = pd.concat([old_listing_data,dd],axis=0,ignore_index=True)
    return old_listing_data

df = fetching_df("master_data",'Active_Jobs_with_categorisation')
df.to_excel(r"/home/ec2-user/scrape_data/master_data/Active_Jobs_with_categorisation.xlsx")

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

