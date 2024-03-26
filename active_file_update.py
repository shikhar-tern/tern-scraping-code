import pandas as pd
from datetime import date
from datetime import datetime
from datetime import timedelta
import boto3
from botocore.exceptions import ClientError
import botocore
import numpy as np
import warnings
from bs4 import BeautifulSoup
import time
import re
import s3fs as s3
warnings.filterwarnings('ignore')

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
        dd = pd.read_csv(obj['Body'])
        old_listing_data = pd.concat([old_listing_data,dd],axis=0,ignore_index=True)
    return old_listing_data

def push_to_s3(x,y):
    print(f'Pushing {y} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    df = pd.read_csv(f"/home/ec2-user/scrape_data/{x}/{y}.csv")
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}.csv',Key = f'{x}/{y}.csv')
    print(f'{y} pushed to bucket in {x}')

def active_inactive(x):
    if x.days <= 0 :
        return 'closed'
    else:
        return 'active'

def salary_check(x):
    if x == 'Cyflog: Yn dibynnu ar brofiad':
        return ['Depends on experience','Depends on experience']
    else:
        x = x.replace(' to ',' ').replace(' a year','').replace('£','').replace(',','').replace('Cyflog: ','').replace(' an hour','').strip().split(' ')
        if len(x) == 1:
            return [float(x[0]),float(x[0])]
        elif len(x) == 2:
            return [float(x[0]),float(x[1])]
        elif len(x) == 3:
            if x[0] == 'Depends':
                return ['Depends on experience','Depends on experience']
            else:
                return [float(x[0]),float(x[0])]
        elif len(x) == 4:
            return [float(x[0]),float(x[1])]

def fixing_job_ref(code):
    if '<' in str(code):
        soup = BeautifulSoup(str(code), 'html.parser')
        return soup.text
    else:
        return code

# Function to remove the specified pattern from a URL
def remove_keyword_param(url):
    try:
        pattern = re.compile(r'keyword=[^&]+&')
        return re.sub(pattern, '', url)
    except:
        return url

def extract_job_codes(link):
    # Define the regex pattern to match the job code
    pattern = r'/jobadvert/([A-Za-z0-9-]+)\?'
    # Use re.search to find the match in the link
    match = re.search(pattern, link)
    # Check if a match is found and extract the job code
    if match:
        job_code = match.group(1)
        return job_code
    else:
        return '-'

def short_link(link):
    try:
        a = link.split('?')[0]
    except:
        a = link
    return a

# #### Job Description Master
def extract_job_codes_(link):
    # Define the regex pattern to match the job code
    pattern = r'/jobadvert/([A-Za-z0-9-]+)?'
    # Use re.search to find the match in the link
    match = re.search(pattern, link)
    # Check if a match is found and extract the job code
    if match:
        job_code = match.group(1)
        return job_code
    else:
        return '-'

def data_list(x):
    s3 = boto3.resource("s3")
    s3_bucket = s3.Bucket("nhs-dataset")
    dir = x
    files_in_s3 = [f.key.split(dir + "/") for f in s3_bucket.objects.filter(Prefix=dir).all()]
    # Remove the 0th element
    files_in_s3.pop(0)   
    # Flatten the remaining nested lists
    flat_list = [item for sublist in files_in_s3 for item in sublist]   
    filtered_list = [item for item in flat_list if item != '']
    prefixed_list = [f'{x}/' + item for item in filtered_list]
    return prefixed_list

def jd_data_list(x):
    s3 = boto3.resource("s3")
    s3_bucket = s3.Bucket("nhs-dataset")
    dir = x
    files_in_s3 = [f.key.split(dir + "/") for f in s3_bucket.objects.filter(Prefix=dir).all()]
    # Remove the 0th element
    files_in_s3.pop(0)
    # Flatten the remaining nested lists
    flat_list = [item for sublist in files_in_s3 for item in sublist]
    filtered_list = [item for item in flat_list if item != '']
    prefixed_list = [f'{x}/' + item for item in filtered_list]
    pattern = re.compile(r'/job_information_updated_\d{4}-\d{2}-\d{2}\.csv$')
    filtered_list_2 = [element for element in prefixed_list if pattern.search(element)]
    return filtered_list_2

def jd_master_df(a,b):
    # print('Starting with Job Description')
    specific_files = jd_data_list(a)
    jd_master = pd.DataFrame()
    for file in specific_files:
        s3 = boto3.resource("s3")
        #load from bucket
        obj = s3.Bucket('nhs-dataset').Object(file).get()
        dd = pd.read_csv(obj['Body'])
        dd['job_url'] = dd['job_url_hit']
        df_not_null = dd[(dd['job_url_hit'].notnull())]
        df_is_null = dd[(dd['job_url_hit'].isnull())]
        df_is_null['job_url_hit'] = 'https://www.jobs.nhs.uk/candidate/jobadvert/' + df_is_null['job_reference_number']
        dd = pd.concat([df_not_null,df_is_null],axis=0,ignore_index=True)
        dd['job_url_hit'] = dd['job_url'].apply(lambda x: remove_keyword_param(x))
        dd['job_code'] = dd['job_url_hit'].apply(lambda x:extract_job_codes_(x))
        dd['short_job_link'] = dd['job_url_hit'].apply(lambda x:short_link(x))
        dd['job_reference_number'] = dd['job_reference_number'].apply(lambda x: fixing_job_ref(x))
        dd = dd.drop_duplicates(['scraped_date','job_url_hit'],keep='first').reset_index(drop=True)
        jd_master = pd.concat([jd_master,dd],axis=0,ignore_index=True)
    specific_files = data_list(b)
    for file in specific_files:
        s3 = boto3.resource("s3")
        #load from bucket
        obj = s3.Bucket('nhs-dataset').Object(file).get()
        dd = pd.read_csv(obj['Body'])
        if 'job_url' in list(dd.columns):
            dd['job_url_hit'] = dd['job_url'].apply(lambda x: remove_keyword_param(x))
            dd['job_code'] = dd['job_url_hit'].apply(lambda x:extract_job_codes(x))
            dd['short_job_link'] = dd['job_url_hit'].apply(lambda x:short_link(x))
            dd['job_reference_number'] = dd['job_reference_number'].apply(lambda x: fixing_job_ref(x))
            dd = dd.drop_duplicates(['scraped_date','job_code'],keep='first').reset_index(drop=True)
            jd_master = pd.concat([jd_master,dd],axis=0,ignore_index=True)
        else:
            dd['job_url'] = dd['job_url_hit']
            dd['job_url_hit'] = dd['job_url'].apply(lambda x: remove_keyword_param(x))
            dd['job_code'] = dd['job_url_hit'].apply(lambda x:extract_job_codes(x))
            dd['short_job_link'] = dd['job_url_hit'].apply(lambda x:short_link(x))
            dd['job_reference_number'] = dd['job_reference_number'].apply(lambda x: fixing_job_ref(x))
            dd = dd.drop_duplicates(['scraped_date','job_code'],keep='first').reset_index(drop=True)
            jd_master = pd.concat([jd_master,dd],axis=0,ignore_index=True)
    del jd_master['page_number']
    # jd_master.to_csv(r"/home/ec2-user/scrape_data/master_data/Jobs_Information_Master.csv",index=False)
    # push_to_s3("master_data","Jobs_Information_Master")
    return jd_master

def update_information(jd_master,listing_all_df):
    start_time = time.time()
    # jd_master = pd.read_csv(r"/home/ec2-user/scrape_data/master_data/Jobs_Information_Master.csv")
    ### Update Code
    jd_master['scraped_date'] = pd.to_datetime(jd_master['scraped_date'],format='ISO8601')
    # Assuming 'closing_date' is the column with date strings
    jd_master['date_posted'] = pd.to_datetime(
        jd_master['date_posted'], 
        infer_datetime_format=True, 
        errors='coerce')
    jd_master.drop_duplicates('short_job_link',keep='last',inplace=True)
    jd_master.reset_index(drop=True,inplace=True)
    # Convert the datetime column to the desired 'yyyy-mm-dd' format
    jd_master['date_posted'] = pd.to_datetime(jd_master['date_posted'].dt.strftime('%Y-%m-%d'))
    listing_all_df['scrap_date'] = pd.to_datetime(listing_all_df['scrap_date'],format='ISO8601')
    # Assuming 'closing_date' is the column with date strings
    listing_all_df['closing_date'] = pd.to_datetime(
        listing_all_df['closing_date'], 
        infer_datetime_format=True, 
        errors='coerce')
    # Convert the datetime column to the desired 'yyyy-mm-dd' format
    listing_all_df['closing_date'] = pd.to_datetime(listing_all_df['closing_date'].dt.strftime('%Y-%m-%d'))
    listing_all_df.drop_duplicates(['short_job_link'],keep="last",inplace=True)
    listing_all_df.reset_index(drop=True,inplace=True)
    listing_all_df['Today_Date'] = str(date.today())
    listing_all_df['Today_Date'] = pd.to_datetime(listing_all_df['Today_Date'])
    listing_all_df['Days_to_close'] = listing_all_df['closing_date'] - listing_all_df['Today_Date']
    listing_all_df['active_inactive'] = listing_all_df['Days_to_close'].apply(lambda x: active_inactive(x))                                             
    listing_all_df['salary_range_'] = listing_all_df['salary'].apply(lambda x: salary_check(x))
    listing_all_df[['salary_range_start','salary_range_end']] = pd.DataFrame(listing_all_df.salary_range_.tolist(), index= listing_all_df.index)
    del listing_all_df['salary_range_']
    listing_all_df['salary_range_start'] = listing_all_df['salary_range_start'].replace('Depends on experience','-')
    listing_all_df['salary_range_end'] = listing_all_df['salary_range_end'].replace('Depends on experience','-')
    print("Starting with Active File")
    active_jobs = listing_all_df[listing_all_df['active_inactive']=='active']
    active_jobs_ = active_jobs[['Role','salary','closing_date','job_code','Today_Date','Days_to_close', 'active_inactive', 'salary_range_start','salary_range_end','short_job_link','scrap_date']]
    active_jobs_2 = active_jobs_.merge(jd_master[['job_summary', 'job_discription','band', 'employer_name',
        'employer_address', 'employer_post_code', 'employer_website',
        'contact_person_position', 'contact_person_name',
        'contact_person_email', 'contact_person_number','short_job_link']],on ='short_job_link',how='left')
    active_jobs_2.reset_index(drop=True,inplace=True)
    # active_jobs_2.to_csv(r"/home/ec2-user/scrape_data/master_data/Active_Jobs.csv",index=False)
    # listing_all_df.to_csv(r"/home/ec2-user/scrape_data/master_data/Latest_Updated_Master.csv",index=False)
    push_to_s3("master_data","Active_Jobs")
    push_to_s3("master_data","Latest_Updated_Master") 
    end_time = time.time()
    duration = end_time - start_time
    print(f"Time taken to create Active Jobs: {duration/60} mintues")
    return active_jobs_2

jd_master = fetching_df('master_data','Jobs_Information_Master')
# jd_master = pd.read_csv(r"/home/ec2-user/scrape_data/master_data/Jobs_Information_Master.csv")
listing_all_df = fetching_df('master_data','Listing_Page_Master')
# listing_all_df = pd.read_csv(r"/home/ec2-user/scrape_data/master_data/Listing_Page_Master.csv")
active_jobs = update_information(jd_master,listing_all_df)
