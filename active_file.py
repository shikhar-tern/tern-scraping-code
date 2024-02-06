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
import boto3
from botocore.exceptions import ClientError
import botocore
import s3fs as s3
# import spacy
# import spacy.cli
# # Load spaCy model
# nlp = spacy.load("en_core_web_sm")
# from spacy.matcher import PhraseMatcher
# from spacy.matcher import DependencyMatcher
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# #### Listing Page Master

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


def push_to_s3(x,y):
    print(f'Pushing {y} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    df = pd.read_csv(f"/home/ec2-user/scrape_data/{x}/{y}.csv")
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}.csv',Key = f'{x}/{y}.csv')
    print(f'{y} pushed to bucket in {x}')

def listing_page_master_df(x):
    print('Starting with Listing Page')
    old_listing_data = pd.DataFrame()
    specific_files = data_list(x)
    for file in specific_files:
        s3 = boto3.resource("s3")
        #load from bucket
        obj = s3.Bucket('nhs-dataset').Object(file).get()
        dd = pd.read_csv(obj['Body'])
        try:
            del dd['Unnamed: 0']
        except:
            pass
        dd['job_url_hit'] = dd['job_url'].apply(lambda x: remove_keyword_param(x))
        dd['scrap_date'] = file.split('all_')[-1].strip('.csv')
        dd['job_code'] = dd['job_url_hit'].apply(lambda x:extract_job_codes(x))
        dd['short_job_link'] = dd['job_url_hit'].apply(lambda x:short_link(x))
        dd = dd.drop_duplicates(['job_url_hit'],keep='first').reset_index(drop=True)
        try:
            del dd['keyword']
        except:
            pass
        old_listing_data = pd.concat([old_listing_data,dd],axis=0,ignore_index=True)
    # old_listing_data.to_csv(r"/home/ec2-user/scrape_data/master_data/Listing_Page_Master.csv",index=False)
    # push_to_s3('master_data','Listing_Page_Master')
    return old_listing_data

listing_page_master = listing_page_master_df('listing_page_data')
listing_page_master['scrap_date'] = pd.to_datetime(listing_page_master['scrap_date'])
# Assuming 'closing_date' is the column with date strings
listing_page_master['closing_date'] = pd.to_datetime(
    listing_page_master['closing_date'], 
    infer_datetime_format=True, 
    errors='coerce')
# Convert the datetime column to the desired 'yyyy-mm-dd' format
listing_page_master['closing_date'] = pd.to_datetime(listing_page_master['closing_date'].dt.strftime('%Y-%m-%d'))


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


def push_to_s3(x,y):
    print(f'Pushing {y} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    df = pd.read_csv(f"/home/ec2-user/scrape_data/{x}/{y}.csv")
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}.csv',Key = f'{x}/{y}.csv')
    print(f'{y} pushed to bucket in {x}')


def jd_master_df(a,b):
    print('Starting with Job Description')
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


jd_master = jd_master_df('job_information_updated','jd_page_data')

### Update Code
jd_master['scraped_date'] = pd.to_datetime(jd_master['scraped_date'])
# Assuming 'closing_date' is the column with date strings
jd_master['date_posted'] = pd.to_datetime(
    jd_master['date_posted'], 
    infer_datetime_format=True, 
    errors='coerce')

# Convert the datetime column to the desired 'yyyy-mm-dd' format
jd_master['date_posted'] = pd.to_datetime(jd_master['date_posted'].dt.strftime('%Y-%m-%d'))


def active_inactive(x):
    if x.days <= 0 :
        return 'closed'
    else:
        return 'active'

def active_job_data_list(x):
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
    filt_list = [item for item in filtered_list if item == 'Active_Jobs.csv']
    prefixed_list = [f'{x}/' + item for item in filt_list]
    return prefixed_list

def pull_active_jobs_append_new_job_list(x):
    specific_files = active_job_data_list(x)
    active_jobs_df = pd.DataFrame()
    for file in specific_files:
        s3 = boto3.resource("s3")
        #load from bucket
        obj = s3.Bucket('nhs-dataset').Object(file).get()
        dd = pd.read_csv(obj['Body'])
        active_jobs_df = pd.concat([active_jobs_df,dd],axis=0,ignore_index=True)
    active_job_list = list(set(active_jobs_df['short_job_link'].unique()))
    return active_job_list

def update_information(link,job_lists):
    print(f"Started with: {link}")
    # job_lists = list(listing_page_master['short_job_link'].unique())
    a = listing_page_master[listing_page_master['short_job_link']==link].sort_values('scrap_date')
    b = jd_master[jd_master['short_job_link']==link].sort_values('scraped_date')
    
    if len(a) == 1  and (list(a['scrap_date'])[0] in list(b['scraped_date'])):
        c = a[['Role','salary','closing_date', 'job_code', 'short_job_link','scrap_date']].merge(b[['scraped_date','date_posted']],left_on='scrap_date',right_on='scraped_date',how='outer') #<---- check how
        c['Role'] = c['Role'].fillna(method='ffill')
        c['salary'] = c['salary'].fillna(method='ffill')
        c['closing_date'] = c['closing_date'].fillna(method='ffill')
        c['job_code'] = c['job_code'].fillna(method='ffill')
        c['short_job_link'] = c['short_job_link'].fillna(method='ffill')
        c['scrap_date'] = c['scrap_date'].fillna(method='ffill')

        # Assuming df is your DataFrame
        c['scraped_date'] = c['scraped_date'].fillna(method='ffill')
        c['date_posted'] = c['date_posted'].fillna(method='ffill')

        c['Today_Date'] = str(date.today())
        c['Today_Date'] = pd.to_datetime(c['Today_Date'])

        #days to close
        c['Days_to_close'] = c['closing_date'] - c['Today_Date']

        #job_active_inactive_flag
        c['active_inactive'] = c['Days_to_close'].apply(lambda x: active_inactive(x))

        #Latested date of update
        c['Updated_on'] = str(max(c['scraped_date']).date())

        c['salary_changed'] = c['salary'] != c['salary'].shift(1)

        c['latest_salary'] = c['salary'].where(c['salary_changed'], None)

        c['salary_change_date'] = c['scrap_date'].where(c['salary_changed'], pd.NaT)

        # Fill NaN values in 'latest_salary' and 'salary_change_date'
        c['latest_salary'] = c['latest_salary'].ffill()
        c['salary_change_date'] = c['salary_change_date'].ffill()

        c = c.drop(columns=['salary_changed'])

        print(f"{job_lists.index(link)} done out of {len(job_lists)}")

        return c

    elif (len(a)==1) and (list(a['scrap_date'])[0] not in list(b['scraped_date'])):
        c = a[['Role','salary','closing_date', 'job_code', 'short_job_link','scrap_date']].merge(b[['scraped_date','date_posted']],left_on='scrap_date',right_on='scraped_date',how='outer') #<---- check how
        c['Role'] = c['Role'].fillna(method='ffill')
        c['salary'] = c['salary'].fillna(method='ffill')
        c['closing_date'] = c['closing_date'].fillna(method='ffill')
        c['job_code'] = c['job_code'].fillna(method='ffill')
        c['short_job_link'] = c['short_job_link'].fillna(method='ffill')
        c['scrap_date'] = c['scrap_date'].fillna(method='ffill')

        # Assuming df is your DataFrame
        c['scraped_date'] = c['scraped_date'].fillna(c['scrap_date'])
        c['date_posted'] = c['date_posted'].fillna(method='bfill')

        c['Today_Date'] = str(date.today())
        c['Today_Date'] = pd.to_datetime(c['Today_Date'])

        #days to close
        c['Days_to_close'] = c['closing_date'] - c['Today_Date']

        #job_active_inactive_flag
        c['active_inactive'] = c['Days_to_close'].apply(lambda x: active_inactive(x))

        #Latested date of update
        c['Updated_on'] = str(max(c['scraped_date']).date())

        c['salary_changed'] = c['salary'] != c['salary'].shift(1)

        c['latest_salary'] = c['salary'].where(c['salary_changed'], None)

        c['salary_change_date'] = c['scrap_date'].where(c['salary_changed'], pd.NaT)

        # Fill NaN values in 'latest_salary' and 'salary_change_date'
        c['latest_salary'] = c['latest_salary'].ffill()
        c['salary_change_date'] = c['salary_change_date'].ffill()

        c = c.drop(columns=['salary_changed'])

        print(f"{job_lists.index(link)} done out of {len(job_lists)}")

        return c

    elif (len(a)>1) and (len(b)==0):
        c = a[['Role','salary','closing_date', 'job_code', 'short_job_link','scrap_date']].merge(b[['scraped_date','date_posted']],left_on='scrap_date',right_on='scraped_date',how='left') #<---- check how
        # Assuming df is your DataFrame
        c['scraped_date'] = c['scraped_date'].fillna(method='ffill')
        c['date_posted'] = c['date_posted'].fillna(method='ffill')

        c['Today_Date'] = str(date.today())
        c['Today_Date'] = pd.to_datetime(c['Today_Date'])

        #days to close
        c['Days_to_close'] = c['closing_date'] - c['Today_Date']

        #job_active_inactive_flag
        c['active_inactive'] = c['Days_to_close'].apply(lambda x: active_inactive(x))

        #Latested date of update
        c['Updated_on'] = str(max(c['scrap_date']).date())

        c['salary_changed'] = c['salary'] != c['salary'].shift(1)

        c['latest_salary'] = c['salary'].where(c['salary_changed'], None)

        c['salary_change_date'] = c['scrap_date'].where(c['salary_changed'], pd.NaT)

        # Fill NaN values in 'latest_salary' and 'salary_change_date'
        c['latest_salary'] = c['latest_salary'].ffill()
        c['salary_change_date'] = c['salary_change_date'].ffill()

        c = c.drop(columns=['salary_changed'])

        print(f"{job_lists.index(link)} done out of {len(job_lists)}")

        return c

    elif (len(a)>1) and (any(elem in list(b['scraped_date']) for elem in list(a['scrap_date']))):
        c = a[['Role','salary','closing_date', 'job_code', 'short_job_link','scrap_date']].merge(b[['scraped_date','date_posted']],left_on='scrap_date',right_on='scraped_date',how='left') #<---- check how
        # Assuming df is your DataFrame
        c['scraped_date'] = c['scraped_date'].fillna(c['scrap_date'])
        c['date_posted'] = c['date_posted'].fillna(method='bfill')

        c['Today_Date'] = str(date.today())
        c['Today_Date'] = pd.to_datetime(c['Today_Date'])

        #days to close
        c['Days_to_close'] = c['closing_date'] - c['Today_Date']

        #job_active_inactive_flag
        c['active_inactive'] = c['Days_to_close'].apply(lambda x: active_inactive(x))

        #Latested date of update
        c['Updated_on'] = str(max(c['scraped_date']).date())

        c['salary_changed'] = c['salary'] != c['salary'].shift(1)

        c['latest_salary'] = c['salary'].where(c['salary_changed'], None)

        c['salary_change_date'] = c['scrap_date'].where(c['salary_changed'], pd.NaT)

        # Fill NaN values in 'latest_salary' and 'salary_change_date'
        c['latest_salary'] = c['latest_salary'].ffill()
        c['salary_change_date'] = c['salary_change_date'].ffill()

        c = c.drop(columns=['salary_changed'])

        print(f"{job_lists.index(link)} done out of {len(job_lists)}")

        return c

    elif (len(a)>1) and (len(b)>=1) and (any(elem not in list(b['scraped_date']) for elem in list(a['scrap_date']))):
        c = a[['Role','salary','closing_date', 'job_code', 'short_job_link','scrap_date']].merge(b[['scraped_date','date_posted']],left_on='scrap_date',right_on='scraped_date',how='outer') #<---- check how

        c['Role'] = c['Role'].fillna(method='ffill')
        c['salary'] = c['salary'].fillna(method='ffill')
        c['closing_date'] = c['closing_date'].fillna(method='ffill')
        c['job_code'] = c['job_code'].fillna(method='ffill')
        c['short_job_link'] = c['short_job_link'].fillna(method='ffill')
        c['scrap_date'] = c['scrap_date'].fillna(method='ffill')
        # Assuming df is your DataFrame
        c['scraped_date'] = c['scraped_date'].fillna(c['scrap_date'])
        c['date_posted'] = c['date_posted'].fillna(method='bfill')

        c['Today_Date'] = str(date.today())
        c['Today_Date'] = pd.to_datetime(c['Today_Date'])

        #days to close
        c['Days_to_close'] = c['closing_date'] - c['Today_Date']

        #job_active_inactive_flag
        c['active_inactive'] = c['Days_to_close'].apply(lambda x: active_inactive(x))

        #Latested date of update
        c['Updated_on'] = str(max(c['scraped_date']).date())

        c['salary_changed'] = c['salary'] != c['salary'].shift(1)

        c['latest_salary'] = c['salary'].where(c['salary_changed'], None)

        c['salary_change_date'] = c['scrap_date'].where(c['salary_changed'], pd.NaT)

        # Fill NaN values in 'latest_salary' and 'salary_change_date'
        c['latest_salary'] = c['latest_salary'].ffill()
        c['salary_change_date'] = c['salary_change_date'].ffill()

        c = c.drop(columns=['salary_changed'])

        print(f"{job_lists.index(link)} done out of {len(job_lists)}")

        return c

def process_link(link):
    new_update = update_information(link,job_lists)
    return new_update, new_update.tail(1)

start_time = time.time()
print(str(datetime.now()))
job_updates = pd.DataFrame()
latest_job_updates = pd.DataFrame()
if __name__ == '__main__':
    # Define the URLs
    job_lists = pull_active_jobs_append_new_job_list('master_data')

    results_list = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=mp.cpu_count()-1) as executor:
        # Submit the scraping function to the executor for each page number
        results = list(executor.map(process_link, job_lists))
        
        for i, (new_update, latest_update) in enumerate(results):
            if new_update is not None and latest_update is not None:
                job_updates = pd.concat([job_updates, new_update], axis=0, ignore_index=True)
                latest_job_updates = pd.concat([latest_job_updates, latest_update], axis=0, ignore_index=True)
                print(f"Result {i + 1} processed and appended")
            else:
                print(f"Result {i + 1} is None. Skipping...")

    job_updates.to_csv(r"/home/ec2-user/scrape_data/master_data/Job_Information_Full_Updated_Master.csv",index=False)
    latest_job_updates.to_csv(r"/home/ec2-user/scrape_data/master_data/Latest_Updated_Master.csv",index=False)
    jd_latest = jd_master[jd_master['scraped_date']==max(jd_master['scraped_date'])]
    active_job = latest_job_updates[latest_job_updates['active_inactive']=='active']
    df_fin = active_job.merge(jd_latest[['job_summary', 'job_discription', 'employer_name',
       'employer_address', 'employer_post_code', 'employer_website',
       'contact_person_position', 'contact_person_name',
       'contact_person_email', 'contact_person_number','short_job_link']],on ='short_job_link',how='left')
    df_fin.to_csv(r"/home/ec2-user/scrape_data/master_data/Active_Jobs.csv",index=False)

    push_to_s3("master_data","Job_Information_Full_Updated_Master")
    push_to_s3("master_data","Latest_Updated_Master")  
    push_to_s3("master_data","Active_Jobs")        
end_time = time.time()
duration = end_time - start_time
print(f"Time taken: {duration/60} mintues")
