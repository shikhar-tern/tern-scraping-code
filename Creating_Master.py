import pandas as pd
import os.path
import glob
import time
from datetime import date
from datetime import datetime
from datetime import timedelta
from bs4 import BeautifulSoup
import requests
import re
import multiprocessing as mp
import concurrent.futures
from requests.exceptions import ConnectionError
from concurrent.futures import ThreadPoolExecutor
import boto3
from botocore.exceptions import ClientError
import botocore
import s3fs as s3


# #### Listing Page Master

# In[2]:


def fixing_job_ref(code):
    if '<' in str(code):
        soup = BeautifulSoup(str(code), 'html.parser')
        return soup.text
    else:
        return code


# In[3]:


# Function to remove the specified pattern from a URL
def remove_keyword_param(url):
    try:
        pattern = re.compile(r'keyword=[^&]+&')
        return re.sub(pattern, '', url)
    except:
        return url


# In[4]:


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


# In[5]:


def short_link(link):
    try:
        a = link.split('?')[0]
    except:
        a = link
    return a


# In[ ]:


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


# In[ ]:


def push_to_s3(x,y):
    print(f'Pushing {y} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    df = pd.read_csv(f"/home/ec2-user/scrape_data/{x}/{y}.csv")
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}.csv',Key = f'{x}/{y}.csv')
    print(f'{y} pushed to bucket in {x}')


# In[ ]:


def listing_page_master_df(x):
    old_listing_data = pd.DataFrame()
    specific_files = data_list(x)
    for file in specific_files:
        s3 = boto3.resource("s3")
        #load from bucket
        obj = s3.Bucket('nhs-dataset').Object(file).get()
        dd = pd.read_csv(obj['Body'],index_col=0)
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
    old_listing_data.to_csv(r"/home/ec2-user/scrape_data/master_data/Listing_Page_Master.csv",index=False)
    push_to_s3('master_data','Listing_Page_Master')
    return old_listing_data


# In[9]:


listing_page_master = listing_page_master_df('listing_page_data')


# #### Merged Master

# In[8]:


def merged_master_df(x):
    old_listing_data = pd.DataFrame()
    specific_files = data_list(x)
    # Print the list of specific files
    for file in specific_files:
        s3 = boto3.resource("s3")
        #load from bucket
        obj = s3.Bucket('nhs-dataset').Object(file).get()
        dd = pd.read_excel(obj['Body'],index_col=0)
        dd['job_url_hit'] = dd['job_url'].apply(lambda x: remove_keyword_param(x))
        dd['job_code'] = dd['job_url_hit'].apply(lambda x:extract_job_codes(x))
        dd['short_job_link'] = dd['job_url_hit'].apply(lambda x:short_link(x))
        dd['job_reference_number'] = dd['job_reference_number'].apply(lambda x: fixing_job_ref(x))
        dd = dd.drop_duplicates(['scrap_date','job_code'],keep='first').reset_index(drop=True) 
        old_listing_data = pd.concat([old_listing_data,dd],axis=0,ignore_index=True)
    old_listing_data.to_csv(r"/home/ec2-user/scrape_data/master_data/Merged_Master.csv",index=False)
    push_to_s3('master_data','Merged_Master')
    return old_listing_data


# In[9]:


merged_master = merged_master_df('master_new_job_data')


# #### New Jobs Master

# In[10]:


def new_jobs_master_df(x):
    old_listing_data = pd.DataFrame()
    specific_files = data_list(x)
    # Print the list of specific files
    for file in specific_files:
        s3 = boto3.resource("s3")
        #load from bucket
        obj = s3.Bucket('nhs-dataset').Object(file).get()
        dd = pd.read_csv(obj['Body'],index_col=0)
        dd['job_url_hit'] = dd['job_url'].apply(lambda x: remove_keyword_param(x))
        dd['job_code'] = dd['job_url_hit'].apply(lambda x:extract_job_codes(x))
        dd['short_job_link'] = dd['job_url_hit'].apply(lambda x:short_link(x))
        dd = dd.drop_duplicates(['scrap_date','job_code'],keep='first').reset_index(drop=True) 
        old_listing_data = pd.concat([old_listing_data,dd],axis=0,ignore_index=True)
    del old_listing_data['keyword']
    old_listing_data.to_csv(r"/home/ec2-user/scrape_data/master_data/New_Jobs_Master.csv",index=False)
    push_to_s3('master_data','New_Jobs_Master')
    return old_listing_data


# In[11]:


new_jobs_master = new_jobs_master_df('new_job_data')


# #### Job Description Master

# In[12]:


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


# In[ ]:


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

specific_files = data_list("job_information_updated")
specific_files


# In[ ]:


def push_to_s3(x,y):
    print(f'Pushing {y} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    df = pd.read_csv(f"/home/ec2-user/scrape_data/{x}/{y}.csv")
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}.csv',Key = f'{x}/{y}.csv')
    print(f'{y} pushed to bucket in {x}')


# In[19]:


def jd_master_df(a,b):
    
    specific_files = jd_data_list(x)
    jd_master = pd.DataFrame()
    for file in specific_files:
        dd = pd.read_csv(file)
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
    
    specific_files = data_list(x)
    for file in specific_files:
        dd = pd.read_csv(file)
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
    
    jd_master.to_csv(r"/home/ec2-user/scrape_data/master_data/Jobs_Information_Master.csv",index=False)
    
    push_to_s3("master_data","Jobs_Information_Master")
    
    return jd_master


# In[20]:


jd_master = jd_master_df('job_information_updated','jd_page_data')







