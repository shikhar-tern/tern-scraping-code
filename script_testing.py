import sys
import pandas as pd
import requests
from requests.exceptions import ConnectionError, ReadTimeout
import json
from bs4 import BeautifulSoup
from lxml import html
import time
import re
import os
from datetime import date
from datetime import datetime
from datetime import timedelta
import multiprocessing as mp
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import threading
from io import StringIO
import json
import boto3
from botocore.exceptions import ClientError
import botocore
import s3fs as s3
import spacy
import spacy.cli
# Load spaCy model
import spacy
from spacy.matcher import DependencyMatcher
from spacy.matcher import Matcher, PhraseMatcher
nlp = spacy.load("en_core_web_sm")
import numpy as np
import warnings
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.message import EmailMessage
import ssl
warnings.filterwarnings('ignore')

def remove_keyword_param(url):
    pattern = re.compile(r'keyword=[^&]+&')
    return re.sub(pattern, '', url)


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
    return link.split('?')[0]

def delete_files(x,j):
    dir = '/home/ec2-user/scrape_data'
    os.remove(dir+"/"+x+"/"+j)
    print(f"File {j} removed from {x}")

def fixing_job_ref(code):
    if '<' in str(code):
        soup = BeautifulSoup(str(code), 'html.parser')
        return soup.text
    else:
        return code


def remove_duplicates(input_list):
    seen = set()
    result = []
    for item in input_list:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result

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
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}.csv',Key = f'{x}/{y}.csv')
    print(f'{y} pushed to bucket in {x}')

def fetch_df_from_s3(file):
    s3 = boto3.resource("s3")
    #load from bucket
    obj = s3.Bucket('nhs-dataset').Object(file).get()
    dd = pd.read_csv(obj['Body'])
    return dd

def for_jd_data_list_df(file):
    dd = fetch_df_from_s3(file)
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
    print(f"Done with {file}")
    return dd

def  for_data_list_df(file):
    dd = fetch_df_from_s3(file)
    if 'job_url' in list(dd.columns):
        dd['job_url_hit'] = dd['job_url'].apply(lambda x: remove_keyword_param(x))
        dd['job_code'] = dd['job_url_hit'].apply(lambda x:extract_job_codes(x))
        dd['short_job_link'] = dd['job_url_hit'].apply(lambda x:short_link(x))
        dd['job_reference_number'] = dd['job_reference_number'].apply(lambda x: fixing_job_ref(x))
        dd = dd.drop_duplicates(['scraped_date','job_code'],keep='first').reset_index(drop=True)
        print(f"Done with {file}")
        return dd
    else:
        dd['job_url'] = dd['job_url_hit']
        dd['job_url_hit'] = dd['job_url'].apply(lambda x: remove_keyword_param(x))
        dd['job_code'] = dd['job_url_hit'].apply(lambda x:extract_job_codes(x))
        dd['short_job_link'] = dd['job_url_hit'].apply(lambda x:short_link(x))
        dd['job_reference_number'] = dd['job_reference_number'].apply(lambda x: fixing_job_ref(x))
        dd = dd.drop_duplicates(['scraped_date','job_code'],keep='first').reset_index(drop=True)
        print(f"Done with {file}")
        return dd
    

# jd_master = jd_master_df('job_information_updated','jd_page_data')
    
till_now_jd_master = pd.read_csv(r"/home/ec2-user/scrape_data/master_data/Jobs_Information_Master.csv")
print(till_now_jd_master.shape)
print(till_now_jd_master.head())
print(till_now_jd_master.columns)

till_now_jd_master['scraped_date'] = pd.to_datetime(till_now_jd_master['scraped_date'])
max_date = max(till_now_jd_master['scraped_date'])
max_date = max_date.date()


list_1 = jd_data_list("job_information_updated")
list_2 = data_list("jd_page_data")
need_to_append = pd.DataFrame()
for i in list_1:
    if pd.to_datetime(i.split("/")[-1].split("_")[-1].replace(".csv","")).date() > max_date:
        print(f"Starting with {i}")
        df = for_jd_data_list_df(i)
        need_to_append = pd.concat([need_to_append,df],axis=0,ignore_index=True)
        print(f"Done with {i}")
    else:
        pass
# print(list_1)
for i in list_2:
    if pd.to_datetime(i.split("/")[-1].split("_")[-1].replace(".csv","")).date() > max_date:
        print(f"Starting with {i}")
        df = for_data_list_df(i)
        need_to_append = pd.concat([need_to_append,df],axis=0,ignore_index=True)
        print(f"Done with {i}")
    else:
        pass
# print(list_2)
print(need_to_append.shape)
print(need_to_append.columns)