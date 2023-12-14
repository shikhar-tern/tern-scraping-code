#!/usr/bin/env python
# coding: utf-8

# In[ ]:


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
# import s3transfer as s3

### Listing Page

# number of pages

def get_max_page_number(x):
    # Define the URL
    url = x

    # Send a GET request to the URL
    response = requests.get(url)

    if response.status_code == 200:
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find the pagination element
        pagination = soup.find_all('ul', class_='nhsuk-pagination__list')

        # Find all the page number links within the pagination element
        page_links = pagination[0].find_all('a', class_='nhsuk-pagination__link')

        # Extract the maximum page number
        max_page_number = int(page_links[0].text.strip().split('of ')[-1])

        print(f"Number of pages: {max_page_number}")
        return max_page_number

    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")

max_page_number = get_max_page_number("https://www.jobs.nhs.uk/candidate/search/results?language=en")

def scrape_url(page_number, request_timeout=30):
    max_retries = 3  # Number of maximum retries
    delay = 2  # Delay between retries in seconds
    retries = 0

    while retries < max_retries:
        try:
            #URL
            urls = f"https://www.jobs.nhs.uk/candidate/search/results?language=en&page={page_number}"

            # Send a GET request to the URL
            response = requests.get(urls, timeout=request_timeout)

            job_title_list = []
            job_title_link_list = []
            compass_list = []
            salary_list = []
            closing_date_list = []
            contract_type_list = []
            working_pattern_list = []

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Parse the HTML content
                soup = BeautifulSoup(response.text, 'html.parser')

                # Find all ul elements with the specified class
                ul_elements = soup.find_all('ul', class_='nhsuk-list search-results')

                # Iterate through the ul elements
                for ul_element in ul_elements:
                    # Find all li elements with the specified class and data-test attribute
                    li_elements = ul_element.find_all('li', class_='nhsuk-list-panel search-result nhsuk-u-padding-3', attrs={'data-test': 'search-result'})

                    # Check if any matching li elements were found
                    if li_elements:
                        # Iterate through the li elements and print their text
                        for li_element in li_elements:
                            #job_title
                            try:
                                job_title = li_element.find_all('div',class_ = 'nhsuk-grid-column-two-thirds')[0].text.strip()
                                job_title_list.append(job_title)
                            except:
                                job_title_list.append('-')

                            #job_title_link
                            try:
                                job_title_link = 'https://www.jobs.nhs.uk' + str(li_element.find('a')['href'])
                                job_title_link_list.append(job_title_link)
                            except:
                                job_title_link_list.append('-')

                            #Compass
                            try:
                                zz = li_element.find_all('h3',class_='nhsuk-u-font-weight-bold')[0].text.strip().split('\n')
                                new_zz = []
                                for i in zz:
                                    if i == '        ' or i =='       ' or i.strip() == 'Compass':
                                        pass
                                    else:
                                        new_zz.append(i.strip())
                                if len(new_zz)>1:
                                    Compass = ' - '.join(new_zz)
                                else:
                                    Compass = new_zz[0]
                                compass_list.append(Compass)
                            except:
                                compass_list.append('-')

                            #Salary
                            try:
                                salary = li_element.find_all('li',class_='marginBt')[0].text.strip('Salary: ').replace('\n            \n','').replace('            ',' ').strip()
                                salary_list.append(salary)
                            except:
                                salary_list.append('-')

                            #closing_date
                            try:
                                closing_date = li_element.find_all('li', attrs={'data-test': 'search-result-closingDate'})[0].text.strip().split('Closing date: ')[-1]
                                closing_date_list.append(closing_date)
                            except:
                                closing_date_list.append('-')

                            #contrat type
                            try:
                                contract_type = li_element.find_all('li', attrs={'data-test': 'search-result-jobType'})[0].text.strip().split('Contract type: ')[-1]
                                contract_type_list.append(contract_type)
                            except:
                                contract_type_list.append('-')

                            #working pattern
                            try:
                                working_pattern = li_element.find_all('li', attrs={'data-test': 'search-result-workingPattern'})[0].text.strip().split('Working pattern: ')[-1]
                                working_pattern_list.append(working_pattern)
                            except:
                                working_pattern_list.append('-')

                            #                 print('-------------------------------------------------------------------------')
                    else:
                        print("No matching li elements found inside the ul element.")
            else:
                print(f"Failed to retrieve the page. Status code: {response.status_code}")

            df = pd.DataFrame(list(zip(job_title_list, job_title_link_list,compass_list,salary_list,closing_date_list,contract_type_list,working_pattern_list)),
                           columns =['job_title', 'job_title_link','compass','salary','closing_date','contract_type','working_pattern'])
            df['Page_Number'] = page_number
            df['url_hit'] = urls
            print(page_number,'done')
            return df
        except (ConnectionError, ReadTimeout) as e:
            print(f"ConnectionError: {e}. Retrying... {urls}")
            time.sleep(2 ** retries)  # Wait for a few seconds before retrying
            retries += 1
            continue

    print(f"Failed to retrieve data from URL: {urls}")
    return None

# Function to remove the specified pattern from a URL
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

def saving_listing_df(final_data):
    final_data['job_url_hit'] = final_data['job_url'].apply(lambda x: remove_keyword_param(x))
    final_data['job_code'] = final_data['job_url_hit'].apply(lambda x:extract_job_codes(x))
    final_data['short_job_link'] = final_data['job_url_hit'].apply(lambda x:short_link(x))
    final_data = final_data.drop_duplicates('short_job_link',keep='first').reset_index(drop=True)
    final_data.to_csv(r"/home/ec2-user/scrape_data/listing_page_data/listing_page_all_{}.csv".format(str(date.today())),index=False)
    return final_data

start_time = time.time()
print(str(datetime.now()))
final_data = pd.DataFrame()
if __name__ == '__main__':

    max_page_numbers = max_page_number
    
    results_list = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=mp.cpu_count()-1) as executor:
        results = list(executor.map(scrape_url, range(1, max_page_numbers + 1)))
        
        for i, result in enumerate(results):
            if result is not None:
#                 print(result)
                results_list.append(result)                      
                print(f"Result {i + 1} appended")
            else:
                print(f"Result {i + 1} is None. Skipping...")
    print("All results processed.")
    print("Creating DataFrame...")
    df = pd.concat(results_list, ignore_index=True)
    df.rename(columns={'job_title_link': 'url'}, inplace=True)
    df.rename(columns={"job_title": 'Role', 'contract_type': 'Employment_Type', 'url': 'job_url'},inplace=True)
    df['scrap_date'] = str(date.today())
    final_data = pd.concat([final_data,df],axis=0,ignore_index=True)
    print('saving keyword file')
    df.to_csv(r"/home/ec2-user/scrape_data/listing_page_data/listing_page_all_{}.csv".format(str(date.today())),index=False)

saving_listing_df(final_data)
end_time = time.time()
duration = end_time - start_time    
print(f"Time taken: {duration/60} minutes")



