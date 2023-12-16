#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


# ### Listing Page

# In[2]:


# number of pages


# In[3]:


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


# In[4]:


max_page_number = get_max_page_number("https://www.jobs.nhs.uk/candidate/search/results?language=en")


# In[4]:


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


# In[5]:


# Function to remove the specified pattern from a URL
def remove_keyword_param(url):
    pattern = re.compile(r'keyword=[^&]+&')
    return re.sub(pattern, '', url)


# In[6]:


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


# In[7]:


def short_link(link):
    return link.split('?')[0]


# In[8]:


def saving_listing_df(final_data):
    final_data['job_url_hit'] = final_data['job_url'].apply(lambda x: remove_keyword_param(x))
    final_data['job_code'] = final_data['job_url_hit'].apply(lambda x:extract_job_codes(x))
    final_data['short_job_link'] = final_data['job_url_hit'].apply(lambda x:short_link(x))
    final_data = final_data.drop_duplicates('short_job_link',keep='first').reset_index(drop=True)
    final_data.to_csv(r"/home/ec2-user/scrape_data/listing_page_data/listing_page_all_{}.csv".format(str(date.today())),index=False)
    return final_data


# In[10]:


def listing_push_to_s3(x,y,z):
    print(f'Pushing {y}_{z} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    df = pd.read_csv(f"/home/ec2-user/scrape_data/{x}/{y}_{z}.csv")
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}_{z}.csv',Key = f'{x}/{y}_{z}.csv')
    print(f'{y}_{z} pushed to bucket in {x}')


# In[10]:


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
    print('saving listing file')
    df.to_csv(r"/home/ec2-user/scrape_data/listing_page_data/listing_page_all_{}.csv".format(str(date.today())),index=False)

saving_listing_df(final_data)
end_time = time.time()
duration = end_time - start_time    
print(f"Time taken: {duration/60} minutes")


# In[12]:


listing_push_to_s3('listing_page_data','listing_page_all',str(date.today()))


# ### JD

# In[13]:


def fixing_job_ref(code):
    if '<' in str(code):
        soup = BeautifulSoup(str(code), 'html.parser')
        return soup.text
    else:
        return code


# In[14]:


def remove_duplicates(input_list):
    seen = set()
    result = []
    
    for item in input_list:
        if item not in seen:
            seen.add(item)
            result.append(item)
    
    return result


# In[15]:


def extract_data(response, url):
    if response is not None:
        # preparing list
        job_title_list = []
        job_summary_list = []
        Main_duties_of_the_job_list = []
        about_us_list = []
        job_discription_list = []
        date_posted_list = []
        job_reference_number_list = []
        qualification_essentials_list = []
        qualification_desirable_list = []
        experience_essentials_list = []
        experience_desirable_list = []
        additional_criteria_essentials_list = []
        additional_criteria_desirable_list = []
        disclosure_and_barring_service_check_list = []
        employer_name_list = []
        employer_address_list = []
        employer_post_code_list = []
        employer_website_list = []
        contact_person_position_list = []
        contact_person_name_list = []
        contact_person_email_list = []
        contact_person_number_list = []
        page_url_list = []
        page_number_list = []
        
        # beautify
        soup = BeautifulSoup(response.text, 'html.parser')

        #job title
        try:
            job_title = soup.find_all('h1',class_='nhsuk-heading-xl nhsuk-u-margin-bottom-2 word-wrap')[0].text.strip()
            job_title_list.append(job_title)
        except:
            job_title_list.append('-')

        #job_summary
        try:
            job_summary = soup.find('p', id='job_overview').text
            job_summary_list.append(job_summary)
        except:
            job_summary_list.append('-')

        #Main_duties_of_the_job
        try:
            Main_duties_of_the_job = soup.find('p',id='job_description').text
            Main_duties_of_the_job_list.append(Main_duties_of_the_job)
        except:
            Main_duties_of_the_job_list.append('-')

        #About us
        try:
            about_us = soup.find('p',id='about_organisation').text
            about_us_list.append(about_us)
        except:
            about_us_list.append('-')

        #job_discription
        try:
            job_discription = soup.find('div',class_='hide-mobile').text.strip().replace('\n',' : ').replace(' \xa0 ',' ').replace('Job description : Job responsibilities : ','').replace('        \r : ','').replace('\r : ','').replace(' : ','')
            job_discription_list.append(job_discription)
        except:
            job_discription_list.append('-')
            
        #date_posted
        try:
            date_posted = soup.find('p',id='date_posted').text
            date_posted_list.append(date_posted)
        except:
            date_posted_list.append('-')
        
        #job_reference_number
        try:
            job_reference = soup.find('p',id='trac-job-reference')
            if job_reference is not None:
                text_value = job_reference.text
                job_reference_number_list.append(job_reference)
            else:
                text_value_2 = soup.find('p',id='job_reference_number').text
                job_reference_number_list.append(text_value_2)
        except:
            job_reference_number_list.append('-')

        #qualification_essentials
        try:
            qualification_essentials = remove_duplicates(soup.find_all('ul',class_='nhsuk-list nhsuk-list--bullet nhsuk-u-margin-bottom-2'))[0].text.strip().replace(' \n\n','. ').replace('\n\n','. ')
            qualification_essentials_list.append(qualification_essentials)
        except:
            qualification_essentials_list.append('-')

        #qualification_desirable
        try:
            qualification_desirable = remove_duplicates(soup.find_all('ul','nhsuk-list nhsuk-list--bullet nhsuk-u-margin-bottom-4'))[0].text.strip().replace(' \n\n','. ').replace('\n\n','. ')
            qualification_desirable_list.append(qualification_desirable)
        except:
            qualification_desirable_list.append('-')

        #experience_essentials
        try:
            experience_essentials = remove_duplicates(soup.find_all('ul',class_='nhsuk-list nhsuk-list--bullet nhsuk-u-margin-bottom-2'))[1].text.strip().replace(' \n\n','. ').replace('\n\n','. ')
            experience_essentials_list.append(experience_essentials)
        except:
            experience_essentials_list.append('-')

        #experience_desirable
        try:
            experience_desirable = remove_duplicates(soup.find_all('ul','nhsuk-list nhsuk-list--bullet nhsuk-u-margin-bottom-4'))[1].text.strip().replace(' \n\n','. ').replace('\n\n','. ')
            experience_desirable_list.append(experience_desirable)
        except:
            experience_desirable_list.append('-')

        #additional_criteria_essentials
        try:
            additional_criteria_essentials = remove_duplicates(soup.find_all('ul',class_='nhsuk-list nhsuk-list--bullet nhsuk-u-margin-bottom-2'))[2].text.strip().replace(' \n\n','. ').replace('\n\n','. ')
            additional_criteria_essentials_list.append(additional_criteria_essentials)
        except:
            additional_criteria_essentials_list.append('-')

        #additional_criteria_desirable
        try:
            additional_criteria_desirable = remove_duplicates(soup.find_all('ul','nhsuk-list nhsuk-list--bullet nhsuk-u-margin-bottom-4'))[2].text.strip().replace(' \n\n','. ').replace('\n\n','. ')
            additional_criteria_desirable_list.append(additional_criteria_desirable)
        except:
            additional_criteria_desirable_list.append('-')

        #disclosure_and_barring_service_check
        try:
            disclosure_and_barring_service_check = soup.find_all('div',id='dbs-container')[0].text.strip().strip('Disclosure and Barring Service Check\n').replace('\n','')
            disclosure_and_barring_service_check_list.append(disclosure_and_barring_service_check)
        except:
            disclosure_and_barring_service_check_list.append('-')

        #employer_name
        try:
            employer_name = soup.find('p',id='employer_name_details').text
            employer_name_list.append(employer_name)
        except:
            employer_name_list.append('-')

        #employer_address
        try:
            line1 = soup.find('p',id='employer_address_line_1').text
            line2 = soup.find('p',id='employer_address_line_2').text
            line3 = soup.find('p',id='employer_town').text
            line4 = soup.find('p',id='employer_county').text
            line5 = soup.find('p',id='employer_postcode').text

            new_line = []
            for kk in [line1,line2,line3,line4,line5]:
                if kk != '':
                    new_line.append(kk)
                else:
                    pass
            final_address = ' '.join(new_line).strip()
            employer_address_list.append(final_address)
        except:
            employer_address_list.append('-')

        #employer_post_code
        try:
            employer_post_code = soup.find('p',id='employer_postcode').text
            employer_post_code_list.append(employer_post_code)
        except:
            employer_post_code_list.append('-')

        #employer_website
        try:
            employer_website = soup.find('p',id='employer_website_url').text.strip(' (Opens in a new tab)')
            employer_website_list.append(employer_website)
        except:
            employer_website_list.append('-')
        
        #contact_person_position
        try:
            contact_person_position = soup.find('div',id='contact_details').find(id='contact_details_job_title').text
            contact_person_position_list.append(contact_person_position)
        except:
            contact_person_position_list.append('-')
                
        #contact_person_name
        try:
            contact_person_name = soup.find('div',id='contact_details').find(id='contact_details_name').text
            contact_person_name_list.append(contact_person_name)
        except:
            contact_person_name_list.append('-')
        
        #contact_person_email
        try:
            contact_person_email = soup.find('div',id='contact_details').find(id='contact_details_email').text
            contact_person_email_list.append(contact_person_email)
        except:
            contact_person_email_list.append('-')
            
        #contact_person_number
        try:
            contact_person_number = soup.find('div',id='contact_details').find(id='contact_details_number').text
            contact_person_number_list.append(contact_person_number)
        except:
            contact_person_number_list.append('-')

        #page_url
        page_url_list.append(url)

        #page number
        page_number_list.append(url.split('page=')[-1])

        #     time.sleep(1)
        #making DataFrame
        df3 = pd.DataFrame(list(zip(job_title_list,job_summary_list,
        Main_duties_of_the_job_list,
        about_us_list,
        job_discription_list,
        date_posted_list,
        job_reference_number_list,
        qualification_essentials_list,
        qualification_desirable_list,
        experience_essentials_list,
        experience_desirable_list,
        additional_criteria_essentials_list,
        additional_criteria_desirable_list,
        disclosure_and_barring_service_check_list,
        employer_name_list,
        employer_address_list,
        employer_post_code_list,
        employer_website_list,
        contact_person_position_list,
        contact_person_name_list,
        contact_person_email_list,
        contact_person_number_list,
        page_url_list,                         
        page_number_list)),
                    columns=['job_title','job_summary',
        'Main_duties_of_the_job',
        'about_us',
        'job_discription',
        'date_posted',
        'job_reference_number',
        'qualification_essentials',
        'qualification_desirable',
        'experience_essentials',
        'experience_desirable',
        'additional_criteria_essentials',
        'additional_criteria_desirable',
        'disclosure_and_barring_service_check',
        'employer_name',
        'employer_address',
        'employer_post_code',
        'employer_website',
        'contact_person_position',
        'contact_person_name',
        'contact_person_email',
        'contact_person_number',                    
        'job_url',
        'page_number'])
        df3['scraped_date'] = str(date.today())
        return df3
    else:
        job_title_list,job_summary_list = ['-']
        Main_duties_of_the_job_list = ['-']
        about_us_list = ['-']
        job_discription_list = ['-']
        date_posted_list = ['-']
        job_reference_number_list = ['-']
        qualification_essentials_list = ['-']
        qualification_desirable_list = ['-']
        experience_essentials_list = ['-']
        experience_desirable_list = ['-']
        additional_criteria_essentials_list = ['-']
        additional_criteria_desirable_list = ['-']
        disclosure_and_barring_service_check_list = ['-']
        employer_name_list = ['-']
        employer_address_list = ['-']
        employer_post_code_list = ['-']
        employer_website_list = ['-']
        contact_person_position_list = ['-']
        contact_person_name_list = ['-']
        contact_person_email_list = ['-']
        contact_person_number_list = ['-']
        page_url_list = [url]                         
        page_number_list = ['-']
        
        df3 = pd.DataFrame(list(zip(job_title_list,job_summary_list,
        Main_duties_of_the_job_list,
        about_us_list,
        job_discription_list,
        date_posted_list,
        job_reference_number_list,
        qualification_essentials_list,
        qualification_desirable_list,
        experience_essentials_list,
        experience_desirable_list,
        additional_criteria_essentials_list,
        additional_criteria_desirable_list,
        disclosure_and_barring_service_check_list,
        employer_name_list,
        employer_address_list,
        employer_post_code_list,
        employer_website_list,
        contact_person_position_list,
        contact_person_name_list,
        contact_person_email_list,
        contact_person_number_list,
        page_url_list,                         
        page_number_list)),
                    columns=['job_title','job_summary',
        'Main_duties_of_the_job',
        'about_us',
        'job_discription',
        'date_posted',
        'job_reference_number',
        'qualification_essentials',
        'qualification_desirable',
        'experience_essentials',
        'experience_desirable',
        'additional_criteria_essentials',
        'additional_criteria_desirable',
        'disclosure_and_barring_service_check',
        'employer_name',
        'employer_address',
        'employer_post_code',
        'employer_website',
        'contact_person_position',
        'contact_person_name',
        'contact_person_email',
        'contact_person_number',                    
        'job_url',
        'page_number'])
        df3['scraped_date'] = str(date.today())
        return df3


# In[16]:


def scrape_jd_page(url, request_timeout=30):
    max_retries = 3  # Number of maximum retries
    delay = 2  # Delay between retries in seconds
    retries = 0
    dff = None  # Initialize dff to None
    jd_lists = list(new_job['job_url_hit'])
    
    while retries < max_retries:
        try:
            # URL
            urls = url

            # Send a GET request to the URL
            response = requests.get(urls, timeout=request_timeout)

            soup = BeautifulSoup(response.text, 'html.parser')

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                dff = extract_data(response, url)
                print(jd_lists.index(url), 'done out of ', len(jd_lists))

                break  # Break out of the while loop if successful
            else:
                print(f"Failed to retrieve the page. Status code: {response.status_code} for : {url}")

        except (ConnectionError, ReadTimeout) as e:
            print(f"ConnectionError: {e}. Retrying... {url}")
            time.sleep(2 ** retries)  # Wait for a few seconds before retrying
            retries += 1
            if retries == max_retries:
                print(f"Maximum retries reached for: {url}")

    return dff


# ##### new jobs

# In[ ]:


def new_job_data_list(x):
    s3 = boto3.resource("s3")
    s3_bucket = s3.Bucket("nhs-dataset")
    dir = x
    files_in_s3 = [f.key.split(dir + "/") for f in s3_bucket.objects.filter(Prefix=dir).all()]
    # Remove the 0th element
    files_in_s3.pop(0)
    
    # Flatten the remaining nested lists
    flat_list = [item for sublist in files_in_s3 for item in sublist]
    return flat_list


# In[ ]:


def new_job_df(x,final_data):
    old_listing_data = pd.DataFrame()
    specific_files = new_job_data_list(x)
    for file in specific_files[:-1]:
        #load from bucket
        s3 = boto3.resource("s3")
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
        old_listing_data = pd.concat([old_listing_data,dd],axis=0,ignore_index=True)
    new_job = final_data[~(final_data['job_code'].isin(list(old_listing_data['job_code'])))].reset_index(drop=True)
    new_job.to_csv(r"/home/ec2-user/scrape_data/new_job_data/new_job_{}.csv".format(str(date.today())),index=False)
    return new_job


# In[37]:


def new_job_push_to_s3(x,y,z):
    print(f'Pushing {y}_{str(date.today())} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    df = pd.read_csv(f"/home/ec2-user/scrape_data/{x}/{y}_{z}.csv")
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}_{z}.csv',Key = f'{x}/{y}_{z}.csv')
    print(f'{y}_{z} pushed to bucket in {x}')


# In[37]:


def jd_push_to_s3(x,y,z):
    print(f'Pushing {y}_{str(date.today())} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    df = pd.read_csv(f"/home/ec2-user/scrape_data/{x}/{y}_{z}.csv")
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}_{z}.csv',Key = f'{x}/{y}_{z}.csv')
    print(f'{y}_{z} pushed to bucket in {x}')


# In[25]:


new_job = new_job_df('listing_page',final_data)


# In[ ]:


new_job_push_to_s3('new_job_data','new_job',str(date.today()))


# In[36]:


start_time = time.time()
print(str(datetime.now()))
if __name__ == '__main__':
    # Define the URLs
    jd_lists = list(new_job['job_url_hit'])

    results_list = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=mp.cpu_count()-1) as executor:
        # Submit the scraping function to the executor for each page number
        results = list(executor.map(scrape_jd_page, jd_lists))
        
        for i, result in enumerate(results):
            if result is not None:
                results_list.append(result)                      
                print(f"Result {i + 1} appended")
            else:
                print(f"Result {i + 1} is None. Skipping...")
                
    # Create a DataFrame from the results
    df_jd = pd.concat(results,ignore_index=True)
    df_jd.rename(columns={"job_url":'job_url_hit'},inplace=True)
    df_jd['job_reference_number'] = df_jd['job_reference_number'].apply(lambda x: fixing_job_ref(x))
    df_jd.to_csv(r"/home/ec2-user/scrape_data/jd_page_data/jd_page_for_new_job_{}.csv".format(str(date.today())),index=False)

end_time = time.time()
duration = end_time - start_time
print(f"Time taken: {duration/60} mintues")


# In[ ]:


jd_push_to_s3('jd_page_data','jd_page_for_new_job',str(date.today()))


# #### Master DF

# In[38]:


def master_df(new_job,df_jd):
    master_final = new_job.merge(df_jd,on=['job_url_hit'],how='left')
    master_final.to_excel(r"/home/ec2-user/scrape_data/master_new_job_data/master_new_job_{}.xlsx".format(str(date.today())),index=False)
    return master_final


# In[37]:


def master_push_to_s3(x,y,z):
    print(f'Pushing {y}_{str(date.today())} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    df = pd.read_excel(f"/home/ec2-user/scrape_data/{x}/{y}_{z}.xlsx")
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}_{z}.xlsx',
                                         Key = f'{x}/{y}_{z}.xlsx')


# In[39]:


master_final = master_df(new_job,df_jd)


# In[ ]:


master_push_to_s3('master_new_job_data','master_new_job',str(date.today()))


# ##### email new jobs

# In[69]:


# # Specify the path to your JSON file
# file_path = r"/home/ec2-user/tern-scraping-code/email_credentials.json"

# # Read the JSON file and convert it to a dictionary
# with open(file_path, 'r') as json_file:
#     email_cred = json.load(json_file)


# # In[70]:


# # Define a function to apply color and style formatting to the cells
# def format_row(row):
#     return f"""
#         <tr>
#             <td style="padding: 12px 15px; background-color: {'#D6EEEE' if row['New Jobs Count'] % 2 == 0 else '#D6EEEE'}; border: 2px solid #ddd; font-weight: {'bold' if row.name == 'Keywords' else 'normal'}; text-align: center;">{row['Keywords']}</td>
#             <td style="padding: 12px 15px; background-color: {'#D6EEEE' if row['New Jobs Count'] % 2 == 0 else '#D6EEEE'}; border: 2px solid #ddd; font-weight: {'bold' if row.name == 'Keywords' else 'normal'}; text-align: center;">{row['New Jobs Count']}</td>
#         </tr>
#     """


# # In[31]:


# def email_people(email_cred,master_final):
#     # Load your DataFrame from the CSV file
#     new_csv_file_path = r"/home/ec2-user/scrape_data/master_new_job_data/master_new_job_{}.xlsx".format(str(date.today()))
#     # Email configuration
#     sender_email = email_cred['email']
#     sender_password = email_cred['password']
#     receiver_email = ["shikharrajput@gmail.com","safal.verma@tern-group.com"]
# #     receiver_email = ["shikharrajput@gmail.com","safal.verma@tern-group.com","ashita@tern-group.com","akshay.rao@tern-group.com"]

#     subject = f"New Job on {str(date.today())}"
    
#     # Create the email body with a formatted table
#     body = f"""
#     <html>
#     <body>
#       <p>Hi,</p>

#       <p> As of today {str(date.today())} there are {master_final.shape[0]} new jobs on NHS </p>

#       <p>PFA.</p>

#       <p>Best regards,<br>Your Name</p>
#     </body>
#     </html>
#     """
    
#     # Create the email message
#     message = MIMEMultipart()
#     message.attach(MIMEText(body, 'html'))
    
#     # Attach the new CSV file
#     with open(new_csv_file_path, 'rb') as attachment:
#         part = MIMEBase('application', 'octet-stream')
#         part.set_payload(attachment.read())
#         encoders.encode_base64(part)
#         part.add_header('Content-Disposition', f'attachment; filename=New_Job_{str(date.today())}')
#         message.attach(part)
    
#     # Create the email message
#     message['From'] = sender_email
#     message['To'] = ", ".join(receiver_email)
#     message['Subject'] = subject

#     context = ssl.create_default_context()

#     # Connect to the SMTP server and send the email
#     smtp_server = "smtp.gmail.com"
#     smtp_port = 465

#     with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
#         server.login(sender_email, sender_password)
#         server.send_message(message)

#     print("Email sent successfully!")


# # In[32]:


# email_people(email_cred,master_final)


# In[ ]:




