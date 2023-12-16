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
from requests.exceptions import ConnectionError
from concurrent.futures import ThreadPoolExecutor
import time
import threading
import os
import glob
import boto3
from botocore.exceptions import ClientError
import botocore
import s3fs as s3


# In[2]:


def fixing_job_ref(code):
    if '<' in code:
        soup = BeautifulSoup(code, 'html.parser')
        return soup.text
    else:
        return code


# In[3]:


# Function to remove the specified pattern from a URL
def remove_keyword_param(url):
    pattern = re.compile(r'keyword=[^&]+&')
    return re.sub(pattern, '', url)


# In[4]:


def extract_job_codes(link):
    # Define the regex pattern to match the job code
    pattern = pattern = r'/jobadvert/([A-Za-z0-9-]+)\?'

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
    return link.split('?')[0]


# In[6]:


def extracting_keyword(url):
    # Extract the keyword using regex
    match = re.search(r'keyword=([^&]+)', url)

    if match:
        keyword = match.group(1)
        # Decode URL-encoded characters
        keyword = re.sub('%20', ' ', keyword)
        keyword = keyword.replace("'", '%27')
        return keyword
    else:
        return '-'


# In[ ]:


def listing_page_data_list(x):
    s3 = boto3.resource("s3")
    s3_bucket = s3.Bucket("nhs-dataset")
    dir = x
    files_in_s3 = [f.key.split(dir + "/") for f in s3_bucket.objects.filter(Prefix=dir).all()]
    # Remove the 0th element
    files_in_s3.pop(0)
    
    # Flatten the remaining nested lists
    flat_list = [item for sublist in files_in_s3 for item in sublist]
    filtered_list = [item for item in flat_list if item != '']
    return filtered_list


# In[ ]:


def listing_page_master_df(x):
    old_listing_data = pd.DataFrame()
    specific_files = listing_page_data_list(x)
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
    return old_listing_data


# In[ ]:


def listing_page_master_push_to_s3(x,y):
    print(f'Pushing {y} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    df = pd.read_csv(f"/home/ec2-user/scrape_data/{x}/{y}.csv")
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}.csv',Key = f'{x}/{y}.csv')
    print(f'{y} pushed to bucket in {x}')


# In[9]:


listing_page_master = listing_page_master_df('listing_page_data')


# In[ ]:


listing_page_master_push_to_s3('master_data','Listing_Page_Master')


# In[10]:


update_these_links = list(set(listing_page_master['short_job_link']))
print(len(update_these_links))


# In[11]:


del listing_page_master


# ### JD Page

# In[12]:


def remove_duplicates(input_list):
    seen = set()
    result = []
    
    for item in input_list:
        if item not in seen:
            seen.add(item)
            result.append(item)
    
    return result


# In[13]:


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


# In[14]:


def scrape_jd_page(url, request_timeout=30):
    max_retries = 3  # Number of maximum retries
    delay = 2  # Delay between retries in seconds
    retries = 0
    dff = None  # Initialize dff to None
    jd_lists = update_these_links
    
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
            print(f"ConnectionError: {e}. Retrying...")
            time.sleep(delay)  # Wait for a few seconds before retrying
            retries += 1
            if retries == max_retries:
                print(f"Maximum retries reached for: {url}")

    return dff


# In[ ]:


def jd_page_push_to_s3(x,y):
    print(f'Pushing {y} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    df = pd.read_csv(f"/home/ec2-user/scrape_data/{x}/{y}.csv")
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}.csv',Key = f'{x}/{y}.csv')
    print(f'{y} pushed to bucket in {x}')


# In[14]:


start_time = time.time()
print(str(datetime.now()))
if __name__ == '__main__':
    # Define the URLs
    jd_lists = update_these_links[:10000]
    results_list = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=mp.cpu_count()-1) as executor:
        # Submit the scraping function to the executor for each page number
        results = executor.map(scrape_jd_page, jd_lists)
        for i, result in enumerate(results):
            if result is not None:
#                 print(result)
                results_list.append(result)                      
                print(f"Result {i + 1} appended")
            else:
                print(f"Result {i + 1} is None. Skipping...")
    print("All results processed.")
    print("Creating DataFrame...")    
    # Create a DataFrame from the results
    df_jd = pd.concat(results_list,ignore_index=True)
    df_jd.rename(columns={"job_url":'job_url_hit'},inplace=True)
    df_jd.to_csv(r"/home/ec2-user/scrape_data/jd_page_data/job_information_updated_all_0_10000_{}.csv".format(str(date.today())),index=False)

end_time = time.time()
duration = end_time - start_time
print(f"Time taken: {duration/60} mintues")


# In[ ]:


del df_jd


# In[ ]:


jd_page_push_to_s3("jd_page_data",f"job_information_updated_all_0_10000_{str(date.today())}")


# In[15]:


start_time = time.time()
print(str(datetime.now()))
if __name__ == '__main__':
    # Define the URLs
    jd_lists = update_these_links[10000:20000]
    results_list = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=mp.cpu_count()-1) as executor:
        # Submit the scraping function to the executor for each page number
        results = executor.map(scrape_jd_page, jd_lists)
        for i, result in enumerate(results):
            if result is not None:
#                 print(result)
                results_list.append(result)                      
                print(f"Result {i + 1} appended")
            else:
                print(f"Result {i + 1} is None. Skipping...")
    print("All results processed.")
    print("Creating DataFrame...")    
    # Create a DataFrame from the results
    df_jd_2 = pd.concat(results_list,ignore_index=True)
    df_jd_2.rename(columns={"job_url":'job_url_hit'},inplace=True)
    df_jd_2.to_csv(r"/home/ec2-user/scrape_data/jd_page_data/job_information_updated_all_10000_20000_{}.csv".format(str(date.today())),index=False)

end_time = time.time()
duration = end_time - start_time
print(f"Time taken: {duration/60} mintues")


# In[ ]:


del df_jd_2


# In[ ]:


jd_page_push_to_s3("jd_page_data",f"job_information_updated_all_10000_20000_{str(date.today())}")


# In[16]:


start_time = time.time()
print(str(datetime.now()))
if __name__ == '__main__':
    # Define the URLs
    jd_lists = update_these_links[20000:30000]
    results_list = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=mp.cpu_count()-1) as executor:
        # Submit the scraping function to the executor for each page number
        results = executor.map(scrape_jd_page, jd_lists)
        for i, result in enumerate(results):
            if result is not None:
#                 print(result)
                results_list.append(result)                      
                print(f"Result {i + 1} appended")
            else:
                print(f"Result {i + 1} is None. Skipping...")
    print("All results processed.")
    print("Creating DataFrame...")    
    # Create a DataFrame from the results
    df_jd_3 = pd.concat(results_list,ignore_index=True)
    df_jd_3.rename(columns={"job_url":'job_url_hit'},inplace=True)
    df_jd_3.to_csv(r"/home/ec2-user/scrape_data/jd_page_data/job_information_updated_all_20000_30000_{}.csv".format(str(date.today())),index=False)

end_time = time.time()
duration = end_time - start_time
print(f"Time taken: {duration/60} mintues")


# In[ ]:


del df_jd_3


# In[ ]:


jd_page_push_to_s3("jd_page_data",f"job_information_updated_all_20000_30000_{str(date.today())}")


# In[15]:


start_time = time.time()
print(str(datetime.now()))
if __name__ == '__main__':
    # Define the URLs
    jd_lists = update_these_links[30000:40000]
    results_list = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=mp.cpu_count()-1) as executor:
        # Submit the scraping function to the executor for each page number
        results = executor.map(scrape_jd_page, jd_lists)
        for i, result in enumerate(results):
            if result is not None:
#                 print(result)
                results_list.append(result)                      
                print(f"Result {i + 1} appended")
            else:
                print(f"Result {i + 1} is None. Skipping...")
    print("All results processed.")
    print("Creating DataFrame...")    
    # Create a DataFrame from the results
    df_jd_4 = pd.concat(results_list,ignore_index=True)
    df_jd_4.rename(columns={"job_url":'job_url_hit'},inplace=True)
    df_jd_4.to_csv(r"/home/ec2-user/scrape_data/jd_page_data/job_information_updated_all_30000_40000_{}.csv".format(str(date.today())),index=False)

end_time = time.time()
duration = end_time - start_time
print(f"Time taken: {duration/60} mintues")


# In[ ]:


del df_jd_4


# In[ ]:


jd_page_push_to_s3("jd_page_data",f"job_information_updated_all_30000_40000_{str(date.today())}")


# In[16]:


start_time = time.time()
print(str(datetime.now()))
if __name__ == '__main__':
    # Define the URLs
    jd_lists = update_these_links[40000:]
    results_list = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=mp.cpu_count()-1) as executor:
        # Submit the scraping function to the executor for each page number
        results = executor.map(scrape_jd_page, jd_lists)
        for i, result in enumerate(results):
            if result is not None:
#                 print(result)
                results_list.append(result)                      
                print(f"Result {i + 1} appended")
            else:
                print(f"Result {i + 1} is None. Skipping...")
    print("All results processed.")
    print("Creating DataFrame...")    
    # Create a DataFrame from the results
    df_jd_5 = pd.concat(results_list,ignore_index=True)
    df_jd_5.rename(columns={"job_url":'job_url_hit'},inplace=True)
    df_jd_5.to_csv(r"/home/ec2-user/scrape_data/jd_page_data/job_information_updated_all_40000+_{}.csv".format(str(date.today())),index=False)

end_time = time.time()
duration = end_time - start_time
print(f"Time taken: {duration/60} mintues")


# In[ ]:


del df_jd_5


# In[ ]:


jd_page_push_to_s3("jd_page_data",f"job_information_updated_all_40000+_{str(date.today())}")


# In[17]:


df_jd = pd.read_csv(r"/home/ec2-user/scrape_data/jd_page_data/job_information_updated_all_0_10000_{}.csv".format(str(date.today())))
df_jd_2 = pd.read_csv(r"/home/ec2-user/scrape_data/jd_page_data/job_information_updated_all_10000_20000_{}.csv".format(str(date.today())))
df_jd_3 = pd.read_csv(r"/home/ec2-user/scrape_data/jd_page_data/job_information_updated_all_20000_30000_{}.csv".format(str(date.today())))
df_jd_4 = pd.read_csv(r"/home/ec2-user/scrape_data/jd_page_data/job_information_updated_all_30000_40000_{}.csv".format(str(date.today())))
df_jd_5 = pd.read_csv(r"/home/ec2-user/scrape_data/jd_page_data/job_information_updated_all_40000+_{}.csv".format(str(date.today())))


# In[18]:


final_jd_data = pd.concat([df_jd,df_jd_2,df_jd_3,df_jd_4,df_jd_5],axis=0,ignore_index=True)


# In[19]:


final_jd_data['job_reference_number'] = final_jd_data['job_reference_number'].astype('string').apply(lambda x: fixing_job_ref(x))


# In[ ]:


def jd_page_push_to_s3(x,y):
    print(f'Pushing {y} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    df = pd.read_csv(f"/home/ec2-user/scrape_data/{x}/{y}.csv")
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}.csv',Key = f'{x}/{y}.csv')
    print(f'{y} pushed to bucket in {x}')


# In[21]:


final_jd_data.to_csv(r"/home/ec2-user/scrape_data/jd_page_data/job_information_updated_{}.csv".format(str(date.today())),index=False)


# In[ ]:


jd_page_push_to_s3("jd_page_data",f"job_information_updated_{str(date.today())}")


# In[ ]:





# In[ ]:




