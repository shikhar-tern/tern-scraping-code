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


# ### Listing Page

# number of pages
# sys.stdout = open(r'/home/ec2-user/scrape_data/log.txt','w')


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


def listing_push_to_s3(x,y,z):
    print(f'Pushing {y}_{z} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}_{z}.csv',Key = f'{x}/{y}_{z}.csv')
    print(f'{y}_{z} pushed to bucket in {x}')

def saving_listing_df(final_data):
    final_data['job_url_hit'] = final_data['job_url'].apply(lambda x: remove_keyword_param(x))
    final_data['job_code'] = final_data['job_url_hit'].apply(lambda x:extract_job_codes(x))
    final_data['short_job_link'] = final_data['job_url_hit'].apply(lambda x:short_link(x))
    final_data = final_data.drop_duplicates('short_job_link',keep='first').reset_index(drop=True)
    final_data.to_csv(r"/home/ec2-user/scrape_data/listing_page_data/listing_page_all_{}.csv".format(str(date.today())),index=False)
    listing_push_to_s3('listing_page_data','listing_page_all',str(date.today()))
    return final_data

# Read the JSON file and convert it to a dictionary
# Specify the path to your JSON file
file_path = r"/home/ec2-user/tern-scraping-code/email_credentials.json"
with open(file_path, 'r') as json_file:
    email_cred = json.load(json_file)

#Email with Just Text
def email_people(email_cred, x):
    # Email configuration
    sender_email = email_cred['email']
    sender_password = email_cred['password']
    receiver_email = ["shikharrajput@gmail.com","safal.verma@tern-group.com","ashita@tern-group.com"]
    subject = f"{x} Pushed to S3 on {str(date.today())}"
    # Create the email body with a formatted table
    body = f"""
    <html>
    <body>
      <p>Hi,</p>
      <p>{x} pushed to S3 on {str(date.today())}. </p>
      <p></p>
    </body>
    </html>
    """  
    # Create the email message
    message = MIMEMultipart()
    message.attach(MIMEText(body, 'html'))
    # Set up the SSL context
    context = ssl.create_default_context()
    # Connect to the SMTP server and send the email
    smtp_server = "smtp.gmail.com"
    smtp_port = 465
    with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, receiver_email, message.as_string())
    print("Email sent successfully!")

def delete_files(x,j):
    dir = '/home/ec2-user/scrape_data'
    os.remove(dir+"/"+x+"/"+j)
    print(f"File {j} removed from {x}")


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
delete_files("listing_page_data",f"listing_page_all_{str(date.today())}.csv")
# email_people(email_cred,"Listing Page")

# ### JD

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


def extract_data(response, url):
    if response is not None:
        # preparing list
        job_title_list = []
        job_summary_list = []
        Main_duties_of_the_job_list = []
        about_us_list = []
        job_discription_list = []
        date_posted_list = []
        band_list = []
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

        #band
        try:
            band = soup.find('p',id='payscheme-band').text
            band_list.append(band)
        except:
            band_list.append('-')
        
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
        band_list,
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
        'band',
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
        band_list = ['-']
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
        band_list,
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
        'band',
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

def new_job_data_list(x):
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
    filt_list = [item for item in filtered_list if item == 'Listing_Page_Master.csv']
    prefixed_list = [f'{x}/' + item for item in filt_list]
    return prefixed_list


def new_job_push_to_s3(x,y,z):
    print(f'Pushing {y}_{z} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}_{z}.csv',Key = f'{x}/{y}_{z}.csv')
    print(f'{y}_{z} pushed to bucket in {x}')


def new_job_df(x,final_data):
    old_listing_data = pd.DataFrame()
    specific_files = new_job_data_list(x)
    for file in specific_files:
        #load from bucket
        s3 = boto3.resource("s3")
        obj = s3.Bucket('nhs-dataset').Object(file).get()
        dd = pd.read_csv(obj['Body'])
        try:
            del dd['Unnamed: 0']
        except:
            pass
        dd['job_url'] = dd['job_url_hit']
        dd['job_url_hit'] = dd['job_url'].apply(lambda x: remove_keyword_param(x))
        dd['scrap_date'] = file.split('all_')[-1].strip('.csv')
        dd['job_code'] = dd['job_url_hit'].apply(lambda x:extract_job_codes(x))
        dd['short_job_link'] = dd['job_url_hit'].apply(lambda x:short_link(x))
        dd = dd.drop_duplicates(['scrap_date','job_url_hit'],keep='first').reset_index(drop=True)
        old_listing_data = pd.concat([old_listing_data,dd],axis=0,ignore_index=True)
    new_job = final_data[~(final_data['job_code'].isin(list(old_listing_data['job_code'])))].reset_index(drop=True)
    new_job.to_csv(r"/home/ec2-user/scrape_data/new_job_data/new_job_{}.csv".format(str(date.today())),index=False)
    new_job_push_to_s3('new_job_data','new_job',str(date.today()))
    delete_files("new_job_data",f"new_job_{str(date.today())}.csv")
    # email_people(email_cred,"New Jobs")
    return new_job


new_job = new_job_df('master_data',final_data)


def jd_push_to_s3(x,y,z):
    print(f'Pushing {y}_{z} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}_{z}.csv',Key = f'{x}/{y}_{z}.csv')
    print(f'{y}_{z} pushed to bucket in {x}')


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
    jd_push_to_s3('jd_page_data','jd_page_for_new_job',str(date.today()))
    delete_files("jd_page_data",f"jd_page_for_new_job_{str(date.today())}.csv")
end_time = time.time()
duration = end_time - start_time
# email_people(email_cred,"JD page for new jobs")
print(f"Time taken: {duration/60} mintues")

# #### Master DF

def master_push_to_s3(x,y,z):
    print(f'Pushing {y}_{z} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}_{z}.csv',Key = f'{x}/{y}_{z}.csv')

def master_df(new_job,df_jd):
    master_final = new_job.merge(df_jd,on=['job_url_hit'],how='left')
    master_final.to_csv(r"/home/ec2-user/scrape_data/master_new_job_data/master_new_job_{}.csv".format(str(date.today())),index=False)
    master_push_to_s3('master_new_job_data','master_new_job',str(date.today()))
    delete_files("master_new_job_data",f"master_new_job_{str(date.today())}.csv")
    # email_people(email_cred,"Master New Jobs")
    return master_final

master_final = master_df(new_job,df_jd)

#----------------------------------------------------------------------------------------------------------------------------------

print('Starting with Creating Masters')
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
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}.csv',Key = f'{x}/{y}.csv')
    print(f'{y} pushed to bucket in {x}')

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

def listing_page_master_df(x):
    print('Starting with Listing Page')
    listing_all_df = fetching_df('master_data','Listing_Page_Master')
    listing_all_df['scrap_date'] = pd.to_datetime(listing_all_df['scrap_date'],format='ISO8601')
    max_date = max(listing_all_df['scrap_date']).date()
    old_listing_data = pd.DataFrame()
    specific_files = data_list(x)
    for file in specific_files:
        if pd.to_datetime(file.split("/")[-1].split("_")[-1].replace(".csv","")).date() > max_date:
            print(f'Starting with: {file}')
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
        else:
            pass
    listing_all_df = pd.concat([listing_all_df,old_listing_data],axis=0,ignore_index=True)    
    listing_all_df.to_csv(r"/home/ec2-user/scrape_data/master_data/Listing_Page_Master.csv",index=False)
    push_to_s3('master_data','Listing_Page_Master')
    delete_files("master_data","Listing_Page_Master.csv")
    # email_people(email_cred,"Listing Page Master")
    return listing_all_df

listing_page_master = listing_page_master_df('listing_page_data')
# #### Merged Master

def merged_master_df(x):
    print('Starting With Merged Master')
    merged_master_all_df = fetching_df('master_data','Merged_Master')
    merged_master_all_df['scrap_date'] = pd.to_datetime(merged_master_all_df['scrap_date'],format='ISO8601')
    max_date = max(merged_master_all_df['scrap_date']).date()
    old_listing_data = pd.DataFrame()
    specific_files = data_list(x)
    # Print the list of specific files
    for file in specific_files:
        if pd.to_datetime(file.split("/")[-1].split("_")[-1].replace(".csv","")).date() > max_date:
            print(f"Starting with {file}")
            s3 = boto3.resource("s3")
            #load from bucket
            obj = s3.Bucket('nhs-dataset').Object(file).get()
            dd = pd.read_csv(obj['Body'])
            dd['job_url_hit'] = dd['job_url'].apply(lambda x: remove_keyword_param(x))
            dd['job_code'] = dd['job_url_hit'].apply(lambda x:extract_job_codes(x))
            dd['short_job_link'] = dd['job_url_hit'].apply(lambda x:short_link(x))
            dd['job_reference_number'] = dd['job_reference_number'].apply(lambda x: fixing_job_ref(x))
            dd = dd.drop_duplicates(['scrap_date','job_code'],keep='first').reset_index(drop=True) 
            old_listing_data = pd.concat([old_listing_data,dd],axis=0,ignore_index=True)
        else:
            pass
    merged_master_all_df = pd.concat([merged_master_all_df,old_listing_data],axis=0,ignore_index=True)
    merged_master_all_df.to_csv(r"/home/ec2-user/scrape_data/master_data/Merged_Master.csv",index=False)
    push_to_s3('master_data','Merged_Master')
    delete_files("master_data","Merged_Master.csv")
    # email_people(email_cred,"Merged Master")
    return merged_master_all_df

merged_master = merged_master_df('master_new_job_data')
del merged_master

# #### New Jobs Master
def new_jobs_master_df(x):
    print('Starting with New Jobs Data')
    new_jobs_master_all_df = fetching_df('master_data','New_Jobs_Master')
    new_jobs_master_all_df['scrap_date'] = pd.to_datetime(new_jobs_master_all_df['scrap_date'],format='ISO8601')
    max_date = max(new_jobs_master_all_df['scrap_date']).date()
    old_listing_data = pd.DataFrame()
    specific_files = data_list(x)
    # Print the list of specific files
    for file in specific_files:
        if pd.to_datetime(file.split("/")[-1].split("_")[-1].replace(".csv","")).date() > max_date:
            print(f"Starting with {file}")
            s3 = boto3.resource("s3")
            #load from bucket
            obj = s3.Bucket('nhs-dataset').Object(file).get()
            dd = pd.read_csv(obj['Body'])
            dd['job_url_hit'] = dd['job_url'].apply(lambda x: remove_keyword_param(x))
            dd['job_code'] = dd['job_url_hit'].apply(lambda x:extract_job_codes(x))
            dd['short_job_link'] = dd['job_url_hit'].apply(lambda x:short_link(x))
            dd = dd.drop_duplicates(['scrap_date','job_code'],keep='first').reset_index(drop=True) 
            old_listing_data = pd.concat([old_listing_data,dd],axis=0,ignore_index=True)
        else:
            pass
    new_jobs_master_all_df = pd.concat([new_jobs_master_all_df,old_listing_data],axis=0,ignore_index=True)
    new_jobs_master_all_df.to_csv(r"/home/ec2-user/scrape_data/master_data/New_Jobs_Master.csv",index=False)
    push_to_s3('master_data','New_Jobs_Master')
    delete_files("master_data","New_Jobs_Master.csv")
    # email_people(email_cred,"New Jobs Master")
    return new_jobs_master_all_df
new_jobs_master = new_jobs_master_df('new_job_data')
del new_jobs_master

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

def jd_updated_data_list(x):
    s3 = boto3.resource("s3")
    s3_bucket = s3.Bucket("nhs-dataset")
    dir = x
    files_in_s3 = [f.key.split(dir + "/") for f in s3_bucket.objects.filter(Prefix=dir).all()]
    # Remove the 0th element
    files_in_s3.pop(0)
    
    # Flatten the remaining nested lists
    flat_list = [item for sublist in files_in_s3 for item in sublist]
    filtered_list = [item for item in flat_list if item != '']
    #Only picking Active_Jobs
    filt_list = [item for item in filtered_list if item == 'Jobs_Information_Master.csv']
    prefixed_list = [f'{x}/' + item for item in filt_list]
    return prefixed_list


def pull_jd_updated_df(x):
    specific_files = jd_updated_data_list(x)
    for file in specific_files:
        s3 = boto3.resource("s3")
        #load from bucket
        obj = s3.Bucket('nhs-dataset').Object(file).get()
        dd = pd.read_csv(obj['Body'])
    return dd

def jd_master_df(a,b):
    #last_information_updated
    till_now_jd_master = pull_jd_updated_df('master_data')
    till_now_jd_master['scraped_date'] = pd.to_datetime(till_now_jd_master['scraped_date'],format='ISO8601')
    max_date = max(till_now_jd_master['scraped_date'])
    max_date = max_date.date()
    #appending only new to old
    need_to_append = pd.DataFrame()
    list_1 = jd_data_list(a)
    for i in list_1:
        if pd.to_datetime(i.split("/")[-1].split("_")[-1].replace(".csv","")).date() > max_date:
            print(f"Starting with {i}")
            df = for_jd_data_list_df(i)
            del df['page_number']
            need_to_append = pd.concat([need_to_append,df],axis=0,ignore_index=True)
            print(f"Done with {i}")
        else:
            pass
    print('------------------------------------------')
    list_2 = data_list(b)
    for i in list_2:
        if pd.to_datetime(i.split("/")[-1].split("_")[-1].replace(".csv","")).date() > max_date:
            print(f"Starting with {i}")
            df = for_data_list_df(i)
            del df['page_number']
            need_to_append = pd.concat([need_to_append,df],axis=0,ignore_index=True)
            print(f"Done with {i}")
        else:
            pass
    jd_master = pd.concat([till_now_jd_master,need_to_append],axis=0,ignore_index=True)
    jd_master.to_csv(r"/home/ec2-user/scrape_data/master_data/Jobs_Information_Master.csv",index=False)
    push_to_s3("master_data","Jobs_Information_Master")
    delete_files("master_data","Jobs_Information_Master.csv")
    # email_people(email_cred,"Jobs Information Master")
    return jd_master

jd_master = jd_master_df('job_information_updated','jd_page_data')


### Update Code
def fixing_date(jd_master,listing_page_master):
    jd_master['scraped_date'] = pd.to_datetime(jd_master['scraped_date'],format='ISO8601')
    listing_page_master['scrap_date'] = pd.to_datetime(listing_page_master['scrap_date'],format='ISO8601')
    # Assuming 'closing_date' is the column with date strings
    listing_page_master['closing_date'] = pd.to_datetime(
        listing_page_master['closing_date'], 
        infer_datetime_format=True, 
        errors='coerce')
    # Convert the datetime column to the desired 'yyyy-mm-dd' format
    listing_page_master['closing_date'] = pd.to_datetime(listing_page_master['closing_date'].dt.strftime('%Y-%m-%d'))
    # Assuming 'closing_date' is the column with date strings
    jd_master['date_posted'] = pd.to_datetime(
        jd_master['date_posted'], 
        infer_datetime_format=True, 
        errors='coerce')
    # Convert the datetime column to the desired 'yyyy-mm-dd' format
    jd_master['date_posted'] = pd.to_datetime(jd_master['date_posted'].dt.strftime('%Y-%m-%d'))
    print(f"Done with fixing dates")
    return jd_master,listing_page_master

jd_master,listing_page_master = fixing_date(jd_master,listing_page_master)

#there are two types
#1. job found
#2. job not found or closed

#check and flags for 
#1.flag for job active or not (2 columns - active_not_active flag and how_many_days_to_close)
## active and not active flag
#1.1 if active then in how many days the job is going to get closed
#1.2 if not active then flag it as not active

#2.flag change for salary (3 columns - salary_same_or_not,salary_changed_to,salary_changed_when)
## salary same or not flag
#2.1 if salary changed then when it changed and what it changed to
#2.2 if salary same then flag it as not changed

#3.Latested date of update (latest_job_info_update)
#3.1 when was job information updated last

def active_inactive(x):
    if x.days <= 0 :
        return 'closed'
    else:
        return 'active'

def salary_check(x):
    x = x.replace(' to ',' ').replace(' a year','').replace('£','').replace(',','').replace(' an hour','').split(' ')
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

def update_information(jd_master,listing_all_df):
    print(f"Starting with Updating")
    start_time = time.time()
    # jd_master = pd.read_csv(r"/home/ec2-user/scrape_data/master_data/Jobs_Information_Master.csv")
    ### Update Code
    jd_master['scraped_date'] = pd.to_datetime(jd_master['scraped_date'])
    # Assuming 'closing_date' is the column with date strings
    jd_master['date_posted'] = pd.to_datetime(
        jd_master['date_posted'], 
        infer_datetime_format=True, 
        errors='coerce')
    jd_master.drop_duplicates('short_job_link',keep='last',inplace=True)
    jd_master.reset_index(drop=True,inplace=True)
    # Convert the datetime column to the desired 'yyyy-mm-dd' format
    jd_master['date_posted'] = pd.to_datetime(jd_master['date_posted'].dt.strftime('%Y-%m-%d'))
    listing_all_df['scrap_date'] = pd.to_datetime(listing_all_df['scrap_date'])
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
    active_jobs_2.to_csv(r"/home/ec2-user/scrape_data/master_data/Active_Jobs.csv",index=False)
    listing_all_df.to_csv(r"/home/ec2-user/scrape_data/master_data/Latest_Updated_Master.csv",index=False)
    push_to_s3("master_data","Active_Jobs")
    push_to_s3("master_data","Latest_Updated_Master")
    delete_files("master_data","Active_Jobs.csv")
    delete_files("master_data","Latest_Updated_Master.csv")
    end_time = time.time()
    duration = end_time - start_time
    # email_people(email_cred,"Active Jobs")
    print(f"Time taken to create Active Jobs: {duration/60} mintues")
    return active_jobs_2

# jd_master = jd_master_df('job_information_updated','jd_page_data')
# jd_master = fetching_df('master_data','Jobs_Information_Master')
# listing_all_df = fetching_df('master_data','Listing_Page_Master')
# listing_all_df = pd.read_csv(r"/home/ec2-user/scrape_data/master_data/Listing_Page_Master.csv")
active_jobs = update_information(jd_master,listing_page_master)

#####Categorisation
def specialisation_list_df_list(x,y):
    df = pd.read_excel(r"/home/ec2-user/scrape_data/master_data/{}.xlsx".format(x),sheet_name=y)
    df_list = list(df[y].unique())
    return df_list

def specialisation_list_df(x,y):
    df = pd.read_excel(r"/home/ec2-user/scrape_data/master_data/{}.xlsx".format(x),sheet_name=y)
    return df

#active_Jobs
active_jobs['Role_'] = active_jobs['Role'].str.replace("-"," - ").str.replace("/"," / ").str.replace('*'," ").str.replace(","," , ").str.replace("'","").str.replace('–',' – ')

#other_keywords
administration_keywords = specialisation_list_df_list('Final_Speciality','Admin_Keywords')
healthcare_keywords = specialisation_list_df_list('Final_Speciality','HCA_Keywords')
engineer_keywords = specialisation_list_df_list('Final_Speciality','Engineer_Keywords')

#speciality_list
final_speciality = specialisation_list_df('Final_Speciality','Final_Speciality')
final_speciality['Speciality_Lower'] = final_speciality['Specialties'].apply(lambda x: str(x).lower().strip())
final_speciality.rename(columns={'Category':"Major Specialisation",'Specialties':"Specialisation","Speciality_Lower":"Specialisation_Lower"},inplace=True)

##### AHP
ahp_df = specialisation_list_df('Final_Speciality','AHP_Keywords')
ahp_df['Speciality_2'] = ahp_df['Speciality'].str.split(',')
ahp_df = ahp_df.explode('Speciality_2').reset_index(drop=True)
ahp_df['Speciality_Lower'] = ahp_df['Speciality_2'].apply(lambda x: str(x).lower().strip())
ahp_df.rename(columns={"Major Speciality":'Major Specialisation','Speciality_2':'Specialisation','Speciality_Lower':"Specialisation_Lower"},inplace=True)

#nurse_speciality
nurse_final_speciality = specialisation_list_df('Final_Speciality','Nurse_Keywords')
nurse_final_speciality['Specialisation_Lower'] = nurse_final_speciality['Specialisation'].apply(lambda x: str(x).lower().strip())

#keywords_for_doctors
keyword_list_1 = specialisation_list_df_list('Final_Speciality','Doctor_Keywords')

### Nurse
def nurse_classification(x,y):
    ### Nurse
    # Define the job titles to match
    job_titles = x
    # Create the Matcher and PhraseMatcher objects
    matcher = Matcher(nlp.vocab)
    phrase_matcher = PhraseMatcher(nlp.vocab)
    # Add patterns to the Matcher for exact matches
    for job_title in job_titles:
        matcher.add(job_title, [[{"LOWER": job_title.lower()}]])
    # Add patterns to the PhraseMatcher for phrase matches
    patterns = [nlp.make_doc(text) for text in job_titles]
    phrase_matcher.add("JOB_TITLES", patterns)
    def classify_title(title):
        doc = nlp(title)
        matches = matcher(doc) + phrase_matcher(doc)  # Combine both match results
        matches = sorted(matches, key=lambda x: x[1])  # Sort matches by their start position
        matched_titles = []
        for match_id, start, end in matches:
            span = doc[start:end]  # The matched span
            if span.text not in matched_titles:  # Check for duplicates
                matched_titles.append(span.text)
        return matched_titles if matched_titles else []
    ####
    y['Role_Lower'] = y['Role_'].apply(lambda x: str(x).lower().strip())
    y[(y['Role_Lower'].str.contains('nurse')) | (y['Role_Lower'].str.contains('nursing')) | (y['Role_Lower'].str.contains('midwife')) | (y['Role_Lower'].str.contains('sister'))]
    y.loc[(y['Role_Lower'].str.contains('nurse')) | (y['Role_Lower'].str.contains('nursing')) ,'Nurse_Tag']="Nurse"
    y["Nurse_Tag"] = y["Nurse_Tag"].fillna('')
    y.loc[y["Nurse_Tag"]=="Nurse",'Nurse_Specialisation'] = y.loc[y["Nurse_Tag"]=="Nurse",'Role_Lower'].apply(lambda x: classify_title(x))
    y['Nurse_Specialisation_Len'] = y['Nurse_Specialisation'].apply(lambda x: "" if str(x)=="nan" else len(x))
    y['Nurse_Specialisation_Final'] = y['Nurse_Specialisation'].apply(lambda x: x[0] if str(x)!="nan" and len(x)>0 else "")
    print("Nursing Categorisation Done")
    return y

active_jobs = nurse_classification(nurse_final_speciality['Specialisation_Lower'].unique(),active_jobs)

#physician_associate
def physician_associate_classification(x):
    #physician_associate
    #######
    # def classify_title(title):
    #     doc = nlp(title)
    #     matches = matcher(doc) + phrase_matcher(doc)  # Combine both match results
    #     matches = sorted(matches, key=lambda x: x[1])  # Sort matches by their start position
    #     matched_titles = []
    #     for match_id, start, end in matches:
    #         span = doc[start:end]  # The matched span
    #         if span.text not in matched_titles:  # Check for duplicates
    #             matched_titles.append(span.text)
    #     return matched_titles if matched_titles else []
    #####
    x.loc[(x['Role_Lower'].str.contains('physician associates')) | (x['Role_Lower'].str.contains('physician associate')) | (x['Role_Lower'].str.contains('physician assistant')),'Physician_Associate_Tag'] = "Physician_Associate"
    x["Physician_Associate_Tag"] = x["Physician_Associate_Tag"].fillna('')
    x['Physician_Associate_Specialisation'] = ''
    x['Physician_Associate_Specialisation_Len'] = ''
    x['Physician_Associate_Specialisation_Final'] = ''
    print("Physician Associate Categorisation Done")
    return x

active_jobs = physician_associate_classification(active_jobs)

#ahp
def aha_classification(x,y):
    # Define the job titles to match
    job_titles = x
    # Create the Matcher and PhraseMatcher objects
    matcher = Matcher(nlp.vocab)
    phrase_matcher = PhraseMatcher(nlp.vocab)
    # Add patterns to the Matcher for exact matches
    for job_title in job_titles:
        matcher.add(job_title, [[{"LOWER": job_title.lower()}]])
    # Add patterns to the PhraseMatcher for phrase matches
    patterns = [nlp.make_doc(text) for text in job_titles]
    phrase_matcher.add("JOB_TITLES", patterns)    
    #######
    def classify_title(title):
        doc = nlp(title)
        matches = matcher(doc) + phrase_matcher(doc)  # Combine both match results
        matches = sorted(matches, key=lambda x: x[1])  # Sort matches by their start position
        matched_titles = []
        for match_id, start, end in matches:
            span = doc[start:end]  # The matched span
            if span.text not in matched_titles:  # Check for duplicates
                matched_titles.append(span.text)
        return matched_titles if matched_titles else []
    ####
    y["AHP_Tag"] = ''
    y.loc[y['Role_'].str.contains('Allied Health Professionals'),'AHP_Tag'] = 'AHP'
    y['AHP_Specialisation'] = y['Role_Lower'].apply(lambda x: classify_title(x))
    y['AHP_Specialisation_Len'] = y['AHP_Specialisation'].apply(lambda x: len(x))
    y.loc[active_jobs['AHP_Specialisation_Len'] !=0,'AHP_Tag'] = "AHP"
    y['AHP_Specialisation_Final'] = y['AHP_Specialisation'].apply(lambda x: x[0] if len(x)>0 else "")
    print("AHP Categorisation Done")
    return y

active_jobs = aha_classification(ahp_df['Specialisation_Lower'].unique(),active_jobs)

#Doctor
def doctor_classification(x,y):
    active_jobs['Doctor_Tag'] = ''
    # Define the job titles to match
    job_titles = x
    # Create the Matcher and PhraseMatcher objects
    matcher = Matcher(nlp.vocab)
    phrase_matcher = PhraseMatcher(nlp.vocab)
    # Add patterns to the Matcher for exact matches
    for job_title in job_titles:
        matcher.add(job_title, [[{"LOWER": job_title.lower()}]])
    # Add patterns to the PhraseMatcher for phrase matches
    patterns = [nlp.make_doc(text) for text in job_titles]
    phrase_matcher.add("JOB_TITLES", patterns)
    #######
    def classify_title(title):
        doc = nlp(title)
        matches = matcher(doc) + phrase_matcher(doc)  # Combine both match results
        matches = sorted(matches, key=lambda x: x[1])  # Sort matches by their start position
        matched_titles = []
        for match_id, start, end in matches:
            span = doc[start:end]  # The matched span
            if span.text not in matched_titles:  # Check for duplicates
                matched_titles.append(span.text)
        return matched_titles if matched_titles else []
    ######
    y['Doctor_Specialisation'] = y['Role_Lower'].apply(lambda x: classify_title(x))
    y['Doctor_Specialisation_Len'] = y['Doctor_Specialisation'].apply(lambda x: len(x))
    y.loc[(y['Doctor_Specialisation_Len']!=0) & (y['Nurse_Tag']!='Nurse') & (y['Physician_Associate_Tag']!='Physician_Associate') & (y['AHP_Tag']!='AHP'),'Doctor_Tag']='Doctor'
    y['Doctor_Specialisation_Final'] = y['Doctor_Specialisation'].apply(lambda x: x[0] if len(x)>0 else "")
    print("Doctor Categorisation Done")
    return y

active_jobs = doctor_classification(final_speciality['Specialisation_Lower'].unique(),active_jobs)

#Administration
def Administration_classification(x,y):
    administration_keywords_lower = [i.strip().lower() for i in x]
    # Define the job titles to match
    job_titles = administration_keywords_lower
    # Create the Matcher and PhraseMatcher objects
    matcher = Matcher(nlp.vocab)
    phrase_matcher = PhraseMatcher(nlp.vocab)
    # Add patterns to the Matcher for exact matches
    for job_title in job_titles:
        matcher.add(job_title, [[{"LOWER": job_title.lower()}]])
    # Add patterns to the PhraseMatcher for phrase matches
    patterns = [nlp.make_doc(text) for text in job_titles]
    phrase_matcher.add("JOB_TITLES", patterns)
    #######
    def classify_title(title):
        doc = nlp(title)
        matches = matcher(doc) + phrase_matcher(doc)  # Combine both match results
        matches = sorted(matches, key=lambda x: x[1])  # Sort matches by their start position
        matched_titles = []
        for match_id, start, end in matches:
            span = doc[start:end]  # The matched span
            if span.text not in matched_titles:  # Check for duplicates
                matched_titles.append(span.text)
        return matched_titles if matched_titles else []
    #####
    y['Admin_Tag'] = ''
    y['Admin_Specialisation'] = y['Role_Lower'].apply(lambda x: classify_title(x))
    y['Admin_Specialisation_Len'] = y['Admin_Specialisation'].apply(lambda x: len(x))
    y.loc[(y['Admin_Specialisation_Len']!=0) & (y['Doctor_Tag']!='Doctor') & (y['Nurse_Tag']!='Nurse') & (y['Physician_Associate_Tag']!='Physician_Associate') & (y['AHP_Tag']!='AHP'),'Admin_Tag']='Admin'
    y['Admin_Specialisation_Final'] = y['Admin_Specialisation'].apply(lambda x: x[0] if len(x)>0 else "")
    print("Administration Categorisation Done")
    return y

active_jobs = Administration_classification(administration_keywords,active_jobs)

#Health care Assistanct
def hca_classification(x,y):
    hca_keywords_lower = [i.strip().lower() for i in x]
    # Define the job titles to match
    job_titles = hca_keywords_lower
    # Create the Matcher and PhraseMatcher objects
    matcher = Matcher(nlp.vocab)
    phrase_matcher = PhraseMatcher(nlp.vocab)
    # Add patterns to the Matcher for exact matches
    for job_title in job_titles:
        matcher.add(job_title, [[{"LOWER": job_title.lower()}]])
    # Add patterns to the PhraseMatcher for phrase matches
    patterns = [nlp.make_doc(text) for text in job_titles]
    phrase_matcher.add("JOB_TITLES", patterns)
    #######
    def classify_title(title):
        doc = nlp(title)
        matches = matcher(doc) + phrase_matcher(doc)  # Combine both match results
        matches = sorted(matches, key=lambda x: x[1])  # Sort matches by their start position
        matched_titles = []
        for match_id, start, end in matches:
            span = doc[start:end]  # The matched span
            if span.text not in matched_titles:  # Check for duplicates
                matched_titles.append(span.text)
        return matched_titles if matched_titles else []
    ########
    y['HCA_Tag'] = ''
    y['HCA_Specialisation'] = y['Role_Lower'].apply(lambda x: classify_title(x))
    y['HCA_Specialisation_Len'] = y['HCA_Specialisation'].apply(lambda x: len(x))
    y.loc[(y['HCA_Specialisation_Len']!=0) & (y['Doctor_Tag']!='Doctor') & (y['Nurse_Tag']!='Nurse') & (y['Physician_Associate_Tag']!='Physician_Associate') & (y['AHP_Tag']!='AHP') & (y['Admin_Tag']!='Admin'),'HCA_Tag']='HCA'
    y['HCA_Specialisation_Final'] = y['HCA_Specialisation'].apply(lambda x: x[0] if len(x)>0 else "")
    print("HCA Categorisation Done")
    return y

active_jobs = hca_classification(healthcare_keywords,active_jobs)

#engineer
def engineer_classification(x,y):
    engineer_keywords_lower = [i.strip().lower() for i in x]
    # Define the job titles to match
    job_titles = engineer_keywords_lower
    # Create the Matcher and PhraseMatcher objects
    matcher = Matcher(nlp.vocab)
    phrase_matcher = PhraseMatcher(nlp.vocab)
    # Add patterns to the Matcher for exact matches
    for job_title in job_titles:
        matcher.add(job_title, [[{"LOWER": job_title.lower()}]])
    # Add patterns to the PhraseMatcher for phrase matches
    patterns = [nlp.make_doc(text) for text in job_titles]
    phrase_matcher.add("JOB_TITLES", patterns)
    #######
    def classify_title(title):
        doc = nlp(title)
        matches = matcher(doc) + phrase_matcher(doc)  # Combine both match results
        matches = sorted(matches, key=lambda x: x[1])  # Sort matches by their start position
        matched_titles = []
        for match_id, start, end in matches:
            span = doc[start:end]  # The matched span
            if span.text not in matched_titles:  # Check for duplicates
                matched_titles.append(span.text)
        return matched_titles if matched_titles else []
    ######
    y['Engineer_Tag'] = ''
    y['Engineer_Specialisation'] = y['Role_Lower'].apply(lambda x: classify_title(x))
    y['Engineer_Specialisation_Len'] = y['Engineer_Specialisation'].apply(lambda x: len(x))
    y.loc[(y['Engineer_Specialisation_Len']!=0) & (y['Doctor_Tag']!='Doctor') & (y['Nurse_Tag']!='Nurse') & (y['Physician_Associate_Tag']!='Physician_Associate') & (y['AHP_Tag']!='AHP') & (y['Admin_Tag']!='Admin') & (y['HCA_Tag']!='HCA') & (y['Engineer_Tag']!='Admin'),'Engineer_Tag']='Engineer'
    y['Engineer_Specialisation_Final'] = y['Engineer_Specialisation'].apply(lambda x: x[0] if len(x)>0 else "")
    print("Engineer Categorisation Done")
    return y

active_jobs = engineer_classification(engineer_keywords,active_jobs)

def push_to_s3_categorisation_file(x,y):
    print(f'Pushing {y} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}.xlsx',Key = f'{x}/{y}.xlsx')
    print(f'{y} pushed to bucket in {x}')


def final_checks(active_jobs,final_speciality,nurse_final_speciality,ahp_df):
    #final_tag
    active_jobs['Final_Tag'] = ''
    active_jobs.loc[active_jobs['Doctor_Tag']=='Doctor','Final_Tag'] = 'Doctor'
    active_jobs.loc[active_jobs['Nurse_Tag']=='Nurse','Final_Tag'] = 'Nurse'
    active_jobs.loc[(active_jobs['Nurse_Tag']=='Nurse') & (active_jobs['AHP_Tag']=="AHP"),'Final_Tag'] = 'Nurse'
    active_jobs.loc[(active_jobs['Engineer_Tag']=='Engineer'),'Final_Tag'] = 'Engineer'
    active_jobs.loc[(active_jobs['Physician_Associate_Tag']=='Physician_Associate'),'Final_Tag'] = 'Physician_Associate'
    active_jobs.loc[(active_jobs['Admin_Tag']=='Admin'),'Final_Tag'] = 'Admin'
    active_jobs.loc[(active_jobs['HCA_Tag']=='HCA'),'Final_Tag'] = 'HCA'
    active_jobs.loc[(active_jobs['AHP_Tag']=='AHP'),'Final_Tag'] = 'AHP'    
    #Final-specialisation
    active_jobs['Final_Specialisation'] = ''
    active_jobs.loc[active_jobs['Final_Tag']=='Doctor','Final_Specialisation'] = active_jobs['Doctor_Specialisation_Final']
    active_jobs.loc[active_jobs['Final_Tag']=='Nurse','Final_Specialisation'] = active_jobs['Nurse_Specialisation_Final']
    active_jobs.loc[active_jobs['Final_Tag']=='Engineer','Final_Specialisation'] = active_jobs['Engineer_Specialisation_Final']
    active_jobs.loc[active_jobs['Final_Tag']=='Physician_Associate','Final_Specialisation'] = active_jobs['Physician_Associate_Specialisation_Final']
    active_jobs.loc[active_jobs['Final_Tag']=='Admin','Final_Specialisation'] = active_jobs['Admin_Specialisation_Final']
    active_jobs.loc[active_jobs['Final_Tag']=='HCA','Final_Specialisation'] = active_jobs['HCA_Specialisation_Final']
    active_jobs.loc[active_jobs['Final_Tag']=='AHP','Final_Specialisation'] = active_jobs['AHP_Specialisation_Final']
    #separate_df
    doctors_df = active_jobs[active_jobs['Final_Tag']=='Doctor'].merge(final_speciality[['Major Specialisation','Specialisation','Specialisation_Lower']],left_on='Final_Specialisation',right_on='Specialisation_Lower',how='left')
    nurse_df = active_jobs[active_jobs['Final_Tag']=='Nurse'].merge(nurse_final_speciality[['Major Specialisation','Specialisation','Specialisation_Lower']],left_on='Final_Specialisation',right_on='Specialisation_Lower',how='left')
    ahp_df_ = active_jobs[active_jobs['Final_Tag']=='AHP'].merge(ahp_df[['Major Specialisation','Specialisation','Specialisation_Lower']],left_on='Final_Specialisation',right_on='Specialisation_Lower',how='left')
    active_jobs['Major Specialisation'] = ''
    active_jobs['Specialisation'] = ''
    active_jobs['Specialisation_Lower'] = ''
    active_jobs_without_doctor_nurse = active_jobs[(active_jobs['Final_Tag']!='Doctor') & (active_jobs['Final_Tag']!='Nurse') & (active_jobs['Final_Tag']!='AHP')]
    active_jobs_final = pd.concat([doctors_df,nurse_df,ahp_df_,active_jobs_without_doctor_nurse],ignore_index=True)
    active_jobs_final_2 = active_jobs_final[['Role', 'salary', 'closing_date', 'job_code', 'active_inactive', 'salary_range_start',
       'salary_range_end', 'short_job_link', 'scrap_date', 'job_summary',
       'job_discription', 'employer_name', 'employer_address',
       'employer_post_code', 'employer_website', 'contact_person_position',
       'contact_person_name', 'contact_person_email', 'contact_person_number','Doctor_Tag','Nurse_Tag',
        'Physician_Associate_Tag','Admin_Tag','Engineer_Tag','HCA_Tag','AHP_Tag','Final_Tag',
        'Major Specialisation', 'Specialisation']]
    active_jobs_final_2['Major Specialisation'].fillna("",inplace=True)
    active_jobs_final_2['Specialisation'].fillna("",inplace=True)
    active_jobs_final_2.loc[(active_jobs_final_2['Final_Tag']=='') & ((active_jobs_final_2['Role'].str.lower().str.strip().str.contains('doctor')) | active_jobs_final_2['Role'].str.lower().str.strip().str.contains('doctors')),'Final_Tag'] = 'Doctor'
    active_jobs_final_2.to_excel(r"/home/ec2-user/scrape_data/master_data/Active_Jobs_with_categorisation.xlsx")
    push_to_s3_categorisation_file('master_data','Active_Jobs_with_categorisation')
    email_people(email_cred,"Active Jobs with categorisation and all other Files")
    return active_jobs_final_2

active_jobs_final_2 = final_checks(active_jobs,final_speciality,nurse_final_speciality,ahp_df)
