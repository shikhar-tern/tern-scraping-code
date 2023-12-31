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
import spacy
import spacy.cli
# Load spaCy model
nlp = spacy.load("en_core_web_sm")
from spacy.matcher import PhraseMatcher
from spacy.matcher import DependencyMatcher
import numpy as np
import warnings
warnings.filterwarnings('ignore')


# ### Listing Page

# number of pages
sys.stdout = open(r'/home/ec2-user/scrape_data/log.txt','w')


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
    df = pd.read_csv(f"/home/ec2-user/scrape_data/{x}/{y}_{z}.csv")
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
    return new_job


new_job = new_job_df('master_data',final_data)


def jd_push_to_s3(x,y,z):
    print(f'Pushing {y}_{z} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    df = pd.read_csv(f"/home/ec2-user/scrape_data/{x}/{y}_{z}.csv")
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
end_time = time.time()
duration = end_time - start_time
print(f"Time taken: {duration/60} mintues")

# #### Master DF

def master_push_to_s3(x,y,z):
    print(f'Pushing {y}_{z} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    df = pd.read_csv(f"/home/ec2-user/scrape_data/{x}/{y}_{z}.csv")
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}_{z}.csv',Key = f'{x}/{y}_{z}.csv')

def master_df(new_job,df_jd):
    master_final = new_job.merge(df_jd,on=['job_url_hit'],how='left')
    master_final.to_csv(r"/home/ec2-user/scrape_data/master_new_job_data/master_new_job_{}.csv".format(str(date.today())),index=False)
    master_push_to_s3('master_new_job_data','master_new_job',str(date.today()))
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
    df = pd.read_csv(f"/home/ec2-user/scrape_data/{x}/{y}.csv")
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}.csv',Key = f'{x}/{y}.csv')
    print(f'{y} pushed to bucket in {x}')


def listing_page_master_df(x):
    print('Starting with Listing Page')
    old_listing_data = pd.DataFrame()
    specific_files = data_list(x)
    for file in specific_files:
        print(f'Starting with: {file}')
        s3 = boto3.resource("s3")
        #load from bucket
        obj = s3.Bucket('nhs-dataset').Object(file).get()
        dd = pd.read_csv(obj['Body'])
        # print(dd.columns)
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


listing_page_master = listing_page_master_df('listing_page_data')


# #### Merged Master

def merged_master_df(x):
    print('Starting With Merged Master')
    old_listing_data = pd.DataFrame()
    specific_files = data_list(x)
    # Print the list of specific files
    #print(specific_files)
    for file in specific_files:
       # print(f"Starting with {file}")
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
    old_listing_data.to_csv(r"/home/ec2-user/scrape_data/master_data/Merged_Master.csv",index=False)
    push_to_s3('master_data','Merged_Master')
    return old_listing_data


merged_master = merged_master_df('master_new_job_data')
del merged_master


# #### New Jobs Master
def new_jobs_master_df(x):
    print('Starting with New Jobs Data')
    old_listing_data = pd.DataFrame()
    specific_files = data_list(x)
    # Print the list of specific files
    for file in specific_files:
        s3 = boto3.resource("s3")
        #load from bucket
        obj = s3.Bucket('nhs-dataset').Object(file).get()
        dd = pd.read_csv(obj['Body'])
        dd['job_url_hit'] = dd['job_url'].apply(lambda x: remove_keyword_param(x))
        dd['job_code'] = dd['job_url_hit'].apply(lambda x:extract_job_codes(x))
        dd['short_job_link'] = dd['job_url_hit'].apply(lambda x:short_link(x))
        dd = dd.drop_duplicates(['scrap_date','job_code'],keep='first').reset_index(drop=True) 
        old_listing_data = pd.concat([old_listing_data,dd],axis=0,ignore_index=True)
    del old_listing_data['keyword']
    old_listing_data.to_csv(r"/home/ec2-user/scrape_data/master_data/New_Jobs_Master.csv",index=False)
    push_to_s3('master_data','New_Jobs_Master')
    return old_listing_data


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
    jd_master.to_csv(r"/home/ec2-user/scrape_data/master_data/Jobs_Information_Master.csv",index=False)
    push_to_s3("master_data","Jobs_Information_Master")
    return jd_master


jd_master = jd_master_df('job_information_updated','jd_page_data')


### Update Code
jd_master['scraped_date'] = pd.to_datetime(jd_master['scraped_date'])
listing_page_master['scrap_date'] = pd.to_datetime(listing_page_master['scrap_date'])

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

def pull_active_jobs_append_new_job_list(x,new_job):
    specific_files = active_job_data_list(x)
    active_jobs_df = pd.DataFrame()
    for file in specific_files:
        s3 = boto3.resource("s3")
        #load from bucket
        obj = s3.Bucket('nhs-dataset').Object(file).get()
        dd = pd.read_csv(obj['Body'])
        active_jobs_df = pd.concat([active_jobs_df,dd],axis=0,ignore_index=True)
    active_job_list = list(set(active_jobs_df['short_job_link'].unique())) + list(set(new_job['short_job_link']))
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
    job_lists = pull_active_jobs_append_new_job_list('master_data',new_job)

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


### categorisation
def remove_roll_extract_elements(text, phrase_matcher):
    doc = nlp(text)
    matches = phrase_matcher(doc)
    result_list = []

    # Extract matched phrases
    phrase_matches = [doc[start:end].text for match_id, start, end in matches]

    # If phrase_matches is not empty, use it as the result
    if phrase_matches:
        result_list = phrase_matches
    else:
        # Try exact match
        exact_matches = [word.text for word in doc if word.text.lower() in [keyword.lower() for keyword in remove_roles_lower]]
        # If exact_matches is not empty, use it as the result
        if exact_matches:
            result_list = exact_matches
    result_list = list(set(result_list))
    return result_list

def k1_extract_elements(text, phrase_matcher):
    doc = nlp(text)
    matches = phrase_matcher(doc)

    # Extract matched phrases
    phrase_matches = [doc[start:end].text for match_id, start, end in matches]

    # If phrase_matches is not empty, use it as the result
    if phrase_matches:
        result_list = list(set(phrase_matches))
    else:
        # Try exact match
        exact_matches = [word.text for word in doc if word.text.lower() in [keyword.lower() for keyword in keyword_list_1_lower]]
        
        # If exact_matches is not empty, use it as the result
        if exact_matches:
            result_list = list(set(exact_matches))
        else:
            # If both phrase_matches and exact_matches are empty, return an empty list
            result_list = []

    return result_list

def k2_extract_elements(text, phrase_matcher):
    doc = nlp(text)
    matches = phrase_matcher(doc)

    # Extract matched phrases
    phrase_matches = [doc[start:end].text for match_id, start, end in matches]

    # If phrase_matches is not empty, use it as the result
    if phrase_matches:
        result_list = list(set(phrase_matches))
    else:
        # Try exact match
        exact_matches = [word.text for word in doc if word.text.lower() in [keyword.lower() for keyword in keyword_list_2_lower]]
        
        # If exact_matches is not empty, use it as the result
        if exact_matches:
            result_list = list(set(exact_matches))
        else:
            # If both phrase_matches and exact_matches are empty, return an empty list
            result_list = []

    return result_list

def fixes(x):
    if len(x)==0:
        return x
    else:
        return list(set(final_speciality.loc[final_speciality['Specialties_Lower'].isin(x),'Major Specialisation']))
    
def fixes_spe(x):
    if len(x)==0:
        return x
    else:
        return list(set(final_speciality.loc[final_speciality['Specialties_Lower'].isin(x),'Specialties']))
    
def capitalize_elements(x):
    l1 = []
    for i in x.split(', '):
        l1.append(i.capitalize())
    return ', '.join(l1)

def tag_role_uk_with_specialties_fixed(role, keyword_list_2_lower):
    """
    Revised function to more accurately tag roles, ensuring nurses and administrative roles are not misclassified as doctors.
    """
    # Define keywords for each category
    nurse_keywords = ['Nurse', 'Nursing','Sister']
    doctor_keywords = keyword_list_2_lower  # List of doctor specializations
    healthcare_keywords = ['Therapist', 'Psychologist', 'Healthcare', 'Clinical','Pharmacist', 'Practitioner', 'Physician', 'Medical','HCA','Health care assistant','Midwife']
    administration_keywords = ['Admin support','Admin', 'Administrator', 'Manager', 'Coordinator', 'Supervisor', 'Lead', 'Secretary', 'Support','Receptionist', 'Clerk','Clerical officer','Administration assistant','Mdt', 'Analyst',' Business Intelligence']
    engineer_keywords = ['Engineer', 'Engineering', 'Technical', 'Technician']
    project_management_keywords = ['Project', 'Programme', 'Planner']

    role_lower = role.lower()

    # Check for nurse keywords first to avoid misclassification
    if any(keyword.lower() in role_lower for keyword in nurse_keywords):
        return 'Nurse'

    # Check if the role matches any doctor specialization
    elif any(specialization.lower() in role_lower for specialization in doctor_keywords):
        return 'Doctors'

    # Check for other categories
    elif any(keyword.lower() in role_lower for keyword in healthcare_keywords):
        return 'Health Care Assistant'
    elif any(keyword.lower() in role_lower for keyword in administration_keywords):
        return 'Administration'
    elif any(keyword.lower() in role_lower for keyword in engineer_keywords):
        return 'Engineer'
    elif any(keyword.lower() in role_lower for keyword in project_management_keywords):
        return 'Project Management'
    else:
        return 'Other - Requires Further Review'

def find_first_match(row):
    x = row['Role']
    y = row['Specialties']
    
    x = x.lower()
    y = y.lower()

    y_list = y.split(', ')

    find_element = []
    for i in y_list:
        start_index = x.find(i)
        if start_index != -1:
            end_index = start_index + len(i) - 1
        else:
            end_index = None
        find_element.append((i,start_index,end_index))
    min_element = min(find_element, key=lambda x: x[1])
    return min_element[0].title()

def fixes_(x):
    a = list(set(final_speciality.loc[final_speciality['Specialties']==x,'Major Specialisation']))
    if len(a) == 0:
        return None
    else:
        return a[0]
    
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
    
def band_grade(x):
    #band and the grade
    if (str(x) == 'nan') | (str(x) == '-'):
        return ['-','-']
    elif str(x)[0] == 'B':
        return [x,'-']
    elif str(x)[0] != 'B':
        return ['-',x]
    
def specialisation_list(x):
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
    filt_list = [item for item in filtered_list if item == 'Final_Speciality.csv']
    prefixed_list = [f'{x}/' + item for item in filt_list]
    return prefixed_list


def final_speciality_df(x):
    old_listing_data = pd.DataFrame()
    specific_files = specialisation_list(x)
    for file in specific_files:
        #load from bucket
        s3 = boto3.resource("s3")
        obj = s3.Bucket('nhs-dataset').Object(file).get()
        dd = pd.read_csv(obj['Body'])
        old_listing_data = pd.concat([old_listing_data,dd],axis=0,ignore_index=True)
    return old_listing_data


active_jobs = active_job.copy()
jd_latest = jd_master.copy()
listing = listing_page_master.copy()
final_speciality = final_speciality_df('master_data')
final_speciality['Specialties_Lower'] = final_speciality['Specialties'].apply(lambda x: x.lower())
final_speciality.rename(columns={"Category":'Major Specialisation'},inplace=True)

df = active_jobs.copy()

# keyword_list_1 = ["Doctors", "Medics","Health care assistants","Physician's assistants", "Nurses"]
keyword_list_1 = ["Junior Doctor","General Practitioner","Consultant","Specialty Doctor","Specialist","ST3+ Middle Grade",
                  "Locum Consultant","Senior Consultant","Sr Consultant","Sr. Consultant","Senior Specialist"]
remove_roles = ['HCA','Healthcare Assistant','Health Care Assistant','Nurses','Nurse','Sister','Secretary','Palliative Care','Administration Assistant','Administrator','Receptionist','MDT','Clerk','Clerical Officer','Lab Assistant','Manager','Admin Support','Trainee','Pharmacist','Dietitian','Pharmacy']


keyword_list_1_lower = [x.lower() for x in keyword_list_1]
keyword_list_2 = list(final_speciality['Specialties_Lower'].unique())
keyword_list_2_lower = [x.lower() for x in keyword_list_2]

remove_roles_lower = [x.lower() for x in remove_roles]

### preprocessing
df['Role'] = df['Role'].str.replace("-"," - ").str.replace("/"," / ").str.replace('*'," ").str.replace(","," , ").str.replace("'","").str.replace('–',' – ')
df['Role_Lower'] = df['Role'].apply(lambda x: x.lower())

df2 = df.copy()
# Initialize PhraseMatcher with the nlp pipeline
phrase_matcher = PhraseMatcher(nlp.vocab)
patterns = [nlp(keyword) for keyword in remove_roles_lower]
phrase_matcher.add('Keywords', None, *patterns)
df2['Tag_1_list'] = df2['Role_Lower'].apply(lambda x: remove_roll_extract_elements(x, phrase_matcher))
df2['Tag_1'] = df2['Tag_1_list'].apply(lambda x: ', '.join(x))
# Initialize PhraseMatcher with the nlp pipeline
phrase_matcher = PhraseMatcher(nlp.vocab)
patterns = [nlp(keyword) for keyword in keyword_list_1_lower]
phrase_matcher.add('Keywords', None, *patterns)
# Apply the function to each row in the 'Role' column
df2['k1_Matches_list'] = df2['Role_Lower'].apply(lambda x: k1_extract_elements(x, phrase_matcher))
df2['k1_Matches'] = df2['k1_Matches_list'].apply(lambda x: ', '.join(x))
# Initialize PhraseMatcher with the nlp pipeline
phrase_matcher = PhraseMatcher(nlp.vocab)
patterns = [nlp(keyword) for keyword in keyword_list_2_lower]
phrase_matcher.add('Keywords', None, *patterns)
# Apply the function to each row in the 'Role' column
df2['k2_Matches_list'] = df2['Role_Lower'].apply(lambda x: k1_extract_elements(x, phrase_matcher))
df2['k2_Matches'] = df2['k2_Matches_list'].apply(lambda x: ', '.join(x))
df2['Major Specialisation'] = df2['k2_Matches_list'].apply(lambda x: fixes(x))
df2['Major Specialisation'] = df2['Major Specialisation'].apply(lambda x: ', '.join(x))
df2['Specialties'] = df2['k2_Matches_list'].apply(lambda x: ', '.join(fixes_spe(x)))
df2['Tag_2_Doctors'] = ''
#fixed doctors
df2.loc[(df2['Tag_1']=='') & (df2['k1_Matches']!='') & (df2['Specialties']!=''),'Tag_2_Doctors'] = 'Doctors'
#fixed doctors
df2.loc[(df2['Tag_1']=='') & (df2['k1_Matches']=='') & (df2['Specialties']!=''),'Tag_2_Doctors'] = 'Doctors'
#Final tag
df2['Final_Tag'] = ''
#fixed doctors
df2.loc[(df2['Tag_1']=='') & (df2['k1_Matches']!='') & (df2['Specialties']!=''),'Final_Tag'] = 'Doctors'
#fixed doctors
df2.loc[(df2['Tag_1']=='') & (df2['k1_Matches']=='') & (df2['Specialties']!=''),'Final_Tag'] = 'Doctors'
## make them as tag_1
df2.loc[(df2['Tag_1']!='') & (df2['k1_Matches']=='') & (df2['Specialties']==''),'Final_Tag'] = df2['Tag_1']
## mark them as tag_1
df2.loc[(df2['Tag_1']!='') & (df2['k1_Matches']!='') & (df2['Specialties']==''),'Final_Tag'] = df2['Tag_1']
## mark them as tag_1
df2.loc[(df2['Tag_1']!='') & (df2['k1_Matches']=='') & (df2['Specialties']!=''),'Final_Tag'] = df2['Tag_1']
## mark them as tag_1
df2.loc[(df2['Tag_1']!='') & (df2['k1_Matches']!='') & (df2['Specialties']!=''),'Final_Tag'] = df2['Tag_1']
## don't know what to do with them
df2.loc[(df2['Tag_1']=='') & (df2['k1_Matches']!='') & (df2['Specialties']==''),'Final_Tag'] = ''
## don't know what to do with them right now
df2.loc[(df2['Tag_1']=='') & (df2['k1_Matches']=='') & (df2['Specialties']==''),'Final_Tag'] = ''
df2['Final_Tag'] = df2['Final_Tag'].apply(lambda x: capitalize_elements(x))
df3 = df2[['Role', 'salary', 'closing_date', 'job_code', 'short_job_link',
       'scrap_date', 'scraped_date', 'date_posted', 'Today_Date',
       'Days_to_close', 'active_inactive', 'Updated_on', 'latest_salary',
       'salary_change_date', 'job_summary', 'job_discription', 'employer_name',
       'employer_address', 'employer_post_code', 'employer_website',
       'contact_person_position', 'contact_person_name',
       'contact_person_email', 'contact_person_number','Tag_1','k1_Matches',
       'Major Specialisation', 'Specialties', 'Tag_2_Doctors', 'Final_Tag']]
df3.rename(columns={"Tag_1":'Remove_List','k1_Matches':'Keywords'},inplace=True)
# Re-apply the revised tagging function to the dataframe
df3['Tags_for_remaining'] = df3['Role'].apply(lambda x: tag_role_uk_with_specialties_fixed(x, keyword_list_2_lower))
df3['Finally_Done'] = ''
#Health Care Assistant
df3.loc[(df3['Final_Tag']=='Doctors') & (df3['Tags_for_remaining']=='Health Care Assistant'), 'Finally_Done'] = 'Health Care Assistant'
#Nurse
df3.loc[(df3['Final_Tag']=='Doctors') & (df3['Tags_for_remaining']=='Health Care Assistant'), 'Finally_Done'] = 'Nurse'
#Health care assistant
df3.loc[(df3['Tags_for_remaining']=='Doctors') & (df3['Final_Tag']=='') & (df3['Role'].str.contains('Midwife')==True),'Finally_Done'] = 'Health Care Assistant'
#Administration
df3.loc[(df3['Tags_for_remaining']=='Doctors') & (df3['Final_Tag']=='') & (df3['Role'].str.contains('Analyst')==True),'Finally_Done'] = 'Administration'
#Administration
df3.loc[(df3['Tags_for_remaining']=='Doctors') & (df3['Final_Tag']=='') & (df3['Role'].str.contains('Assistant')==True),'Finally_Done'] = 'Administration'
#Therapies
df3.loc[(df3['Tags_for_remaining']=='Doctors') & (df3['Final_Tag']=='') & (df3['Role'].str.contains('Therapies')==True),'Finally_Done'] = 'Health Care Assistant'
#fixed doctors
df3.loc[(df3['Tags_for_remaining']=='Doctors') & (df3['Final_Tag']=='Doctors'), 'Finally_Done'] = 'Doctors'
#Pharmacist
df3.loc[(df3['Tags_for_remaining']=='Doctors') & (df3['Final_Tag'].isin(['Pharmacy','Pharmacist','Pharmacist, Pharmacy', 'Pharmacy, Pharmacist','Manager, Pharmacy','Trainee, Pharmacy','Pharmacist, Palliative care','Administrator, Pharmacy'])), 'Finally_Done'] = 'Pharmacist'
#fixed Dietitian
df3.loc[(df3['Tags_for_remaining']=='Doctors') & (df3['Final_Tag'].isin(['Dietitian','Dietitian, Palliative care', 'Palliative care, Dietitian'])), 'Finally_Done'] = 'Dietitian'
#Health Care Assistant
df3.loc[(df3['Tags_for_remaining']=='Doctors') & (df3['Final_Tag'].isin(['Hca', 'Health care assistant, Hca','Healthcare assistant','Health care assistant','Hca, Health care assistant'])), 'Finally_Done'] = 'Health Care Assistant'
#Palliative care
df3.loc[(df3['Tags_for_remaining']=='Doctors') & (df3['Final_Tag'].isin(['Palliative care', 'Palliative care, Secretary','Secretary, Palliative care', 'Palliative care, Manager'])), 'Finally_Done'] = 'Palliative Care'
#Sister, ->Health Care Assistant
df3.loc[(df3['Tags_for_remaining']=='Doctors') & (df3['Final_Tag'].isin(['Nurse', 'Nursing','Sister'])), 'Finally_Done'] = 'Nurse'
#Manager -> Administration
df3.loc[(df3['Tags_for_remaining']=='Doctors') & (df3['Final_Tag'].isin(['Manager','Secretary','Administrator','Administrator, Palliative care','Palliative care, Administrator','Receptionist','Clerk','Receptionist, Clerk','Clerical officer, Receptionist', 'Receptionist, Clerical officer','Clerical officer','Admin support','Administration assistant','Administrator, Receptionist','Receptionist, Administrator','Administrator, Secretary','Secretary, Administrator', 'Admin support, Receptionist','Admin support, Administrator','Mdt', 'Secretary, Mdt', 'Mdt, Secretary', 'Admin support, Administrator','Manager, Mdt','Analyst'])), 'Finally_Done'] = 'Administration'
#Other - Requires Further Review
df3.loc[(df3['Tags_for_remaining']=='Doctors') & (df3['Final_Tag']=='Trainee'), 'Finally_Done'] = 'Other - Requires Further Review'
#Other - Requires Further Review
df3.loc[(df3['Tags_for_remaining']=='Doctors') & (df3['Final_Tag'] ==''), 'Finally_Done'] = 'Other - Requires Further Review'
#'' ones
df3.loc[df3['Finally_Done']=='','Finally_Done'] = df3['Tags_for_remaining']
test_1 = df3[df3['Major Specialisation'].str.contains(',')==False]
test = df3[df3['Major Specialisation'].str.contains(',')==True]
test['single_category'] = test.apply(find_first_match, axis=1)
test['Major Specialisation'] = test['single_category'].apply(lambda x: fixes_(x))
del test['single_category']
df4 = pd.concat([test_1,test],axis=0,ignore_index=True)
df5 = df4[['Role', 'salary', 'closing_date', 'job_code', 'short_job_link',
       'scrap_date', 'scraped_date', 'date_posted', 'Today_Date',
       'Days_to_close', 'active_inactive', 'Updated_on', 'latest_salary',
       'salary_change_date', 'job_summary', 'job_discription', 'employer_name',
       'employer_address', 'employer_post_code', 'employer_website',
       'contact_person_position', 'contact_person_name',
       'contact_person_email', 'contact_person_number', 'Major Specialisation', 'Specialties', 'Finally_Done']]
df5.rename(columns={'Finally_Done':'Categories'},inplace=True)
df5.drop(columns=['job_summary', 'date_posted','job_discription', 'employer_name',
       'employer_address', 'employer_post_code', 'employer_website',
       'contact_person_position', 'contact_person_name',
       'contact_person_email', 'contact_person_number'],inplace=True)
jd_latest['scraped_date'] = pd.to_datetime(jd_latest['scraped_date'])
jd_latest = jd_latest.sort_values(['scraped_date','short_job_link'],ascending=[True,True]).reset_index(drop=True)
jd_latest_2 = jd_latest.groupby(['scraped_date','short_job_link']).tail(1)
df6 = df5.merge(jd_latest_2[['job_summary', 'date_posted', 'job_discription', 'Main_duties_of_the_job', 'about_us','band',
                             'employer_name', 'qualification_essentials', 'qualification_desirable',
       'experience_essentials', 'experience_desirable',
       'additional_criteria_essentials', 'additional_criteria_desirable',
       'disclosure_and_barring_service_check',
       'employer_address', 'employer_post_code', 'employer_website',
       'contact_person_position', 'contact_person_name',
       'contact_person_email', 'contact_person_number','short_job_link']],on ='short_job_link',how='left').drop_duplicates('short_job_link',keep='last')
df6['date_posted'] = pd.to_datetime(
    df6['date_posted'], 
    infer_datetime_format=True, 
    errors='coerce')
df6.rename(columns={'scrap_date':'job_listing_page_scrape_date','scraped_date':'job_description_page_scrape_date',
                    'Updated_on':'job_description_page_latest_scrape_date','salary':'salary_range','Role':'role',
                    'closing_date':'job_closing_date','Today_Date':'scraped_file_creation_date',
                    'active_inactive':'job_active_inactive','latest_salary':'salary_range_updated',
                    'salary_change_date':'date_salary_range_updated','Major Specialisation':'major_specialisation',
                    'Specialties':'specialisation','Categories':'profession','date_posted':'job_posted_date',
                    'job_discription':'job_description','employer_name':'employer_or_trust_name',
                    'employer_address':'employer_or_trust_address','employer_post_code':'employer_or_trust_postcode',
                    'employer_website':'employer_or_trust_website','contact_person_position':'job_contact_person_designation',
                    'contact_person_name':'job_contact_person_name','contact_person_email':'job_contact_person_email',
                    'contact_person_number':'job_contact_person_phonenumber','Main_duties_of_the_job':'main_duties_of_the_job'},inplace=True)
del df6['Days_to_close']
df6['profession'] = df6['profession'].str.replace('Doctors','Doctor').str.replace('Health Care Assistant','Care Worker (HCA)')
df6['salary_range_'] = df6['salary_range'].apply(lambda x: salary_check(x))
df6[['salary_range_start','salary_range_end']] = pd.DataFrame(df6.salary_range_.tolist(), index= df6.index)
del df6['salary_range_']
listing['scrap_date'] = pd.to_datetime(listing['scrap_date'])
listing = listing.sort_values(['scrap_date','short_job_link'],ascending=[True,True]).reset_index(drop=True)
listing_1 = listing.drop_duplicates('short_job_link',keep='last').reset_index(drop=True)
df7 = df6.merge(listing_1[['short_job_link','Employment_Type','working_pattern']],on='short_job_link',how='left')
df7.rename(columns={"Employment_Type":"contract_type"},inplace=True)
df7['band_'] = df7['band'].apply(lambda x: band_grade(x))
df7[['band','grade']] = pd.DataFrame(df7.band_.tolist(), index= df7.index)
del df7['band_']
del df7['disclosure_and_barring_service_check']
del df7['salary_range_updated']
df7['salary_range_start'] = df7['salary_range_start'].replace('Depends on experience','-')
df7['salary_range_end'] = df7['salary_range_end'].replace('Depends on experience','-')
df7.rename(columns={'contract_type':'job_type'},inplace=True)
df7['working_pattern'] = df7['working_pattern'].str.strip()
df7['job_type'] = df7['job_type'].str.strip()
for i in df7.select_dtypes('object').columns.to_list():
    df7[i] = df7[i].astype(str)
    df7[i] = df7[i].str.strip()
df7 = df7.replace('¬','').replace('Äì','').replace('  â€“  ',' ')
df7['working_pattern'] = df7['working_pattern'].str.split(',').str[0]
df7['Area_Specialisation_Doctors'] = df7['profession'].apply(lambda x: 'Doctors' if x == 'Doctor' else '')
df7['Area_Specialisation_Nurse_CW'] = df7['profession'].apply(lambda x: 'Nurse CW' if x in ['Care Worker (HCA)', 'Nurse'] else '')
df7.loc[df7['Area_Specialisation_Doctors']!='','Area_Specialisation_Doctors'] = df7['major_specialisation']
df7.loc[df7['Area_Specialisation_Nurse_CW']!='','Area_Specialisation_Nurse_CW'] = df7['major_specialisation']
df7.to_csv(r"/home/ec2-user/scrape_data/master_data/Active_Jobs_with_categorisation.csv",index=False)
push_to_s3("master_data","Active_Jobs_with_categorisation")