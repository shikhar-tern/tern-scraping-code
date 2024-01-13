import pandas as pd
import spacy
import spacy.cli
# Load spaCy model
nlp = spacy.load("en_core_web_sm")
from spacy.matcher import PhraseMatcher
from spacy.matcher import DependencyMatcher
import boto3
from botocore.exceptions import ClientError
import botocore
import numpy as np
import warnings
warnings.filterwarnings('ignore')

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


active_jobs = fetching_df('master_data','Active_Jobs')
jd_latest = fetching_df('master_data','Jobs_Information_Master')
listing = fetching_df('master_data','Listing_Page_Master')
final_speciality = fetching_df('master_data','Final_Speciality')
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
df7 = df7.replace('¬','').replace('Äì','').replace('  â€“  ',' ').replace('ÔøΩ','').replace('â€“','')
df7['working_pattern'] = df7['working_pattern'].str.split(',').str[0]
df7['Area_Specialisation_Doctors'] = df7['profession'].apply(lambda x: 'Doctors' if x == 'Doctor' else '')
df7['Area_Specialisation_Nurse_CW'] = df7['profession'].apply(lambda x: 'Nurse CW' if x in ['Care Worker (HCA)', 'Nurse'] else '')
df7.loc[df7['Area_Specialisation_Doctors']!='','Area_Specialisation_Doctors'] = df7['major_specialisation']
df7.loc[df7['Area_Specialisation_Nurse_CW']!='','Area_Specialisation_Nurse_CW'] = df7['major_specialisation']
df7.to_csv(r"/home/ec2-user/scrape_data/master_data/Active_Jobs_with_categorisation.csv",index=False)
push_to_s3("master_data","Active_Jobs_with_categorisation")