import pandas as pd
import spacy
import spacy.cli
# Load spaCy model
import spacy
from spacy.matcher import DependencyMatcher
from spacy.matcher import Matcher, PhraseMatcher
# Load the spaCy model
nlp = spacy.load("en_core_web_sm")
import numpy as np
from difflib import get_close_matches
import boto3
from botocore.exceptions import ClientError
import botocore
import s3fs as s3
from googleapiclient.http import MediaFileUpload
from google_drive_service import Create_Service
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.message import EmailMessage
import ssl
import os
from datetime import date
from datetime import datetime
from datetime import timedelta
import warnings
warnings.filterwarnings('ignore')

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
    #Only picking Active_Jobs
    filt_list = [item for item in filtered_list if item == 'Active_Jobs.csv']
    prefixed_list = [f'{x}/' + item for item in filt_list]
    return prefixed_list

def active_jobs_df(x):
    specific_files = active_job_data_list(x)
    active_jobs_df = pd.DataFrame()
    for file in specific_files:
        s3 = boto3.resource("s3")
        #load from bucket
        obj = s3.Bucket('nhs-dataset').Object(file).get()
        dd = pd.read_csv(obj['Body'])
        active_jobs_df = pd.concat([active_jobs_df,dd],axis=0,ignore_index=True)
    return active_jobs_df

def specialisation_list_df_list(x,y):
    df = pd.read_excel(r"/home/ec2-user/scrape_data/master_data/{}.xlsx".format(x),sheet_name=y)
    df_list = list(df[y].unique())
    return df_list

def specialisation_list_df(x,y):
    df = pd.read_excel(r"/home/ec2-user/scrape_data/master_data/{}.xlsx".format(x),sheet_name=y)
    return df

def push_to_s3(x,y):
    print(f'Pushing {y} to s3 bucket in {x}')
    s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')
    df = pd.read_csv(f"/home/ec2-user/scrape_data/{x}/{y}.csv")
    #push to bucket
    s3.Bucket('nhs-dataset').upload_file(Filename = f'/home/ec2-user/scrape_data/{x}/{y}.csv',Key = f'{x}/{y}.csv')
    print(f'{y} pushed to bucket in {x}')

#active_Jobs
active_jobs = active_jobs_df('master_data')
active_jobs['Role_'] = active_jobs['Role'].str.replace("-"," - ").str.replace("/"," / ").str.replace('*'," ").str.replace(","," , ").str.replace("'","").str.replace('–',' – ')

#other_keywords
administration_keywords = specialisation_list_df_list('Final_Speciality','Admin_Keywords')
healthcare_keywords = specialisation_list_df_list('Final_Speciality', 'HCA_Keywords')
engineer_keywords = specialisation_list_df_list('Final_Speciality', 'Engineer_Keywords')

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

def push_to_drive():
    CLIENT_SECRET_FILE = 'CLIENT_SECRET_FILE.json'
    API_NAME = 'drive'
    API_VERSION = 'v3'
    SCOPES = ['https://www.googleapis.com/auth/drive']
    service = Create_Service(CLIENT_SECRET_FILE,API_NAME,API_VERSION,SCOPES)
    folder_id = '11PyImlmzGi2FeMxhDZC8c5uA3LKxab13'
    file_tag = 'Active_Jobs_with_categorisation'
    file_name = f'{file_tag}_{str(date.today())}.xlsx'
    mine_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    # Upload a file
    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    media_content = MediaFileUpload(r"/home/ec2-user/scrape_data/master_data/{}_{}.xlsx".format(file_tag,str(date.today())), mimetype=mine_type)
    file = service.files().create(
        body=file_metadata,
        media_body=media_content
    ).execute()
    print(f"File pushed to Drive")
    file_id = file['id']
    share_file_link = "https://docs.google.com/spreadsheets/d/"+file_id+"/edit"
    return share_file_link

#Email with Just Text
def email_people_part_2(email_cred, x,y):
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
      <p>{x} pushed to S3 on {str(date.today())}. {y}</p>
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
    active_jobs_final_2.to_csv(r"/home/ec2-user/scrape_data/master_data/Active_Jobs_with_categorisation.csv",index=False)
    active_jobs_final_2.to_excel(r"/home/ec2-user/scrape_data/master_data/Active_Jobs_with_categorisation_{}.xlsx".format(str(date.today())),index=False)
    push_to_s3('master_data','Active_Jobs_with_categorisation')
    share_file_link = push_to_drive()
    text = f"Active Jobs with categorisation and all other Files"
    text2 = f"Here is the link to the file: {share_file_link}"
    email_people_part_2(email_cred,text,text2)
    delete_files("master_data","Active_Jobs_with_categorisation_{}.xlsx".format(str(date.today())))
    delete_files("master_data","Active_Jobs_with_categorisation.csv")
    return active_jobs_final_2

active_jobs_final_2 = final_checks(active_jobs,final_speciality,nurse_final_speciality,ahp_df)