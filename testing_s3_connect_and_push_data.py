#!/usr/bin/env python
# coding: utf-8

# In[1]:


import boto3
from botocore.exceptions import ClientError
import botocore
import s3fs as s3


# In[ ]:


s3 = boto3.resource(service_name = 's3', region_name = 'eu-west-2')


# In[ ]:


for bucket in s3.buckets.all():
    print(bucket.name)


# In[ ]:




