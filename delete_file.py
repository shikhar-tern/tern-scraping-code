import os
files = os.listdir('/home/ec2-user/scrape_data')
new_list = []
for i in files:
    if i == 'master_data':
        pass
    elif i == 'log.txt':
        pass
    else:
        new_list.append(i)

print(new_list)