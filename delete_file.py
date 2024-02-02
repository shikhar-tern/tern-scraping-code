import os
dir = '/home/ec2-user/scrape_data'
files = os.listdir(dir)
new_list = []
for i in files:
    if i == 'master_data':
        pass
    elif i == 'log.txt':
        pass
    else:
        new_list.append(i)

print(new_list)