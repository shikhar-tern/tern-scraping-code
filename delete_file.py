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

def del_files(dir,x):
    new_path = dir + '/' + x
    files_list = os.listdir(new_path)
    for j in files_list:
        os.remove(dir+"/"+x+"/"+j)

for x in new_list:
    del_files(dir,x)