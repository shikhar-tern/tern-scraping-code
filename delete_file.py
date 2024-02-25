import os
dir = '/home/ec2-user/scrape_data'
files = os.listdir(dir)
new_list = []
for i in files:
    if i == 'log.txt':
        pass
    else:
        new_list.append(i)

def del_files(dir,x):
    new_path = dir + '/' + x
    files_list = os.listdir(new_path)
    for j in files_list:
        if j in ['Listing_Page_Master.csv','Active_Jobs_with_categorisation.xlsx','Jobs_Information_Master.csv','Final_Speciality.xlsx']:
            pass
        else:
            # print(dir+"/"+x+"/"+j)
            os.remove(dir+"/"+x+"/"+j)

for x in new_list:
    del_files(dir,x)