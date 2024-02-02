import os
files = [x for x in os.listdir() if os.path.isdir(x)]
new_list = [item for item in files if item != 'master_data']
print(new_list)