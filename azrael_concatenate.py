from os import walk
from os.path import join, getsize, getmtime, getctime
import pandas as pd
import time
 
today = time.strftime('%Y%m%d') 

root_path = "/home/kibini/bnr_sauvegarde_explorer/results/azrael_20231202/"

r = []
df = pd.DataFrame()

i = 0
for dir_path, dirs, files in walk(root_path):
    for file in files:
        i +  1
        print(i)
        file_path = join(dir_path, file)
        df_file = pd.read_csv(file_path)
        #df = pd.concat([df, df_file])
        r.extend(df_file.to_dict(orient='records'))

df.to_csv(f"/home/kibini/bnr_sauvegarde_explorer/results/all.csv", index=False)
