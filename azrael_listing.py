from os import walk
from os.path import join, getsize, getmtime, getctime
import pandas as pd
import time
 
today = time.strftime('%Y%m%d') 

root_path = f"/home/kibini/bnr"

results = []

for dir_path, dirs, files in walk(root_path):
    print(dir_path)
    for file in files:
        file_path = join(dir_path, file)
        file_data = {}
        file_data["name"] = file
        file_data["path"] = dir_path
        file_data["size"] = getsize(file_path)
        file_data["creation_date"] = getctime(file_path)
        file_data["last_modification_date"] = getmtime(file_path)
        results.append(file_data)

df_results = pd.DataFrame(results)
df_results = df_results[df_results['path'].str[17:22].isin(['BNR_S', 'BNR_T', 'BNR_V'])]
df_results = df_results.sort_values(by='path')
df_results.to_csv(f"/home/kibini/bnr_sauvegarde_explorer/{today}_liste_fichiers.csv", index=False)
