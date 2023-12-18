from os import walk
from os.path import join, getsize, getmtime, getctime
import pandas as pd
import hashlib
from datetime import datetime

today = datetime.now().strftime('%Y%m%d') 

def get_md5hash(filename):
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        while chunk := f.read(4096):
             hash_md5.update(chunk)
    return hash_md5.hexdigest()

files = "results/20231125_liste_fichiers.csv"
files_df = pd.read_csv(files)

pathes = files_df['path'].unique()

# test
pathes = pathes[5348:]

for path in pathes:
    suffix = path.replace("/home/kibini/bnr/", "")
    print(suffix)
    suffix = suffix.replace("/", "__")
    suffix = suffix.lower()
    
    files_chunk = files_df[files_df['path'] == path]
    s = len(files_chunk)
    
    i = 0
    results = []
    
    for file in files_chunk.to_dict(orient='records'):
        if i % 100 == 0:
            timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            print(f"{timestamp}    {i} / {s}")
        i += 1
        file_path = f"{file['path']}/{file['name']}"
    
        file_data = {}
        file_data["name"] = file['name']
        file_data["path"] = file['path']
        file_data["size"] = getsize(file_path)
        file_data["md5"] = get_md5hash(file_path)
        file_data["creation_date"] = getctime(file_path)
        file_data["last_modification_date"] = getmtime(file_path)
        results.append(file_data)
        
    df_results = pd.DataFrame(results)
    df_results.to_csv(f"results/{today}_{suffix}_fichiers.csv", index=False)
    
    print("--------------")
    print()
