from os import walk
from os.path import join, getsize, getmtime, getctime
import pandas as pd
import time

today = time.strftime('%Y%m%d')

root_path = join("data", "bnr_D002")

r = []
for dir_path, dirs, files in walk(root_path):
    for file in sorted(files):
        if file[-6:] == 'csv.gz':
            print(file)
            file_path = join(dir_path, file)
            df_file = pd.read_csv(file_path)
            r.extend(df_file.to_dict(orient='records'))
df = pd.DataFrame(r)
df.to_csv(join("data", "bnr_d002_20231216.csv.gz"), index=False)
