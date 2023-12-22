from os import walk
from os.path import join, getsize, getmtime, getctime
import pandas as pd
import time

today = time.strftime('%Y%m%d')

root_path = join("data")

r = []
for dir_path, dirs, files in walk(root_path):
    for file in sorted(files):
        if file[-6:] == 'csv.gz':
            if file[4:8] == 'd008':
                print(file)
                file_path = join(dir_path, file)
                df_file = pd.read_csv(file_path)
                r.extend(df_file.to_dict(orient='records'))
df = pd.DataFrame(r)
df.to_csv(join("data", "bnr_d008_20231221.csv.gz"), index=False)
