from os import walk
from os.path import join, getsize, getmtime, getctime
import pandas as pd
import time

today = time.strftime('%Y%m%d')

root_path = "data/bnr_D001"

r = []
i = 0
for dir_path, dirs, files in walk(root_path):
    for file in files:
        i +=  1
        print(i)
        file_path = join(dir_path, file)
        df_file = pd.read_csv(file_path)
        r.extend(df_file.to_dict(orient='records'))
df = pd.DataFrame(r)
df.to_csv("data/bnr_d001_20231210.csv.gz", index=False)
