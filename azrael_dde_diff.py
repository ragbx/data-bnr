from os.path import join, isfile
from os import listdir
import pandas as pd
root_path = "data"

# on créée deux dataframes à comparer

azrael_prefix = "d001"
dde_prefix = [f"d{n :03d}" for n in range(2, 27)]

files = [f for f in listdir(root_path) if f[-6:] == 'csv.gz']
dde_df = pd.DataFrame()
for file in files:
    if file[4:8] == azrael_prefix:
        azrael_df = pd.read_csv(join(root_path, file))
        azrael_df['prefix'] = file[4:8]
    elif file[4:8] in dde_prefix:
        dde_df_part = pd.read_csv(join(root_path, file))
        dde_df_part['prefix'] = file[4:8]
        dde_df = pd.concat([dde_df, dde_df_part])

# fichiers pour lesquels on a les md5
dde_df_md5ok = dde_df[~dde_df['md5'].isna()]
dde_df_md5ok_notinazrael = dde_df_md5ok[~dde_df_md5ok['md5'].isin(azrael_df['md5'])]
dde_df_md5ok_notinazrael.to_csv(join(root_path, "dde_df_md5ok_notinazrael.csv.gz"), index=False)
print(len(dde_df_md5ok_notinazrael))

# fichiers pour lesquels on n'a pas les md5
dde_df_md5ko = dde_df[dde_df['md5'].isna()]
dde_df_md5ko_notinazrael = dde_df_md5ko[~dde_df_md5ko['name'].isin(azrael_df['name'])]
dde_df_md5ko_notinazrael.to_csv(join(root_path, "dde_df_md5ko_notinazrael.csv.gz"), index=False)
print(len(dde_df_md5ko_notinazrael))
