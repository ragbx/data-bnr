from os.path import join
import bnr.azrael as azrael

import pandas as pd
import numpy as np

root_path = "data"
file_name = join(root_path, "bnr_dXXX_20231230.csv.gz")
#file_name = join(root_path, "bnr_d001_20231218.csv.gz")
#file_name = join(root_path, "dde_notinazrael.csv.gz")

az2a = azrael.Azrael2analysis()
az2a.create_az(path_az=file_name)

az2a.az['path'] = az2a.az['path'].str.replace("\\", "/")
az2a.split_path()
az2a.get_extension_mimetype()
df = az2a.az
#
#df['prefix'] = 'd001'
df['nb'] = 1

#table = pd.pivot_table(df, values=['nb', 'size_go'], index=['prefix'],
#                       columns=['file_type'], aggfunc='sum', fill_value=0)

#table.to_excel("results/dXX_notinazrael_analyse.xlsx")

df['size_mo'] = 

prefixes = df['prefix'].unique()
for prefix in np.sort(prefixes):
    print(prefix)
    df_by_prefix = df[df['prefix'] == prefix ]
    for file_type in np.sort(df_by_prefix['file_type'].unique()):
        print(f"----{file_type}")
        df_by_prefix_type = df_by_prefix[df_by_prefix['file_type'] == file_type ]
        file_type = file_type.replace(" ", "")
        if len(df_by_prefix_type) > 0:
            df_by_prefix_type = df_by_prefix_type[['prefix', 'name', 'path', 'md5', 'size_mo', 'extension', 'mimetype', 'file_type', 'path1', 'path2', 'path3', 'path4']]
            df_by_prefix_type.to_excel(f"results/dde_notinazrael_{prefix}_{file_type}.xlsx", index=False)
