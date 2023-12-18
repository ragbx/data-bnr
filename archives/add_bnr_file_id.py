import pandas as pd

"""
bnr_file_id, composé de :
- bnr_
- identifiant de source :
    - 001 = azrael au 2/12/2023
- numéro de ligne + 1 dans le dataframe de la source
"""

df = pd.read_csv("data/azrael_20231202.csv.gz")
df = df.sort_values(by=['path', 'name'])
df['path'] = df['path'].str.replace("/home/kibini/bnr/", "")
df = df.assign(bnr_file_id=range(1, len(df) + 1))
df['bnr_file_id'] = df['bnr_file_id'].apply(lambda x: f"bnr_001_{str(x).zfill(8)}")
df = df[['bnr_file_id', 'md5', 'name', 'path', 'size', 'creation_date',
       'last_modification_date']]
df = df.to_csv("data/azrael_20231202_v2.csv.gz", index=False)
