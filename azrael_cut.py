import pandas as pd
import re
from os.path import join
import mimetypes

def get_mimetype(filename):
    if filename:
        mimetype, _ = mimetypes.guess_type(filename)
        return mimetype

def get_extension_by_mimetype(mimetype):
    if mimetype:
        guessed_extension = mimetypes.guess_extension(mimetype)
        return guessed_extension

az = pd.read_csv("data/azrael_20231202.csv_with_id.gz")
az['path'] = az['path'].str.replace('/home/kibini/bnr/', '')
az = az.sort_values(by=['path', 'name'])
az = az[az['name'] != 'Thumbs.db']

az['mimetype'] = az['name'].apply(get_mimetype)
az['guessed_extension'] = az['mimetype'].apply(get_extension_by_mimetype)
az['extension'] = az['name'].str.extract(r'^.*(\.\w+)$')

az['folder'] = az['path'].str.extract(r'(BNR_\w*)\/.*')
az['repository'] = az['path'].str.extract(r'BNR_\w+\/([^\/]+)\/.*')
az['collection'] = az['path'].str.extract(r'BNR_\w+\/[^\/]+\/([^\/]+)\/.*')
az['collection1'] = az['path'].str.extract(r'BNR_\w+\/[^\/]+\/[^\/]+\/([^\/]+)\/.*')
az['collection2'] = az['path'].str.extract(r'BNR_\w+\/[^\/]+\/[^\/]+\/[^\/]+\/([^\/]+)\/.*')

az['prefix'] = az['collection']
az['prefix'] = az['prefix'].str.replace("\s", "_", regex=True)
az.loc[az['prefix'] == 'MED_PRA', 'prefix'] =  az['collection'] + "__" + az['collection1'] + "__" + az['collection2']
az.loc[az['prefix'] == 'AMR_EC', 'prefix'] =  az['collection'] + "__" + az['collection1']

az['size_mo'] = az['size'].apply(lambda x: round(x / 1024 / 1024, 3))

cls = ['prefix', 'name', 'path', 'md5', 'size_mo', 'mimetype', 'guessed_extension', 'extension']
az.to_csv(join('results', 'azrael', f"azrael_all_20231202.csv.gz"), index=False)
cls = ['name', 'path', 'md5', 'size_mo', 'mimetype', 'guessed_extension', 'extension']
for prefix in az['prefix'].unique():
    print(prefix)
    df = az[az['prefix'] == prefix]
    df = df[cls]
    df.to_excel(join('results', 'azrael', f"azrael_{prefix}_20231202.xlsx"), index=False)
