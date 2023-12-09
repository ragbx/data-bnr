"""
On a besoin d'effctuer les tâches suivantes :

- sur un répertoire :
    - obtenir une liste de tous les fichiers présents
    - effectuer des traitements à partir de cette liste (calcul MD5, obtention de métadonnées)
voir Azrael2list

-
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime

from os import walk
from os.path import join, getsize, getmtime, getctime

import hashlib

import mimetypes

def get_md5hash(name):
    hash_md5 = hashlib.md5()
    with open(name, "rb") as f:
        while chunk := f.read(4096):
             hash_md5.update(chunk)
    return hash_md5.hexdigest()

def get_mimetype(filename):
    if filename:
        mimetype, _ = mimetypes.guess_type(filename)
        return mimetype

def get_extension_by_mimetype(mimetype):
    if mimetype:
        guessed_extension = mimetypes.guess_extension(mimetype)
        return guessed_extension

def convert_size(size, from_size='o', to_size='go'):
    if (from_size == 'o') & (to_size == 'ko'):
        n = 1024 ^ 1
        return round( size / n, 2)
    elif (from_size == 'o') & (to_size == 'mo'):
        n = 1024 ^ 2
        return round( size / n, 2)
    elif (from_size == 'o') & (to_size == 'go'):
        n = 1024 ^ 3
        return round( size / n, 2)
    elif (from_size == 'o') & (to_size == 'to'):
        n = 1024 ^ 4
        return round( size / n, 2)
        
def split_every_n_rows(dataframe, chunk_size=2):
    chunks = []
    num_chunks = len(dataframe) // chunk_size + 1
    for index in range(num_chunks):
        chunks.append(dataframe[index * chunk_size:(index+1) * chunk_size])
    return chunks
    
def int2string(n, leading_zeros=8):
    return str(n).zfill(leading_zeros)


class Azrael2list():
    def __init__(self, **kwargs):
        self.today = datetime.now().strftime('%Y%m%d')
        if 'code_disk' in kwargs:
            self.code_disk = kwargs.get('code_disk')
        if 'root_path' in kwargs:
            self.root_path = kwargs.get('root_path')
        
    def list_files(self, **kwargs):
        results = []
        for dir_path, dirs, files in walk(self.root_path):
            #print(dir_path)
            for file in files:
                file_path = join(dir_path, file)
                file_data = {}
                file_data["name"] = file
                file_data["path"] = dir_path
                file_data["size"] = getsize(file_path)
                file_data["creation_date"] = getctime(file_path)
                file_data["last_modification_date"] = getmtime(file_path)
                results.append(file_data)
        self.list_result = pd.DataFrame(results).sort_values(by=['path', 'name'])
    
    def export_list(self, chunksize=400):
        for i, df in enumerate(split_every_n_rows(self.list_result, chunk_size=chunksize)):
            i += 1
            file_number = int2string(i, leading_zeros=8)
            df.to_csv(join("data", "tmp", f"tmp_bnr_{self.code_disk}_{file_number}_{self.today}.csv.gz"), index=False)
            
    def process_lists(self, list_path = join("data", "tmp"), result_size=100):
        results = []
        i = 0
        j = 0
        for dir_path, dirs, files in walk(list_path):
            files = sorted(files)
            for file in files:
                print(file)
                df = pd.read_csv(join(dir_path, file))
                for file_data in df.to_dict(orient='records'):
                    i += 1
                    file_path = join(file_data['path'], file_data['name'])
                    file_data["md5"] = get_md5hash(file_path)
                    results.append(file_data)
                    k = i % result_size
                    if k == 0:
                        j += 1
                        file_number = int2string(j, leading_zeros=8)
                        results_df = pd.DataFrame(results)
                        results_df = results_df[['name', 'path', 'md5', 'creation_date', 'last_modification_date']]
                        results_df.to_csv(join("data", f"bnr_{self.code_disk}_{file_number}_{self.today}.csv.gz") , index=False)
                        
                    

class Azrael2analysis():
    def __init__(self):
        self.today = datetime.now().strftime('%Y%m%d')

    def create_az(self, **kwargs):
        if 'path_prefix' in kwargs:
            self.path_prefix = kwargs.get('path_prefix')
        if 'az' in kwargs:
            self.az = kwargs.get('az')
        elif 'path_az' in kwargs:
            path_az = kwargs.get('path_az')
            self.az = pd.read_csv(path_az)
        self.az = self.az.sort_values(by=['path', 'name'])
        if hasattr(self, 'path_prefix'):
            self.az['path'] = self.az['path'].str.replace(self.path_prefix, "")

    def add_bnr_file_id(self, disk):
        self.az = self.az.assign(bnr_file_id=range(1, len(self.az) + 1))
        self.az['bnr_file_id'] = self.az['bnr_file_id'].apply(lambda x: f"bnr_{disk}_{str(x).zfill(8)}")

    def split_path(self, n=4):
        cols = [f"path{n}" for n in range(n+1)]
        self.az[cols] = self.az['path'].str.split('/', n=n, expand=True)

    def export_az(self, columns, filename, format):
        if columns:
            self.az2export = self.az[columns]
        if format == 'xlsx':
            self.az.to_excel(filename, index='False')
        else:
            self.az.to_csv(filename, index='False')

if __name__ == "__main__":
    pass
    # data_folder = 'data'
    # azrael = Azrael2analysis()
    # azrael.create_az(path_az=join(data_folder, "azrael_20231202_v2_short.csv.gz"))
    # azrael.split_path(n=4)
    # azrael.az['size'].apply(lambda x : convert_size(x, from_size='o', to_size='mo'))
