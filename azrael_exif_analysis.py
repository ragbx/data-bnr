import pandas as pd
from os import listdir
from os.path import join
import json

tags = ['file_path']
folder = "data/exif"
json_files = listdir(folder)
#json_files = ['20231206_bnr_exif_01000.json']
for json_file in json_files:
    f = open(join(folder, json_file), "r")
    images = json.load(f)
    for metadata in images:
        for k in metadata['exif']:
            tags.append(k)
list(set(tags))
for tag in tags:
    print(tags)
