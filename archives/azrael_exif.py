from os import walk
from os.path import join, getsize, getmtime, getctime
import pandas as pd
from datetime import datetime
import exifread
import json
import gzip
import mimetypes

today = datetime.now().strftime('%Y%m%d')
 
root_path = f"/home/kibini/bnr"

results = []

i = 0
j = 0
for dir_path, dirs, files in walk(root_path):
    for file in files:
        mimetype, _ = mimetypes.guess_type(file)
        if mimetype:
            if mimetype[:5] == "image":
                i += 1
                print(i)
                file_path = join(dir_path, file)
                f = open(file_path, 'rb')
                tags = exifread.process_file(f, details=False)
                tags2json = {}
                for k, v in tags.items():
                    tags2json[k] = v.printable
                file_path = file_path.replace("/home/kibini/bnr/", "")
                results.append({'file_path': file_path, 'exif': tags2json})
                if len(results) == 1000:
                    j += 1
                    j_str = str(j).zfill(5)
                    json_str = json.dumps(results)
                    json_bytes = json_str.encode('utf-8')
                    with gzip.open(f"results/{today}_bnr_exif_{j_str}.json.gz", 'w') as fout:
                        fout.write(json_bytes)
                    results = []
                





# for file in files_df.to_dict(orient='records'):
    # file_path = f"{file['path']}/{file['name']}"
    # print(file_path)
    
    # f = open(file_path, 'rb')
    # tags = exifread.process_file(f, details=False)
    # tags2json = {}
    # for k, v in tags.items():
        # tags2json[k] = v.printable
        
    # file_path = file_path.replace("/home/kibini/bnr/", "")
    # results.append({'file_path': file_path, 'md5': file['md5'], 'exif': tags2json})
# json_str = json.dumps(results)
# json_bytes = json_str.encode('utf-8')            

# with gzip.open(f"results/{today}_bnr_sauv__amr__amr_aff_exif.json.gz", 'w') as fout:
    # fout.write(json_bytes)       
