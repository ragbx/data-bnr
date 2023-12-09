import pandas as pd
import numpy as np
import re
from os.path import join

def get_dao_filenames(dao_file_name1, dao_file_name2=None):
    filenames = []

    pat = r"^(.*)_(\d*)(\..*)$"

    if dao_file_name1:
        if isinstance(dao_file_name1, str):
            s = re.search(pat, dao_file_name1)
            if s:
                extension = s.group(3)
                l = len(s.group(2))
                file_min = int(s.group(2))

                if dao_file_name2:
                    if isinstance(dao_file_name2, str):
                        s2 = re.search(pat, dao_file_name2)
                        if s2:
                            file_max = int(s2.group(2))

                            for i in range(file_min, file_max + 1):
                                n = str(i).zfill(l)
                                filenames.append(f"{s.group(1)}_{n}{extension}")
                else:
                    i = file_min
                    n = str(i).zfill(l)
                    filenames.append(f"{s.group(1)}_{n}{extension}")
    return filenames

results_path = "results"
records = pd.read_excel(join(results_path, "ead_inventaires_notices.xlsx"))
filename_with_records = []

for level in ['subfonds', 'series', 'subseries', 'file', 'subfile', 'item']:
    c = f"{level}_dao_file_first"
    c_first = f"{level}_dao_file_first"
    c_last = f"{level}_dao_file_last"
    if c in records:
        records_dao = records[~records[c].isna()]
        for record in records_dao.to_dict(orient='records'):
            if c_first in record:
                if c_last in record:
                    filenames = get_dao_filenames(record[c_first], record[c_last])
                else:
                    filenames = get_dao_filenames(record[c_first])
                for filename in filenames:
                    record['filename'] = filename
                    filename_with_records.append(record.copy())

columns = ['filename', 'eadheader_eadid', 'eadheader_titleproper', 'archdesc_did_repository', 'archdesc_did_unitid',
 'archdesc_did_unittitle', 'subfonds_did_repository',  'subfonds_did_unitid', 'subfonds_did_unittitle',
       'series_did_repository', 'series_did_unitid', 'series_did_unittitle',
       'subseries_did_repository', 'subseries_did_unitid',
       'subseries_did_unittitle', 'file_dao_audience', 'file_did_repository', 'file_did_unitid',
       'file_did_unittitle', 'subfile_did_unitid', 'subfile_did_unittitle',
       'item_dao_audience',
       'item_did_repository', 'item_did_unitid', 'item_did_unittitle']
filename_with_records_df = pd.DataFrame(filename_with_records, columns = columns)
filename_with_records_df.dropna(how='all', axis=1, inplace=True)
filename_with_records_df = filename_with_records_df.drop_duplicates()
filename_with_records_df.to_excel(join(results_path, "ead_files_from_dao.xlsx"), index=False)
