import pandas as pd
import numpy as np
import re
from os.path import join

data_folder = 'data'
az = pd.read_csv(join(data_folder, "azrael_20231202_v2.csv.gz"))

duplicates = az[az.duplicated(subset='md5', keep=False)]
duplicates = duplicates.sort_values(by='md5')
duplicates['to_del'] = False
duplicates['commentaire'] = " "

grouped = duplicates.groupby('md5')
gr = [g for g in grouped.groups]
for g in gr:
    d = grouped.get_group(g)
    if len(d) == 2:
        d = d.sort_values(by='path')
        records = [r for r in d.to_dict(orient='records')]
        rec_index= [r for r in d.to_dict(orient='index')]
        if ((records[0]['path'][:8] == 'BNR_SAUV') & (records[1]['path'][:8] != 'BNR_SAUV')):
            duplicates.at[rec_index[1], 'to_del'] = True
            duplicates.at[rec_index[0], 'commentaire'] = f"{len(d)} doublons, 1er dans SAUV"
            duplicates.at[rec_index[1], 'commentaire'] = f"{len(d)} doublons, 1er dans SAUV"
    elif len(d) == 3:
        d = d.sort_values(by='path')
        records = [r for r in d.to_dict(orient='records')]
        rec_index= [r for r in d.to_dict(orient='index')]
        if ((records[0]['path'][:8] == 'BNR_SAUV') & (records[1]['path'][:8] != 'BNR_SAUV') & (records[2]['path'][:8] != 'BNR_SAUV')):
            duplicates.at[rec_index[1], 'to_del'] = True
            duplicates.at[rec_index[2], 'to_del'] = True
            duplicates.at[rec_index[0], 'commentaire'] = f"{len(d)} doublons, 1er dans SAUV"
            duplicates.at[rec_index[1], 'commentaire'] = f"{len(d)} doublons, 1er dans SAUV"
            duplicates.at[rec_index[2], 'commentaire'] = f"{len(d)} doublons, 1er dans SAUV"
    elif len(d) == 4:
        d = d.sort_values(by='path')
        records = [r for r in d.to_dict(orient='records')]
        rec_index= [r for r in d.to_dict(orient='index')]
        if ((records[0]['path'][:8] == 'BNR_SAUV') & (records[1]['path'][:8] != 'BNR_SAUV') & (records[2]['path'][:8] != 'BNR_SAUV') & (records[3]['path'][:8] != 'BNR_SAUV')):
            duplicates.at[rec_index[1], 'to_del'] = True
            duplicates.at[rec_index[2], 'to_del'] = True
            duplicates.at[rec_index[3], 'to_del'] = True
            duplicates.at[rec_index[0], 'commentaire'] = f"{len(d)} doublons, 1er dans SAUV"
            duplicates.at[rec_index[1], 'commentaire'] = f"{len(d)} doublons, 1er dans SAUV"
            duplicates.at[rec_index[2], 'commentaire'] = f"{len(d)} doublons, 1er dans SAUV"
            duplicates.at[rec_index[3], 'commentaire'] = f"{len(d)} doublons, 1er dans SAUV"

duplicates.loc[duplicates['md5'] == '58c89562f58fd276f592420068db8c09', 'commentaire'] = "OCR vide, à conserver ?"
duplicates.loc[duplicates['md5'] == 'cb29e6b2c2af084e075ba515f9643df0', 'commentaire'] = "fichier caché"
duplicates.loc[duplicates['md5'] == 'cb29e6b2c2af084e075ba515f9643df0', 'to_del'] = True
duplicates.loc[duplicates['md5'] == '0b0d40bea8482df673b8c25851e8b0d3', 'commentaire'] = "OCR vide, à conserver ?"
duplicates.loc[duplicates['md5'] == 'db7269cefe21d34ccd0ebbb48e7b2515', 'commentaire'] = "fichier caché"
duplicates.loc[duplicates['md5'] == 'db7269cefe21d34ccd0ebbb48e7b2515', 'to_del'] = True

cols = ['bnr_file_id', 'md5', 'name', 'path', 'size', 'to_del', 'commentaire']
duplicates["commentaire"] = duplicates["commentaire"].replace(r'^\s$', np.nan, regex=True)
duplicates_auto = duplicates[~duplicates['commentaire'].isna()]
duplicates_mano = duplicates[duplicates['commentaire'].isna()]
duplicates_auto.to_excel("results/doublons_azrael_20231207_auto.xlsx", index=False)
duplicates_mano.to_excel("results/doublons_azrael_20231207_mano.xlsx", index=False)
