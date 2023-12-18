import pandas as pd

no_size = pd.read_csv('data/bnr_d001_20231210.csv.gz')
no_md5 = pd.read_csv('data/bnr_d001_TMP_20231218.csv.gz')
no_md5['path'] = no_md5['path'].str.replace("/home/kibini/bnr/", "")
md5_cmp = pd.read_csv("data/bnr_D001_00000001_20231218.csv.gz")
md5_cmpb = pd.read_csv("data/bnr_D001_00000002_20231218.csv.gz")
md5_cmp = pd.concat([md5_cmp, md5_cmpb])

new = no_md5

no_size = no_size[['name', 'path', 'md5']]
md5_cmp = md5_cmp[['name', 'path', 'md5']]
no_size2 = pd.concat([no_size, md5_cmp])

new2 = pd.merge(new, no_size2, on=['path', 'name'], how='inner')
new2 = new2.rename(columns={'creation_date':'last_content_modification_date', 'last_modification_date':'last_metadata_modification_date'})
new2[['name', 'path', 'md5', 'size', 'last_content_modification_date', 'last_metadata_modification_date']].to_csv("data/bnr_d001_20231218.csv.gz", index=False)
