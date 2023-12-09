import pandas as pd
from os import walk
from os.path import join

"""
Ne fonctionne pas
"""



dir = "results/oai_json"

df_sets = pd.DataFrame()
for dir_path, dirs, files in walk(dir):
    for file in files:
        filename = join(dir_path, file)
        df = pd.read_json(filename)
        df_sets = pd.concat([df, df_sets], ignore_index=True)
cols = ['source', 'date', 'rights', 'language',
       'identifier', 'type', 'title',
       'bnr_set', 'subject', 'description', 'format', 'creator',
       'relation', 'coverage', 'publisher', 'contributor']
df_sets = df_sets[cols]
to_excel = df_sets
for col in cols:
    to_excel[col] = to_excel[col].str.join(", ")
print(to_excel['bnr_set'].value_counts())
df_sets['nb'] = 1

sets_desc = pd.read_csv("results/oai_sets_bnr_description.xlsx")
sets_desc = sets_desc[['setSpec', 'setName']]
records_by_set = df_sets.groupby(['bnr_set'])['nb'].sum()
sets = sets_desc.merge(records_by_set, how='right', left_on='setSpec', right_on='bnr_set')
sets.to_excel("results/oai_notices.xlsx", index=False)
