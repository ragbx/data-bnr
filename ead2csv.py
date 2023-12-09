import pandas as pd
from os import walk
from os.path import join
import codecs
import json

from bnr.rbxeadparser import RbxEadParser

data_folder = 'data'
inventaires_folder = 'ead_inventaires'
ead_data_folder = join(data_folder, inventaires_folder)
res_folder = 'results'

all_records = []
headers = []
for dir_path, dirs, files in walk(ead_data_folder):
    for ead_file in files:
        ead_file_path = join(ead_data_folder, ead_file)

        eadparser = RbxEadParser(ead_file=ead_file_path)
        eadparser.parser()

        res_file = f"{ead_file[:-4]}.xlsx"
        res_file_path = join(res_folder, res_file)

        #columns = ['eadheader_eadid', 'eadheader_titleproper', 'eadheader_publication_date', 'archdesc_did_langmaterial_language', 'archdesc_did_physdesc', 'archdesc_did_repository', 'archdesc_did_unidate_normal', 'archdesc_did_unitdate', 'archdesc_did_unitid', 'archdesc_did_unittitle', 'archdesc_controlaccess_corpname', 'archdesc_controlaccess_genreform', 'archdesc_controlaccess_geogname', 'archdesc_controlaccess_persname', 'archdesc_controlaccess_subject_chrono', 'archdesc_controlaccess_subject_Rameau', 'archdesc_controlaccess_subject_theme', 'archdesc_acqinfo', 'archdesc_note', 'archdesc_scopecontent', 'subfonds_controlaccess_subject_Rameau', 'subfonds_controlaccess_subject_theme', 'subfonds_dao_file_first', 'subfonds_did_physdesc', 'subfonds_did_repository', 'subfonds_did_unidate_normal', 'subfonds_did_unitdate', 'subfonds_did_unitid', 'subfonds_did_unittitle', 'series_controlaccess_corpname', 'series_controlaccess_genreform', 'series_controlaccess_geogname', 'series_controlaccess_geogname_coord', 'series_controlaccess_name', 'series_controlaccess_persname', 'series_controlaccess_subject', 'series_controlaccess_subject_chrono', 'series_controlaccess_subject_Rameau', 'series_controlaccess_subject_theme', 'series_did_physdesc', 'series_did_repository', 'series_did_unidate_normal', 'series_did_unitdate', 'series_did_unitid', 'series_did_unittitle', 'subseries_controlaccess_corpname', 'subseries_controlaccess_genreform', 'subseries_controlaccess_geogname', 'subseries_controlaccess_geogname_coord', 'subseries_controlaccess_name', 'subseries_controlaccess_persname', 'subseries_controlaccess_subject', 'subseries_controlaccess_subject_chrono', 'subseries_controlaccess_subject_Rameau', 'subseries_controlaccess_subject_theme', 'subseries_dao_file_first', 'subseries_dao_file_last', 'subseries_did_physdesc', 'subseries_did_repository', 'subseries_did_unidate_normal', 'subseries_did_unitdate', 'subseries_did_unitid', 'subseries_did_unittitle', 'file_controlaccess_corpname', 'file_controlaccess_genreform', 'file_controlaccess_geogname', 'file_controlaccess_geogname_coord', 'file_controlaccess_name', 'file_controlaccess_persname', 'file_controlaccess_subject', 'file_controlaccess_subject_chrono', 'file_controlaccess_subject_Rameau', 'file_controlaccess_subject_theme', 'file_dao_audience', 'file_dao_file_first', 'file_dao_file_last', 'file_did_physdesc', 'file_did_repository', 'file_did_unidate_normal', 'file_did_unitdate', 'file_did_unitid', 'file_did_unittitle', 'subfile_controlaccess_corpname', 'subfile_controlaccess_genreform', 'subfile_controlaccess_geogname', 'subfile_controlaccess_name', 'subfile_controlaccess_persname', 'subfile_controlaccess_subject_chrono', 'subfile_controlaccess_subject_Rameau', 'subfile_controlaccess_subject_theme', 'subfile_did_physdesc', 'subfile_did_unidate_normal', 'subfile_did_unitdate', 'subfile_did_unitid', 'subfile_did_unittitle', 'item_controlaccess_corpname', 'item_controlaccess_genreform', 'item_controlaccess_geogname', 'item_controlaccess_geogname_coord', 'item_controlaccess_name', 'item_controlaccess_persname', 'item_controlaccess_subject', 'item_controlaccess_subject_chrono', 'item_controlaccess_subject_Rameau', 'item_controlaccess_subject_theme', 'item_dao_audience', 'item_dao_file_first', 'item_dao_file_last', 'item_did_physdesc', 'item_did_repository', 'item_did_unidate_normal', 'item_did_unitdate', 'item_did_unitid', 'item_did_unittitle']

        columns = ['eadheader_eadid', 'eadheader_titleproper', 'archdesc_did_repository', 'archdesc_did_unitid',
        'archdesc_did_unittitle', 'subfonds_dao_file_first', 'subfonds_did_repository', 'subfonds_did_unitid',
        'subfonds_did_unittitle', 'series_did_repository',         'series_did_unitid', 'series_did_unittitle',
        'subseries_dao_file_first', 'subseries_dao_file_last', 'subseries_did_repository', 'subseries_did_unitid',
        'subseries_did_unittitle', 'file_dao_audience', 'file_dao_file_first', 'file_dao_file_last',
        'file_did_repository', 'file_did_unitid', 'file_did_unittitle', 'subfile_did_unitid',
        'subfile_did_unittitle', 'item_dao_audience', 'item_dao_file_first', 'item_dao_file_last',
        'item_did_repository', 'item_did_unitid', 'item_did_unittitle']

        result = pd.DataFrame(eadparser.flat_metadata, columns=columns)
        all_records.append(result)
        header = eadparser.metadata_eadheader
        header['archdesc_did_unitid'] = eadparser.metadata_archdesc['did_unitid']
        header['nb_notices'] = len(result)
        headers.append(header)
        print(f"{ead_file[:-4]},{len(result)}")
        #result.to_excel(res_file_path, index=False)


all_df = pd.concat(all_records)
all_df.to_excel(join(res_folder, "ead_inventaires_notices.xlsx"), index=False)

inventaires = pd.DataFrame(headers)
inventaires.to_excel(join(res_folder, "ead_inventaires_liste.xlsx"), index=False)
