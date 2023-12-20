from bnr.azrael import Azrael2list

"""
Prestation CADN 2020
DDE_2020_I
MED_MS
5992 copiés dans BNR_VERIF le 05/01/2021
"""

root_path = 'E:/IMAGES/'
az2list = Azrael2list(root_path=root_path, code_disk='d002')

az2list.list_files()

az2list.export_list(chunksize=1000)

az2list.process_lists(result_size=1000, exif=True)
