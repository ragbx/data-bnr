from bnr.azrael import Azrael2list

"""
DDE_2008I
"""

root_path = 'E:/'
az2list = Azrael2list(root_path=root_path, code_disk='d003')

#az2list.list_files()

#az2list.export_list(chunksize=1000)

az2list.process_lists(result_size=1000, exif=True)
