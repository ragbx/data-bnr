from bnr.azrael import Azrael2list

root_path = '/home/kibini/bnr/BNR_TAMPON/AMR/'
az2list = Azrael2list(root_path=root_path, code_disk='D001')
az2list.list_files()
az2list.export_list()

az2list.process_lists()