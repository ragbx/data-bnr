from os.path import join
from bnr.azrael import Azrael2analysis

data_folder = "data"
#az2a = Azrael2analysis(path_az=join(data_folder, "bnr_d001_20231210.csv.gz"))
az2a = Azrael2analysis()
az2a.create_az(path_az=join(data_folder, "bnr_d001_20231210.csv.gz"))
#az2a.split_path()
az2a.dates2dt()
#print(az2a.az[az2a.az['creation_date_dt'].dt.year > 2022])
# tmp = az2a.az[az2a.az['creation_date_dt'] > datetime(2023, 12, 1)]
