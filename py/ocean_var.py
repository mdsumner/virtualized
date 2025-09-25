from obstore.store import LocalStore
from concurrent.futures import ThreadPoolExecutor

from virtualizarr import open_virtual_dataset, open_virtual_mfdataset
from virtualizarr.parsers import HDFParser
from virtualizarr.registry import ObjectStoreRegistry
import time
import os
import sys
import glob

if  len(sys.argv) < 2:
   sys.exit("usage: python ocean_var.py varlabel, i.e. 'python ocean_var.py ocean_temp'")

varlabel = sys.argv[1]
store_path = "/g/data/gb6/BRAN/BRAN2023/daily"

ff = [f"file://{f}" for f in glob.glob(f'{store_path}/*{varlabel}*.nc')]
if len(ff) < 1:
   sys.exit("no files found")

outfile = f"virtualized/{varlabel}_2023.parq"

if  os.path.exists(outfile):
  sys.exit(f"outfile {outfile} already exists")

file_url = ff

## now stuff for VirtualiZarr 
store = LocalStore(prefix=store_path)
registry = ObjectStoreRegistry({"file:///": store})
parser = HDFParser()

loadvars = ["Time", "st_ocean", "xt_ocean", "yt_ocean"]
dropvars = ["Time_bnds", "average_DT", "average_T1", "average_T2", "nv", "st_edges_ocean"]
vds = open_virtual_mfdataset(
    file_url,
    parser=parser,
    registry=registry,
    combine="nested",
    concat_dim="Time",
    parallel = False,
    drop_variables = dropvars,
    loadable_variables = loadvars
  )

vds.vz.to_kerchunk(f"virtualized/{varlabel}_2023.parq", format = "parquet")
