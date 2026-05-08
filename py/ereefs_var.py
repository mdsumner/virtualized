from argparse import ArgumentParser
from pathlib import Path

from obspec_utils.registry import ObjectStoreRegistry
from obstore.store import HTTPStore
from virtualizarr import open_virtual_mfdataset
from virtualizarr.parsers import HDFParser


DEFAULT_PREFIX = "https://thredds.nci.org.au/"
DEFAULT_LOADABLE_VARS = ["time", "zc", "latitude", "longitude"]
DEFAULT_DROP_VARS = ["botz"]


parser = ArgumentParser(
    description=(
        "Build a parquet-backed kerchunk store for one eReefs/GBR4 variable "
        "from public NCI THREDDS files."
    )
)
parser.add_argument("varlabel", help="Variable to keep, e.g. temp or salt")
parser.add_argument("outfile", help="Output parquet directory")
parser.add_argument("urls", nargs="+", help="One or more public THREDDS fileServer URLs")
parser.add_argument(
    "--source-prefix",
    default=DEFAULT_PREFIX,
    help=f"HTTP prefix to register with obstore (default: {DEFAULT_PREFIX})",
)
parser.add_argument(
    "--loadable-vars",
    nargs="*",
    default=DEFAULT_LOADABLE_VARS,
    help="Coordinate variables to materialize into the virtual dataset",
)
parser.add_argument(
    "--drop-vars",
    nargs="*",
    default=DEFAULT_DROP_VARS,
    help="Variables to drop before writing the kerchunk parquet store",
)
args = parser.parse_args()

outfile = Path(args.outfile)
if outfile.exists():
    raise SystemExit(f"outfile already exists: {outfile}")

registry = ObjectStoreRegistry({args.source_prefix: HTTPStore(args.source_prefix)})
parser = HDFParser()

vds = open_virtual_mfdataset(
    args.urls,
    registry=registry,
    parser=parser,
    combine="nested",
    concat_dim="time",
    parallel=False,
    drop_variables=args.drop_vars,
    loadable_variables=args.loadable_vars,
)

if args.varlabel not in vds:
    raise SystemExit(
        f"variable {args.varlabel!r} not found; available variables: {list(vds.data_vars)}"
    )

vds[[args.varlabel]].vz.to_kerchunk(str(outfile), format="parquet")
print(f"wrote {outfile}")
