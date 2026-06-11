.libPaths("~/lib") ## this is where custom rhdf5 and blocklist lives
## blocklist: used for installed file in inst/hpc/ helpers
ncpus <- as.integer(Sys.getenv("PBS_NCI_NCPUS_PER_NODE"))

if (!is.na(ncpus) && ncpus > 5) {
  options(blocklist.workers = ncpus - 2)
}
print(sprintf("ncpus: %i", ncpus))

#HELPERS <- Sys.getenv("HELPERS")
source(system.file("hpc/blocklist_hpc.R", package = "blocklist"))     # or source("/g/data/jk72/mds581/vrefs-run/vrefs_hpc.R")
src <- bran_sources()             # the 192-row access/public table
nrow(src)                         # confirm 192

## 1. probe — codec + layout + chunk shape, one file, one open (header only)
.time_probe(src$access[1])        # expect contiguous=FALSE, chunks=1,1,300,300
.time_probe(src$access[nrow(src)])   # same chunks=1,1,300,300 ? same filters?

## 2. one file END TO END — probe + iter + mosaic + write (the stage you haven't timed)
run_one(src, i = 1)               # writes a one-file .zarr to tempdir; times it

## 3. n files — real per-file rate including mosaic-compose + parquet write
# 12-file .zarr to tempdir; prints s/file
out <- run_n(src, n = nrow(src)) # returns the out path
system(paste0("gdal mdim info ZARR:", out))   # dims, dtype, codecs resolve?
