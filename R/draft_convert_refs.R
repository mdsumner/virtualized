# d <- fs::dir_ls(regexp = ".*parq$")
# fs::dir_copy(d, file.path("remote", d))

f <- fs::dir_ls("remote/", regexp = ".*parq$", type = "f", recurse = T)

library(arrow)

pat <- "/g/data/gb6/BRAN/BRAN2023/daily/"
repl <- "https://thredds.nci.org.au/thredds/fileServer/gb6/BRAN/BRAN2023/daily/"
for (i in seq_along(f)) {
  d <- read_parquet(f[i])
  if (!is.raw(d$raw[[1]])) {
    #stop()
    d$path <- stringr::str_replace(d$path, pat, repl)
    write_parquet(d, f[i])
  }
}

