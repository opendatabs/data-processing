library(knitr)
library(data.table)
library(httr)

if (file.exists("OL_Gasverbrauch.R")) {
  #Delete file if it exists
  file.remove("OL_Gasverbrauch.R")
}

knitr::purl("gasverbrauch/Productive/OL_Gasverbrauch.Rmd", output = "OL_Gasverbrauch.R")

fread("pw.txt") -> pw

x <- httr::GET("https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/nbcn-daily_BAS_previous.csv",
          use_proxy(paste0(pw[system=="internet", login], ":", pw[system=="internet", password], "@proxy1.bs.ch"), 3128))
bin <- content(x, "raw")
writeBin(bin, "data/nbcn-daily_BAS_previous.csv")

x <- httr::GET("https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/nbcn-daily_BAS_current.csv",
          use_proxy(paste0(pw[system=="internet", login], ":", pw[system=="internet", password], "@proxy1.bs.ch"), 3128))
bin <- content(x, "raw")
writeBin(bin, "data/nbcn-daily_BAS_current.csv")

original_script <- readLines("OL_Gasverbrauch.R")

modified_script <- gsub("https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte",
                        "data",
                        original_script, fixed = TRUE)

old_line <- 'Europe%2FBerlin")'
new_line <- 'Europe%2FBerlin", use_proxy(paste0(pw[system=="internet", login], ":", pw[system=="internet", password], "@proxy1.bs.ch"), 3128))'

modified_script <- gsub(old_line, new_line, modified_script, fixed = TRUE)

modified_script <- gsub("100358_gasverbrauch.csv", "data/export/100358_gasverbrauch.csv", modified_script, fixed=TRUE)

writeLines(modified_script, "OL_Gasverbrauch.R")

source("OL_Gasverbrauch.R")
