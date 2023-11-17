library(knitr)
library(data.table)
library(httr)

if (file.exists("Stromverbrauch_productiv.R")) {
  #Delete file if it exists
  file.remove("Stromverbrauch_productiv.R")
}

knitr::purl("Stromverbrauch_productiv.Rmd", output = "Stromverbrauch_productiv.R")

fread("pw.txt") -> pw

x <- httr::GET("https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/nbcn-daily_BAS_previous.csv",
          use_proxy(paste0(pw[system=="internet", login], ":", pw[system=="internet", password], "@proxy1.bs.ch"), 3128))
bin <- content(x, "raw")
writeBin(bin, "data/nbcn-daily_BAS_previous.csv")

x <- httr::GET("https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/nbcn-daily_BAS_current.csv",
          use_proxy(paste0(pw[system=="internet", login], ":", pw[system=="internet", password], "@proxy1.bs.ch"), 3128))
bin <- content(x, "raw")
writeBin(bin, "data/nbcn-daily_BAS_current.csv")

original_script <- readLines("Stromverbrauch_productiv.R")

modified_script <- gsub("https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte",
                        "data",
                        original_script, fixed = TRUE)

old_line <- 'httr::GET("https://data.bs.ch/explore/dataset/100233/download/?format=csv&timezone=Europe%2FBerlin")'
new_line <- 'httr::GET("https://data.bs.ch/explore/dataset/100233/download/?format=csv&timezone=Europe%2FBerlin", use_proxy(paste0(pw[system=="internet", login], ":", pw[system=="internet", password], "@proxy1.bs.ch"), 3128))'

modified_script <- gsub(old_line, new_line, modified_script, fixed = TRUE)

old_line <- 'httr::GET("https://data.bs.ch/explore/dataset/100074/download/?format=csv&timezone=Europe%2FBerlin")'
new_line <- 'httr::GET("https://data.bs.ch/explore/dataset/100074/download/?format=csv&timezone=Europe%2FBerlin", use_proxy(paste0(pw[system=="internet", login], ":", pw[system=="internet", password], "@proxy1.bs.ch"), 3128))'

modified_script <- gsub(old_line, new_line, modified_script, fixed = TRUE)

modified_script <- gsub("100245_Strom_Wetter.csv", "data/export/100245_Strom_Wetter.csv", modified_script, fixed=TRUE)
modified_script <- gsub("renv::snapshot()", "", modified_script, fixed=TRUE)

writeLines(modified_script, "Stromverbrauch_productiv.R")

source("Stromverbrauch_productiv.R")