library(knitr)

knitr::purl("Stromverbrauch_productiv.Rmd", output = "Stromverbrauch_productiv.R")

source("Stromverbrauch_productiv.R")