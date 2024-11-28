library(data.table)
library(zoo)
library(lubridate)
library(tidyverse)

get_dataset <- function(url) {
  # Create directory if it does not exist
  data_path <- file.path('code', 'data-processing', 'stata_konoer', 'data')
  if (!dir.exists(data_path)) {
    dir.create(data_path, recursive = TRUE)
  }
  # Download the CSV file
  csv_path <- file.path(data_path , '100020.csv')
  download.file(url, csv_path, mode = "wb")

  # Read the CSV file
  data <- tryCatch(
      read.csv(csv_path, sep = ";", stringsAsFactors = FALSE, encoding = "UTF-8"),
      warning = function(w) NULL,
      error = function(e) NULL
  )
  # if dataframe only has one column or less the data is not ";" separated
  if (is.null(data) || ncol(data) <= 1) {
      stop("The data wasn't imported properly. Very likely the correct separator couldn't be found.\nPlease check the dataset manually and adjust the code.")
  }
  return(data)
}

pathBussen <- "/code/data-processing/kapo_ordnungsbussen/data/Ordnungsbussen_OGD_all.csv"
pathWildeDeponien <- "/code/data-processing/stadtreinigung_wildedeponien/data/wildedeponien_all.csv"
urlStrassenverkehr <- "https://data.bs.ch/explore/dataset/100020/download?format=csv&timezone=Europe%2FZurich"

data_deponien <- fread(pathWildeDeponien, header = TRUE)
data_deponien_new <- data_deponien %>%
  select(-id) %>%
  mutate(abfallkategorie = ifelse(abfallkategorie=="", "Unbekannt", abfallkategorie))%>%
  # mutate(geometry = gsub(".*\\(", "", geometry),
  #        geometry = gsub("\\)", "", geometry)) %>%
  # separate_wider_delim(col=geometry, delim = " ", names = c("longitude","latitude")) %>%
  rename(incident_type_primary = abfallkategorie) %>%
  mutate(parent_incident_type = "Wilde Deponien",
         incident_date = lubridate::date(data_deponien$bearbeitungszeit_meldung),
         year = lubridate::year(incident_date),
         month = lubridate::month(incident_date),
         day_of_week_nr = lubridate::wday(incident_date),
         day_of_week = lubridate::wday(incident_date, label = TRUE, abbr = FALSE),
         hour_of_day = lubridate::hour(bearbeitungszeit_meldung))  %>%
  rownames_to_column(var = "id") %>%
  mutate(x = pmap(.l = list(lon,lat),.f = function(lon,lat,...){
    eRTG3D::transformCRS.3d(data.frame(x = lon, y = lat, z = 260), fromCRS=2056, toCRS=4326)
  }))%>% 
  unnest(x) %>%
  rename(longitude = x, latitude =y)%>%
  select(id,incident_date,year, month,day_of_week_nr,day_of_week,hour_of_day,longitude,latitude,parent_incident_type,incident_type_primary)

# data_deponien_new  %>% View()
#   select(id,incident_date,year, month,day_of_week_nr,day_of_week,hour_of_day,lon,lat ,parent_incident_type,incident_type_primary)
# transformationLonLat <- eRTG3D::transformCRS.3d(as.data.frame(data_deponien_new %>% select(x=lon, y=lat) %>% 
#                                                mutate(z = rep(260, times =dim(data_deponien_new)[1]))), fromCRS=2056, toCRS=4326)%>%
#   rownames_to_column(var = "id")
# data_deponien_new <- data_deponien_new %>% left_join(transformationLonLat %>% select(id, longitude=x , latitude=y), by= join_by(id==id))
# 
write.csv(data_deponien_new,file = "/code/data-processing/stata_konoer/data/data_wildeDeponien.csv", fileEncoding = "UTF-8", row.names = FALSE)
 
#Ordnungsbussen select = c(1,5,6,7,12,19,20)
data_bussen <-fread(pathBussen, header = TRUE)
data_bussen_new <- data_bussen %>%
  rename(incident_type_primary = BuZi,
         incident_type_primary_text = "BuZi Text", 
         vehicle_typ = "KAT BEZEICHNUNG",
         year = "Übertretungsjahr", 
         month = "Übertretungsmonat", 
         latitude = "GPS Breite", 
         longitude = "GPS Länge", 
         day_of_week_nr = "ÜbertretungswochentagNummer", 
         day_of_week = Übertretungswochentag, 
         id = Laufnummer) %>%
  mutate(parent_incident_type = "Ordnungsbussen",
         incident_date =lubridate::ymd(data_bussen$Übertretungsdatum),
         hour_of_day = NA) %>% 
  select(id,incident_date,year,month,day_of_week_nr, day_of_week,hour_of_day,longitude,latitude,parent_incident_type, incident_type_primary,vehicle_typ) %>% 
  filter(!is.na(longitude)) %>%
  filter(!is.na(latitude))
print(head(data_bussen_new))
write.csv(data_bussen_new,file = "/code/data-processing/stata_konoer/data_Ordnungsbussen.csv", fileEncoding = "UTF-8", row.names = FALSE)

data_strassenverkehr <- get_dataset(urlStrassenverkehr)
data_strassenverkehr_new <- data_strassenverkehr %>%
  rename(id = "Eindeutiger Identifikator des Unfalls",
         incident_type_primary= "Beschreibung zum Unfalltyp", 
         year=Unfalljahr,
         x_temp = "Beschreibung der Unfallschwerekategorie", 
         month = Unfallmonat,
         hour_of_day = Unfallstunde) %>% 
  separate_wider_delim(col="Geo Point", delim = ", ", names = c("latitude","longitude")) %>%
  separate_wider_delim(col="Wochentag", delim = " ", names = c("day_of_week_nr","day_of_week")) %>%
  mutate(parent_incident_type = "Verkehrsunfälle",
         longitude = as.numeric(longitude),
         latitude = as.numeric(latitude),
         incident_date = lubridate::ymd(paste(year,month,"1", sep = "-"))) %>% 
  rowwise() %>%
  mutate(x = pmap(.l = list(x_temp),.f = function(...){
          temp <-  stringr::str_split(x_temp, pattern = " ", n = 2)
    return(paste(temp[[1]][1], temp[[1]][2], sep = ","))
    })) %>% 
  separate_wider_delim(col="x", delim = ",", names = c("incident_type_secondary_nr","incident_type_secondary")) %>% 
  select(id,incident_date,year,month,day_of_week_nr, day_of_week,hour_of_day,longitude,latitude,parent_incident_type, incident_type_primary,incident_type_secondary_nr,incident_type_secondary)

write.csv(data_strassenverkehr_new,file = "/code/data-processing/stata_konoer/data/data_strassenverkehr.csv", fileEncoding = "UTF-8", row.names = FALSE)

write.csv(data_strassenverkehr_new %>% select(parent_incident_type,incident_type_primary,incident_type_secondary_nr,incident_type_secondary) %>% unique(),
          file = "/code/data-processing/stata_konoer/data/data_strassenverkehrziffern.csv", fileEncoding = "UTF-8", row.names = FALSE)
