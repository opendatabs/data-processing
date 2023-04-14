## Libraries

library(httr)
library(data.table)
library(dplyr)
library(lubridate)
library(ggplot2)
library(stringr)
library(forecast)
library(fastDummies)
library(zoo)
library(prophet)
library(tidyr)


# Daten importieren und vorbereiten

## Meteorologischen Daten

set.seed(1234)


fread("/code/data-processing/stata_erwarteter_stromverbrauch/pw.txt") -> pw
# pw-Datei muss drei Spalten beinhalten: system, login und password. Diese Datei kannst du als ein Tab-getrennte Text Datei erstellen und dein Internet-Passwort eingeben.

x <- httr::GET("https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/nbcn-daily_BAS_previous.csv",
          use_proxy(paste0(pw[system=="internet", login], ":", pw[system=="internet", password], "@proxy1.bs.ch"), 3128))
bin <- content(x, "raw")
writeBin(bin, "test.csv")

x <- httr::GET("https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/nbcn-daily_BAS_current.csv",
          use_proxy(paste0(pw[system=="internet", login], ":", pw[system=="internet", password], "@proxy1.bs.ch"), 3128))
bin <- content(x, "raw")
writeBin(bin, "test2.csv")


fread("test.csv", sep = ";", colClasses = c("character", "Date", rep("numeric", 10))) %>%
  mutate(
    timestamp = as.POSIXct(
      paste0(
        substr(date, 1, 4),
        "-",
        substr(date, 5, 6),
        "-",
        substr(date, 7, 8),
        " 00:00:00"),
      , format="%Y-%m-%d %H:%M:%S"
    )
  ) %>%
  mutate_at(c("gre000d0", "hto000d0", "nto000d0", "prestad0", "rre150d0", "sre000d0", "tre200d0", "tre200dn", "tre200dx", "ure200d0"), as.numeric) %>%
  bind_rows(
    fread("test2.csv", sep = ";", colClasses = c("character", "Date", rep("numeric", 10))) %>%
      mutate(
        timestamp = as.POSIXct(
          paste0(
            substr(date, 1, 4),
            "-",
            substr(date, 5, 6),
            "-",
            substr(date, 7, 8),
            " 00:00:00"),
          , format="%Y-%m-%d  %H:%M:%S", tz="Europe/Berlin"
        )
      ) %>%
      mutate_at(c("gre000d0", "hto000d0", "nto000d0", "prestad0", "rre150d0", "sre000d0", "tre200d0", "tre200dn", "tre200dx", "ure200d0"), as.numeric)
  ) %>%
  relocate(timestamp) %>%
  select(-c(date, `station/location`)) %>%
  mutate(
    year = as.numeric(substr(timestamp, 1, 4)),
    month = as.numeric(substr(timestamp, 6, 7)),
    day = as.numeric(substr(timestamp, 9, 10))
  ) %>%
  data.frame() -> meteo

invisible(file.remove("test.csv"))
invisible(file.remove("test2.csv"))


## Stromverbrauch-Daten
httr::GET(pw[system=="file_stromverbrauch", url],
          use_proxy(paste0(pw[system=="internet", login], ":", pw[system=="internet", password], "@proxy1.bs.ch"), 3128)
)%>%
  content(., "text") %>%
  fread(sep=";", colClasses = c(timestamp_interval_start_text = "character")) %>%
  select(timestamp = timestamp_interval_start_text, netzlast_kwh = stromverbrauch_kwh, grundversorgte_kunden_kwh, freie_kunden_kwh) %>%
  arrange(timestamp) %>%
  mutate(
    year = as.numeric(substr(timestamp, 1, 4)),
    month = as.numeric(substr(timestamp, 6, 7)),
    day = as.numeric(substr(timestamp, 9, 10))
  ) %>%
  group_by(year, month, day) %>%
  filter(year > 2011) %>%
  summarise(netzlast_kwh = sum(netzlast_kwh, na.rm = T),
            grundversorgte_kunden_kwh = sum(grundversorgte_kunden_kwh, na.rm = T),
            freie_kunden_kwh = sum(freie_kunden_kwh, na.rm = T)) %>%
  ungroup() %>%
  mutate(timestamp = as.POSIXct(
    paste0(year, "-", month, "-", day, " 00:00:00"), format="%Y-%m-%d  %H:%M:%S", tz="Europe/Berlin")
  ) -> strom_daily


## Feiertage, Ferien und sonder Daten


httr::GET("https://data.bs.ch/explore/dataset/100074/download/?format=csv&timezone=Europe%2FBerlin",
           use_proxy(paste0(pw[system=="internet", login], ":", pw[system=="internet", password], "@proxy1.bs.ch"), 3128)
)%>%
  content(., "text") %>%
  fread(sep=";") %>%
  select(tag_datum, name, code, kategorie_name) %>%
  filter(name != "Fasnachtsmontag", name != "Fasnachtsmittwoch", name != "Dies Academicus") %>%
  filter(!(tag_datum == "2008-05-01 00:00:00" & name == "Tag der Arbeit")) %>% # Tag der Arbeit doubles with Auffahrt
  filter(kategorie_name %in% c("Feiertag", "Ferien") | code == "herbstm") %>%
  filter(name != "Semesterferien") -> rd_veranst

rd_veranst %>%
  data.frame() %>%
  mutate(Herbstmesse = if_else(code == "herbstm", "Herbstmesse", "")) %>%
  select(tag_datum, Herbstmesse) %>%
  filter(Herbstmesse != "") %>%
  full_join(
    rd_veranst %>%
      mutate(Feiertage = if_else(kategorie_name == "Feiertag", name, "")) %>%
      select(tag_datum, Feiertage) %>%
      filter(Feiertage != ""),
    by = "tag_datum"
  ) %>%
  full_join(
    rd_veranst %>%
      mutate(Ferien = if_else(kategorie_name == "Ferien", name, "")) %>%
      select(tag_datum, Ferien) %>%
      filter(Ferien != ""),
    by = "tag_datum"
  ) %>%
  mutate(timestamp = as.POSIXct(tag_datum, tz="Europe/Berlin")) %>%
  filter(year(timestamp) > 2011) %>%
  select(timestamp, Herbstmesse, Feiertage, Ferien) %>%
  mutate(
    year = lubridate::year(timestamp),
    month = lubridate::month(lubridate::floor_date(timestamp, "month")),
    day = lubridate::day(lubridate::floor_date(timestamp, "day"))
  ) %>%
  data.frame() -> Veranstaltungen

rm(rd_veranst)


# Datensätze zusammenfügen

meteo %>%
  full_join(Veranstaltungen, by = c("year" = "year", "month" = "month", "day" = "day")) %>%
  full_join(strom_daily, by = c("year" = "year", "month" = "month", "day" = "day")) %>%
  # mutate(weekday = lubridate::wday(timestamp, label = TRUE, abbr = TRUE, locale="German_Germany"),
  #        daytype = if_else(weekday %in% c("So", "Sa"), "Wochenende", "Werktage")) %>%
  # mutate(weekday = lubridate::wday(timestamp, label = TRUE, abbr = TRUE, locale="de_DE"),
  #        daytype = if_else(weekday %in% c("So", "Sa"), "Wochenende", "Werktage")) %>%
  mutate(weekday = lubridate::wday(timestamp, label = TRUE, abbr = TRUE),
         daytype = if_else(weekday %in% c("Sun", "Sat"), "Wochenende", "Werktage")) %>%
  mutate(Covid19_Lockdown = if_else(timestamp < as.POSIXct("2020-03-15", format="%Y-%m-%d"), 0,
                                    if_else(timestamp > as.POSIXct("2020-05-10", format="%Y-%m-%d"), 0, 1))
  ) %>%
  relocate(timestamp, netzlast_kwh, grundversorgte_kunden_kwh, freie_kunden_kwh) %>%
  filter(year(timestamp) > 2011) %>%
  select(-c(timestamp.y, timestamp.x)) %>%
  mutate(HGT = if_else(tre200d0 <= 12, 20-tre200d0, 0)) %>%
  mutate(Herbstmesse = if_else(is.na(Herbstmesse), "No", Herbstmesse),
         Feiertage = if_else(is.na(Feiertage), "No", Feiertage),
         Ferien = if_else(is.na(Ferien), "No", Ferien),
         HGT_sq = HGT^2) %>%
  drop_na() -> data

# Das Modell

data %>%
  mutate(time = as.Date(timestamp, tz = 'Europe/Berlin')) %>%
  select(-timestamp) %>%
  relocate(time) -> data2

#Dummies kreieren
data2dum <- fastDummies::dummy_cols(data2, select_columns = c("weekday"))
data2dum <- fastDummies::dummy_cols(data2dum, select_columns = c("Ferien"))
data2dum <- fastDummies::dummy_cols(data2dum, select_columns = c("Feiertage"))

#Ferien und Feiertage mit Leerschlägen umbenennen
names(data2dum)[which(names(data2dum)=="Ferien_Fasnachtsferien (Sportferien)")] <- "Ferien_Fasnachtsferien"
names(data2dum)[which(names(data2dum) == "Ferien_Osterferien (Frühlingsferien)")] <- "Ferien_Osterferien"
names(data2dum)[which(names(data2dum)=="Feiertage_Tag der Arbeit")] <- "Feiertage_TagderArbeit"

##Kühlgradtage
data2dum$KGT <- 0
data2dum[data2dum$tre200d0 >= 18.3, "KGT"] <- data2dum[data2dum$tre200d0 >= 18.3, "tre200d0"] - 18.3

###Trainingsdatensatz kreieren:
data2dum$netzts <- msts(data2dum$netzlast_kwh, seasonal.periods=c(365.25,7), ts.frequency=7, start=c(2012, 1, 1))
data2dumsub <- subset(data2dum, time < as.Date("2022-07-01"))

###Prognose-Datensatz
data2dumsub22 <- subset(data2dum, time > as.Date("2022-06-30"))

#Datensatz für Modellierung
df <- mutate (
  data2dumsub,
  ds = time,
  y = netzts
)

#Schätze Modell
l <- prophet(interval.width = 0.95, changepoint.prior.scale = 0.05, uncertainty_samples = 10000, yearly.seasonality=F)
l <- add_regressor(l, "Covid19_Lockdown", standardize = F)
l <- add_regressor(l, "gre000d0", standardize = F)
l <- add_regressor(l, "tre200d0", standardize = F)
l <- add_regressor(l, "KGT", standardize = F)
l <- add_regressor(l, "HGT", standardize = F)
l <- add_regressor(l, "HGT_sq", standardize = F)
l <- add_regressor(l, "Ferien_Sommerferien", standardize = F)
l <- add_regressor(l, "Ferien_Weihnachtsferien", standardize = F)
l <- add_regressor(l, "Ferien_Herbstferien", standardize = F)
l <- add_regressor(l, "Ferien_Fasnachtsferien", standardize = F)
l <- add_regressor(l, "Ferien_Osterferien", standardize = F)
l <- add_regressor(l, "Feiertage_Auffahrt", standardize = F)
l <- add_regressor(l, "Feiertage_Bundesfeiertag", standardize = F)
l <- add_regressor(l, "Feiertage_Pfingssonntag", standardize = F)
l <- add_regressor(l, "Feiertage_Pfingstmontag", standardize = F)
l <- add_regressor(l, "Feiertage_Karfreitag", standardize = F)
l <- add_regressor(l, "Feiertage_Ostersonntag", standardize = F)
l <- add_regressor(l, "Feiertage_Ostermontag", standardize = F)
l <- add_regressor(l, "Feiertage_Weihnachten", standardize = F)
l <- add_regressor(l, "Feiertage_Stephanstag", standardize = F)
l <- add_regressor(l, "Feiertage_TagderArbeit", standardize = F)
l <- add_regressor(l, "Feiertage_Neujahrstag", standardize = F)
l <- add_regressor(l, "Feiertage_Heiligabend", standardize = F)
l <- add_regressor(l, "Feiertage_Silvester", standardize = F)
m <- fit.prophet(l, df)

letztebeob <- dim(data2)[1]
spaltebeob <- data2[letztebeob,]

make_future_dataframe(m, periods = as.numeric(as.Date(spaltebeob$time) - as.Date("2022-06-30"))) %>%
  mutate(ds = as.Date(ds)) %>%
  full_join(data2dum[,c("time", "Covid19_Lockdown", "gre000d0", "tre200d0", "KGT", "HGT", "HGT_sq", "Ferien_Sommerferien", "Ferien_Weihnachtsferien", "Ferien_Herbstferien", "Ferien_Fasnachtsferien", "Ferien_Osterferien", "Feiertage_Bundesfeiertag", "Feiertage_Karfreitag", "Feiertage_Pfingssonntag", "Feiertage_Pfingstmontag", "Feiertage_Ostersonntag", "Feiertage_Ostermontag", "Feiertage_Auffahrt", "Feiertage_Weihnachten", "Feiertage_Heiligabend", "Feiertage_TagderArbeit", "Feiertage_Neujahrstag", "Feiertage_Stephanstag", "Feiertage_Silvester")],
            by = c("ds" = "time")) -> future

future22 <- subset(future, ds > as.Date("2022-06-30"))

forecastalle <- predict(m, future)
forecast <- predict(m, future22)

#Vergangenheit und Zukunft
data2dum$forecast <- forecastalle$yhat
data2dum$vgl <- data2dum$netzlast_kwh - data2dum$forecast
mean(abs(data2dum$vgl)) #Messung der summierten absuluten Abweichungen gesamter Datensatz
data2dum$forecast_lowFI <-  forecastalle$yhat_lower
data2dum$forecast_highFI <- forecastalle$yhat_upper

#Zukunft
data2dumsub22$forecast <- forecast$yhat
data2dumsub22$vgl <- data2dumsub22$netzlast_kwh - data2dumsub22$forecast
mean(abs(data2dumsub22$vgl)) #Messung der summierten absuluten Abweichungen Zukunftsperiode

#Hier KIs für rollende Durchschnitte bauen:
posterior_samples_weekly <- predictive_samples(m,future22)$yhat
x <- seq(from = 1, to = as.numeric(as.Date(spaltebeob$time) - as.Date("2022-06-30"))-6, by = 1)
y <- seq(from = 7, to = as.numeric(as.Date(spaltebeob$time) - as.Date("2022-06-30")), by = 1)
loop_indices <- cbind(x,y)

#leerer Datensatz
store_values <- matrix(0, nrow = as.numeric(as.Date(spaltebeob$time) - as.Date("2022-06-30")), ncol = 2)

#loop berechnet summierte KIs
for(i in 1:nrow(loop_indices)){
  store_values [i, ] = quantile(colSums(posterior_samples_weekly[loop_indices[i, 1]:loop_indices[i, 2], ]), probs = c(0.025, 0.975))
}

#7-Tage Durchschnitte
store_values_day <- store_values / 7
data2dum$rollav <- rollmean(data2dum$netzlast_kwh, k = 7, fill = NA)

###Erster Tag für rollenden Durchschnitt ist der 4.7., letzter vier Tage vor Ende
roll22 <- subset(data2dum, time>as.Date("2022-07-03"))
roll22 <- subset(roll22, time<as.Date(paste(spaltebeob$year, spaltebeob$month, spaltebeob$day, sep="-"))-2)
roll22$lower <- NA
roll22$lower[1:dim(roll22)[1]] <- store_values_day[c(1:dim(roll22)[1]),1]
roll22$upper <- NA
roll22$upper[1:dim(roll22)[1]] <- store_values_day[c(1:dim(roll22)[1]),2]

#Zusammenfügen, umbennenen und OGD-Datensatz erstellen
rollklein <- roll22[,c("time", "rollav", "lower", "upper")]
names(rollklein)[3] <- "roll_forecast_lowFI"
names(rollklein)[4] <- "roll_forecast_highFI"
fullexport <- data2dum[,c("time", "netzlast_kwh", "forecast", "vgl", "forecast_lowFI", "forecast_highFI")]
fullexport$time <- as.Date(fullexport$time)
names(fullexport)[which(names(fullexport)=="netzlast_kwh")] <- "stromverbrauch"
names(fullexport)[which(names(fullexport)=="vgl")] <- "vgl_real_minus_forecast"
fullexport$trainorforecast <- "f"
fullexport[fullexport$time < as.Date("2022-07-01"), "trainorforecast"] <- "t"
fullexport <- merge(fullexport, rollklein, by=c("time"), all.x=T)
fullexport$timestamp <- NULL
wd <- getwd()
write.csv2(fullexport, paste0(wd, "/stata_erwarteter_stromverbrauch/data/export/100245_Strom_Wetter.csv"), row.names=F, na = "")


