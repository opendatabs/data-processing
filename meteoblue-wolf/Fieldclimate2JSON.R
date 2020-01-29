pessl_api<-function(delta_time,time_backwards){
  
  #delta_time <- 15
  #time_backwards <-3
  #setwd("/Users/jonasbieri/Documents/Meteoblue/r")
  
  require("curl")
  require("digest")
  require("jsonlite")

  curl.method <- "GET"
  curl.req <- "/user/stations"
  curl.now <- strftime(Sys.time(), "%A, %D %b %Y %H:%M:%S %Z")
  curl.sign <- paste(curl.method,curl.req,curl.now,pubkey,sep="")
  this.sign <- hmac(privkey, curl.sign, algo = c("sha256"))
  curl.handle <- new_handle()
  handle_setheaders(curl.handle,
                    "Accept" = "application/json",
                    "Authorization" = paste("hmac ",pubkey,":",this.sign,sep=""),
                    "Date" = curl.now)
  
  
  #handle_setopt(curl.handle, ssl_verifypeer = FALSE)
  # handle_setopt(curl.handle,
  
  #print(curl_options())
  
  # _options())
  
  # handle_setopt(curl.handle)
  
  temp.json <- tempfile()
  curl_download(url=paste("https://api.fieldclimate.com/v1",curl.req,sep=""),  handle=curl.handle, temp.json)
  
  json_data <- fromJSON(temp.json, flatten=TRUE)
  json_data[,c(24,26,28)] <- NA
  
  
  
  
  stn.name <- json_data$name.original
  metadata.end<-json_data$dates.max_date
  ############################################
  #Fetch all data station for station
  # 
  # Request sounds like this
  # /data/{{FORMAT}}/{{STATION-ID}}/{{DATA-GROUP}}/
  # from/{{FROM-UNIX-TIMESTAMP}}/to/{{TO-UNIX-TIMESTAMP
  ############################################
  
  date<-Sys.Date()
  time<-Sys.time()
  unix_time_start<-as.character(round(as.numeric(as.POSIXct(time))-60*60*24*time_backwards-3600,digits = 0))
  unix_time_end<-as.character(round(as.numeric(as.POSIXct(time)),digits = 0))
  
  num_unix_time_end<-round(as.numeric(as.POSIXct(time)),digits = 0)
  
  previous_end <- floor(num_unix_time_end / (delta_time * 60)) * (delta_time * 60)
  previous_start <- floor(num_unix_time_end / (delta_time * 60)) * (delta_time * 60) - time_backwards * 24 * 60 * 60 
  previous_date_end<-as.POSIXct(previous_end, origin="1970-01-01")
  previous_date_start<-as.POSIXct(previous_start, origin="1970-01-01")
  
  unix_time.metadata.end <- round(as.numeric(as.POSIXct(metadata.end)),digits = 0)
  diff_time<-unix_time.metadata.end - num_unix_time_end
  
  Filter<-which(diff_time < -1800)
  # Over the project duration, this can be expanded until e.g. 2022. For now just for 2019.
  time.axis <- seq(previous_date_start,previous_date_end, 60*delta_time)
  
  # Declare data container
  all.temp.data <- data.frame(datetime = time.axis)
  all.rh.data <- data.frame(datetime = time.axis)
  all.precip.data <- data.frame(datetime = time.axis)
  
  # Loop over sensors
   for (i in 1:length(stn.name)){ 
     curl.req <- paste0("/data/optimized/",stn.name[i],"/raw/from/",unix_time_start,"/to/",unix_time_end)
    
    curl.method <- "GET"
    curl.now <- strftime(Sys.time(), "%A, %D %b %Y %H:%M:%S %Z")
    curl.sign <- paste(curl.method,curl.req,curl.now,pubkey,sep="")
    this.sign <- hmac(privkey, curl.sign, algo = c("sha256"))
    curl.handle <- new_handle()
    handle_setheaders(curl.handle,
                      "Accept" = "application/json",
                      "Authorization" = paste("hmac ",pubkey,":",this.sign,sep=""),
                      "Date" = curl.now)
    temp.json <- tempfile()
    curl_download(url=paste("https://api.fieldclimate.com/v1",curl.req,sep=""),  handle=curl.handle, temp.json)
    if (i != Filter[1] | length(Filter) == 0){
    data <- fromJSON(temp.json, flatten=TRUE)
    this.sensor <- NULL
    this.sensor$datetime <- as.POSIXct(data$dates)+3600 ### summertime/wintertime problem
    this.sensor$temp <- data$data[[4]]$aggr$avg
    this.sensor$rh <- data$data[[5]]$aggr$avg
    this.sensor$precip <- data$data[[3]]$aggr$sum
    
    this.data <- data.frame(datetime = time.axis)
  #if (variable == "temp"){
    interpol.temp.data <- data.frame(approx(this.sensor$datetime,this.sensor$temp, xout = this.data$datetime, 
                                            rule = 1, method = "linear", ties = mean))
  #} else if(variable == "rh"){
    interpol.rh.data <- data.frame(approx(this.sensor$datetime,this.sensor$rh, xout = this.data$datetime, 
                                            rule = 1, method = "linear", ties = mean))
  #} else if (variable == "precip"){
    interpol.precip.data <- data.frame(approx(this.sensor$datetime,this.sensor$precip, xout = this.data$datetime, 
                                            rule = 1, method = "linear", ties = mean))
  #}
    
   } else {
     this.data <- data.frame(datetime = time.axis)
      xx<-seq(1,length(time.axis),1)
      is.na(xx)<-seq(1,length(time.axis),1)
      interpol.temp.data<-data.frame(this.data$datetime,xx)
      interpol.rh.data<-data.frame(this.data$datetime,xx)
      interpol.precip.data<-data.frame(this.data$datetime,xx)
      Filter <- Filter[-1]
    }
    names(interpol.temp.data)[names(interpol.temp.data) == "x"] <- "datetime"
    names(interpol.temp.data)[names(interpol.temp.data) == "y"] <- stn.name[i]
    
    this.data$temp <- round(interpol.temp.data[,2],digits=2)
    names(this.data)[names(this.data) == "temp"] <- stn.name[i]
    
    this.data$rh <- round(interpol.rh.data[,2],digits=2)
    names(this.data)[names(this.data) == "rh"] <- stn.name[i] 
    
    this.data$precip <- round(interpol.precip.data[,2],digits=1)
    names(this.data)[names(this.data) == "precip"] <- stn.name[i] 
    
    all.temp.data <- cbind(all.temp.data, this.data[,c(1,2)])
    all.temp.data = all.temp.data[unique(names(all.temp.data))]
    
    all.rh.data <- cbind(all.rh.data, this.data[,c(1,3)])
    all.rh.data = all.rh.data[unique(names(all.rh.data))]
    
    all.precip.data <- cbind(all.precip.data, this.data[,c(1,4)])
    all.precip.data = all.precip.data[unique(names(all.precip.data))]
  }
  
  
  write.csv(all.temp.data,file=paste0("output/csv/temperature_",time,".csv"),row.names =FALSE)
  write.csv(all.rh.data,file=paste0("output/csv/rh_",time,".csv"),row.names =FALSE)
  write.csv(all.precip.data,file=paste0("output/csv/precip_",time,".csv"),row.names =FALSE)
  
  temp.data<-list(all.temp.data)
  rh.data<-list(all.rh.data)
  precip.data<-list(all.precip.data)
  all.data<-list(temp.data,rh.data,precip.data)
  names(all.data)<-c("temp","rh","precip")
  return(all.data)
}

pessl_api(15,3)