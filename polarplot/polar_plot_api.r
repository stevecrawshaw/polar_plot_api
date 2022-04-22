# Libraries ----

library("openair")

# Variables ----

# date_on <- "2022-04-01 00:00:00"
# date_off <- "2022-04-07 23:59:59"
# sensor_id <- "70326"
# pollutant <- "pm10"
# Functions for checking inputs ----
# dates
dates_ok <- function(date_on, date_off){

    recent <- . %>% as.Date() %>% year() %>% `>`(2018)
    
    not_too_recent <- . %>% as.Date() %>% `<`(Sys.Date() - 1)
    
    order_dates <- function(date_on, date_off){
        date_off %>% as.Date() > date_on %>% as.Date()
        }

interval_length <- function(date_on, date_off){
    stop <- date_off %>% as.Date()
    start <- date_on %>% as.Date()
        difftime(stop, start, unit = "hours") %>%
            as.integer() > 48
        }

return(
    recent(date_on) &
    recent(date_off) &
    order_dates(date_on, date_off) &
    not_too_recent(date_off) &
    interval_length(date_on, date_off))
}

# Function for download aq and met data ----

get_data <- function(date_on, date_off, sensor_id){
# have we got most (95%) of the data
    check_length <- function(date_on, date_off, dl_data){
        nms <- names(dl_data)
        ismet <- any(grepl("wd", nms)) # met data half hourly so adjust check
        if(ismet) mult = 2L else mult = 1L
        start = as.POSIXct(date_on)
        end = as.POSIXct(date_off)
        # do at least 95% of data exist?
        (difftime(end, start, units = "hours") %>% 
            as.integer() * mult / nrow(dl_data)) %>% 
            abs() > 0.95 %>% 
            return()
    }

    ld_raw <- getODSExport(select_str = "sensor_id, date, pm10, pm2_5",
                           date_col = "date",
                           dateon = date_on,
                           dateoff = date_off,
                           where_str = glue("sensor_id={sensor_id}"),
                           dataset = "luftdaten_pm_bristol",
                           order_by = NULL,
                           refine = NULL,
                           apikey = NULL) %>% 
        rename(pm2.5 = pm2_5)

# met

met_raw <- getODSExport(select_str = "date_time, ws, wd",
                        where_str = "",
                        date_col = "date_time",
                        dateon = date_on,
                        dateoff = date_off,
                        dataset = "met-data-bristol-lulsgate",
                       order_by = NULL,
                       refine = NULL,
                       apikey = NULL)
# check to see if data matches
data_ok <- check_length(date_on, date_off, dl_data = ld_raw) &
    check_length(date_on, date_off, dl_data = met_raw)

if(data_ok) {
met_proc_tbl <- met_raw %>% 
    select(date = date_time, ws, wd) %>% 
    timeAverage(avg.time = "hour")

joined_tbl <- ld_raw %>% 
    left_join(met_proc_tbl, by = "date") %>% 
    return()
} else {
    return(print("Data returned does not match dates selected"))
}

}

# Plotting Function ----

polar_plot <- function(joined_tbl, pollutant = "pm10"){
    
sensor <- unique(joined_tbl$sensor_id)[1]
    
start <- joined_tbl$date %>% 
    min() %>% 
    format("%d/%m/%Y")

stop  <- joined_tbl$date %>% 
    max() %>% 
    format("%d/%m/%Y")
        
    pp <- polarPlot(joined_tbl,
                    pollutant = pollutant, 
                    statistic = "max",
                    main = glue("Polar plot of {pollutant} for {sensor}"),
                    sub = glue("From {start} to {stop}"))
return(plot(pp$plot))    
}

# Check dates and download data if valid ----
# 
# if(dates_ok){
# joined_tbl <- get_data(date_on, date_off, sensor_id)
# } else {
#     print("Unable to dowload data due to invalid dates")
# }
# 
# if(exists("joined_tbl")){
# polar_plot(joined_tbl, pollutant = "pm2.5")
# } else {
#     print("No data to plot")
# }

