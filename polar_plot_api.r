# install.packages("xfun")
library("xfun")
packages <- c("plumber", "tidyverse", "openair", "glue")
pkg_attach2(packages)

# Source import functions ----

source("../airquality/importODS.R")

# Variables ----

date_on <- "2022-04-01 00:00:00"
date_off <- "2022-04-14 23:59:59"
sensor_id <- "70326"
pollutant <- "pm10"


# Checking inputs ----



recent <- . %>% as.Date() %>% year() %>% `>`(2018)
not_too_recent <- . %>% as.Date() %>% `<`(Sys.Date() - 1)
order_dates <- function(date_on, date_off){
    date_off %>% as.Date() > date_on %>% as.Date()
}

not_too_recent("2022-04-13")

order_dates(date_on, date_off)

check_length <- function(date_on, date_off, ld_raw){
    start = as.POSIXct(date_on)
    end = as.POSIXct(date_off)
    difftime(end, start, units = "hours") %>% 
        as.integer() == nrow(ld_raw)
}


# get some sample data

# aq
ld_raw <- getODSExport(select_str = "sensor_id, date, pm10, pm2_5",
                       date_col = "date",
                       dateon = date_on,
                       dateoff = date_off,
                       where_str = glue("sensor_id={sensor_id}"),
                       dataset = "luftdaten_pm_bristol",
                       order_by = NULL,
                       refine = NULL,
                       apikey = NULL)# %>% 
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

met_proc_tbl <- met_raw %>% 
    select(date = date_time, ws, wd, rh, temp) %>% 
    timeAverage(avg.time = "hour")

# join

joined_tbl <- ld_raw %>% 
    left_join(met_proc_tbl, by = "date")


# plot

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

plot(pp$plot)

?polarPlot


