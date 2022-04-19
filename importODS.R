library("xfun")
pkg_attach2(c("tidyverse", "httr", "jsonlite", "glue", "anytime", "lubridate", "assertive"))
# Parse Column Types for a dataset using the /dataset_id API endpoint

# Get Fields ----
get_fields_fnc <- function(dataset_id, apikey){

base_url <- "https://opendata.bristol.gov.uk/api/v2/catalog/datasets/"
# dataset_id <- "cycling-west-of-england-network"
url <- glue("{base_url}{dataset_id}/")
qry <- list(
  select = "*",
  lang = "en",
  timezone = "UTC",
  apikey = apikey
)

r_dataset_id <- GET(url = url, query = qry, encode = "json")
sc <- r_dataset_id %>% status_code()
  if(sc == 200){
  fields_tbl <- r_dataset_id %>%
    content(as = "text") %>%
    fromJSON() %>% 
    pluck("dataset", "fields") %>%
    select(-annotations) %>% 
    as_tibble()
  
  data_type_tbl <- tribble(
    ~ODS_type, ~R_type, ~abb,
    "text", "character", "c",
    "int", "integer", "i",
    "date", "date", "D",
    "datetime", "datetime", "T",
    "double", "double", "d",
    "geo_point_2d", "character", "c",
    "geo_shape", "character", "c"
    
  )
  
  dsf <- fields_tbl %>% 
    left_join(data_type_tbl, by = c("type" = "ODS_type"))
  
  } else {
    dsf <- sc
  }

return(dsf)
}

# fields_f <- get_fields_fnc("luftdaten_pm_bristol")
# 
# function to get the col_types to pass to read_delim
# if select = * then don't filter
# if select is vectorish of fields, separate and use as filter criteria
# if select is vector of length 1 use that as a filter on name
# allcols_tbl comes from get_field_fnc

# Get Column Types ----

get_col_types_fnc <- function(select_str, allcols_tbl){
  
  if(select_str == "*"){
    col_tbl <- allcols_tbl
  } else if(str_detect(select_str, ", ")){
    col_tbl <- filter(allcols_tbl, name %in% as_vector(str_split(select_str, pattern = ", ")))
  } else {
    col_tbl <-  filter(allcols_tbl, name %in% select_str)
  }
  # do some re ordering if a selection of fields are queried so that col_types matche
  # field order
  if(str_detect(select_str, ", ")){
    select_ordered_tbl <- enframe(unlist(str_split(select_str, pattern = ", "))) %>% 
      inner_join(col_tbl, by = c("value" = "name"))
  } else {
    select_ordered_tbl <- col_tbl
  }
  return(select_ordered_tbl)
}


# get_col_types_fnc(select_str = "sensor_id, date", allcols_tbl = fields_f)

# Date Helper ----

datehelper_fnc <- function(dateon, dateoff){
  # make a range string from two dates
  
  # dateon <- "2021-01-01 00:02:00"
  # dateoff <- "01/02/2021"
  
  dates_chr <- c("dateon" = dateon, "dateoff" = dateoff)
  dates_vec <- parse_date_time2(dates_chr, orders = c("Ymd", "dmY", "YmdHMS", "dmYHMS", "YmdHM", "dmYHM"))

  # stopifnot(!any(is.na(dates_vec)),
  #           dates_vec[2] > dates_vec[1])
  
  tp <- strftime(dates_vec, format = "%Y-%m-%dT%H:%M:%S")

  where_str_date_portion <- glue(" IN ['{tp[1]}' TO '{tp[2]}']")
  return(where_str_date_portion)
  }
  
# datehelper_fnc(dateon = "2021-02-02", dateoff = "03-03-2021")  

# Import ODS AQ ----

importODSAQ <-
  function(pollutant = c("all", "nox", "no2", "no", "pm10", "pm2.5", "o3"),
           siteid = c("203", "215", "463", "270", "500", "501"),
           dateFrom = "2018-01-01",
           dateTo = "2018-01-02",
           includeGeo = FALSE) {

    #---------------------------------VARIABLES-----------------------------

    
    # dateFrom <- "2020-01-01"
    # dateTo <- "2020-01-02"
    # pollutant <- c("nox", "no2)
    # siteid = "all"
    # includeGeo = F
    
#--------------------------------ASSERT VALID DATA------------------------
dates <- c(dateFrom, dateTo)
#pollutant names
pollutant <- tolower(str_replace(pollutant, "[.]", "")) #make nice for the ODS
    
#any(pollutant == "no2")
 pollnames <- #vector of valid pollutant names
    c(
      "nox",
      "no2",
      "no",
      "co",
      "so2",
      "o3",
      "pm10",
      "pm25",
      "vpm10",
      "nvpm10",
      "nvpm25",
      "vpm25",
      "temp",
      "rh",
      "press"
    )
    if (any(pollutant != "all")) { #pollutant specified
     
      if (!any(pollutant %in% pollnames)) {
        stop(
          c(
            "You have entered one or more invalid pollutant names. Possible values are: ",
            paste0(pollnames, collapse = " "), ". Please also review the schema: https://opendata.bristol.gov.uk/explore/dataset/air-quality-data-continuous/information/?disjunctive.location"
          )
        )
      } #if any pollutant arguments don't match field names from the air-quality-data-continuous dataset, stop the function
    }
    
    # DATES
    #function to check whether dates are valid
  is.convertible.to.date <- function(x) all(!is.na(as.Date(x, tz = 'UTC', format = '%Y-%m-%d')))
    
    if(!is.convertible.to.date(dates)){
      stop("You have supplied a non - compliant date string: Should be in the format 2020-02-28")
    }
    
    if (any(as.Date(dates) > Sys.Date())) {
      stop("dateFrom and dateTo cannot be in the future")
    }# are they in the past?
    
    if (!is_logical(includeGeo)) {
      stop("includeGeo must be TRUE or FALSE") #CHECK LOGICAL VARIABLES ARE
    }
    
    if (as.Date(dateFrom) - as.Date(dateTo) >= 0) {
      stop("The end date precedes or is equal to the start date.")
    }
    # CREATE DATE QUERY PORTION OF ODS SQL STRING----------------
    date_query_string <-
      paste0("date_time IN ['",
             dateFrom,
             "T00:00:00' TO '",
             dateTo,
             "T23:59:00']")

#--------function to get query string for all the sites measuring a vector of pollutants-------
    #this uses the refine=key:value syntax in a list to specify the facets on which to filter
    #this works with this dataset (air-quality-monitoring-sites)
    #because the pollutants are in one field, but split by a processor in the #configuration of the dataset
    site_polls <- function(pollutant) {
      #if (any(siteid == "all")) {
        sel <- "siteid, pollutants"
        rows <- 100
        url <-
          "https://opendata.bristol.gov.uk/api/v2/catalog/datasets/air-quality-monitoring-sites/records"
        #just select continuous otherwise DT's are returned for NO2
        listvec <-
          c(paste0("pollutants:", toupper(pollutant)),
            "instrumenttype:Continuous (Reference)")
        lst <- as.list(listvec) #make a list for the refine section
        names(lst) <- rep("refine", length(listvec)) #of the query list
        qry_list_a <-
          list(select = sel, rows = rows) # the base qry list
        qry_add <- c(qry_list_a, lst) #append lists
        raq <- GET(url = url, query = qry_add) #get response object
        sites <- content(raq, as = "text") %>%
          jsonlite::fromJSON() %>%
          `[[`("records") %>%
          `[[`("record") %>%
          `[[`("fields") %>%
          pull() #extract DF from JSON \ lists and pull vector of sites that measure the pollutant
        (site_where_qry <-
            paste0("siteid = ", sites, collapse = " OR ")) #return the sites as a ODS SQL query string
      #}
    }
    
  basefields <- "date_time as date, siteid, location"
    
    if(!includeGeo){
      (allpolls <- paste0(pollnames, collapse = ", "))
      (polls =  paste0(pollutant, collapse = ", "))
        } else {
          (allpolls = paste(paste0(pollnames, collapse = ", "), "geo_point_2d", sep = ", "))
          (polls =  paste(paste0(pollutant, collapse = ", "), "geo_point_2d", sep = ", "))
      end_col_types <- paste0(paste0(rep("n", length(pollutant)), collapse = ""), "c", collapse = "")
        } 
    
    
    if (any(pollutant == "all")) {
      (select_str <-
        paste(basefields, allpolls, sep = ", "))
      end_col_types = "nnnnnnnnnnnnnnn"
 
      if (any(siteid == "all")) {
        #all siteids all pollutants
        where_str <- date_query_string
      } else {
        #siteids specified.  all pollutants
        where_str <-
          paste0(date_query_string,
                 " AND (",
                 paste0("siteid=", siteid, collapse = " OR "),
                 ")")
      }
 
    } else {
      #pollutants specified
      
      (select_str <- paste(basefields, polls, sep = ", "))
      end_col_types <- paste0(rep("n", length(pollutant)), collapse = "")
 
      if (any(siteid == "all")) {
        #pollutants specified     all sites
        where_str <-
          paste0(date_query_string, " AND (", site_polls(pollutant), ")")
      } else {
        #pollutants and siteids specified
        where_str <-
          paste0(date_query_string,
                 " AND (",
                 paste0("siteid=", siteid, collapse = " OR "),
                 ")")
      }

    }
    # DEFINE BASE URL AND QUERY STRING FOR API CALL
    base_url <-
      "https://opendata.bristol.gov.uk/api/v2/catalog/datasets/air-quality-data-continuous/exports/csv"
    qry_list <-
      list(select = select_str,
           where = where_str,
           sort = "-date_time")
    # GET the response object
    r <- GET(url = base_url, query = qry_list)
    
    #retrieve the csv data from the response object and change names to be openair compliant
    if (!http_error(r)) {
      aqdata <- content(r, as = "text") %>%
        read_delim(
          na = "",
          delim = ";",
          col_types = paste0("Tic", end_col_types, collapse = "")
        ) 
      names(aqdata) <- aqdata %>%
        names() %>%
        str_replace_all(pattern = "pm25", replacement = "pm2.5")
      return(aqdata)
    } else {
      return(FALSE)
    }
    
  }

# Get ODS Export ----

getODSExport <- function(select_str = "siteid, pm25",
                         date_col = "date_time",
                         dateon = "2021-01-01",
                         dateoff = "02/01/2021",
                         where_str = "siteid = '452'",
                         order_by = "siteid, date_time",
                         refine = "current:True",
                         dataset = "air-quality-data-continuous",
                         apikey = NULL) {
  
  # select_str = "sensor_id, date, pm10, pm2_5, geo_point_2d"
  # date_col = "date"
  # dateon = date_on
  # dateoff = date_off
  # where_str = sts_sensors
  # refine = NULL
  # apikey = NULL
  # dataset = "luftdaten_pm_bristol"
  
base_url <- glue("https://opendata.bristol.gov.uk/api/v2/catalog/datasets/{dataset}/exports/csv/")

# get the fields from the API

allcols_tbl <- get_fields_fnc(dataset_id = dataset, apikey = apikey)
column_tbl <- get_col_types_fnc(select_str = select_str, allcols_tbl = allcols_tbl)
limit <- -1L
# remake the correctly ordered select query string
select_str_ordered <- paste0(column_tbl[["value"]], collapse = ", ")
# make the shortcut string for col types
col_type <- paste0(column_tbl[["abb"]], collapse = "")
# browser()
# Make the where_str: if date_col and dates present construct a date range string
# and add to the other terms for the filter given in where_str
    if(is.character(date_col) & is.character(dateon) & is.character(dateoff)){
      dateportion <- datehelper_fnc(dateon, dateoff)
          if(where_str == "" || is.null(where_str)){
            where_qry_str <- glue("{date_col}{dateportion}")
          } else {
          where_qry_str <- glue("({where_str}) AND {date_col}{dateportion}")
          }
    } else {
      if(where_str == "" || is.null(where_str)){
      #if there's no date filter and no other filter limit query to 1000
      limit <- 1000L
      } 
      where_qry_str <- where_str
      
    }

qry_list <- list(select = select_str_ordered,
                 where = where_qry_str,
                 refine = refine,
                 apikey = apikey,
                 order_by = order_by,
                 limit = limit)

r <- GET(url = base_url, query = qry_list) 

if(!http_error(r)){ #FALSE = no error

content(r, as="text") %>% 
   read_delim(delim = ";", col_types = col_type) %>% 
    return()
} else {
  return(error = list(status_code(r),
              r$url))
}
}

# Get ODS Records ----

# limited to 100 records - can aggregate
getODSRecords <- function(select_str = "siteid, avg(pm25) as mean_pm25",
                          where_str = "date_time IN ['2020-01-01T00:00:00' TO '2020-12-31T23:59:00']",
                          groupby_str = "siteid, year(date_time)",
                          dataset = "air-quality-data-continuous",
                          orderby_str = "siteid",
                          apikey = NULL,
                          limit = 10) {
  base_url <- glue("https://opendata.bristol.gov.uk/api/v2/catalog/datasets/{dataset}/records")
  if(select_str == "all") {select_str <- "*"}
  qry_list <- list(select = select_str,
                   group_by = groupby_str,
                   where = where_str,
                   apikey = NULL,
                   order_by = orderby_str,
                   limit = limit)
  r <- GET(url = base_url, query = qry_list) 
  if(!http_error(r)){ #FALSE = no error
    
    content(r, as="text") %>% 
      fromJSON() %>% 
      pluck("records", "record", "fields") %>%
      return()
  }
}

# testing ----
# 
# dataset <- "no2-tubes-raw"
# select_str <- "mid_date, siteid, concentration, dateon, dateoff"
# # groupby_str <- "siteid, year(date_time)"
# where_str <- ""
# where_str <- NULL
# date_on <-  "2020-01-01"
# date_off <-  "2021-01-03"
# date_on <-  NULL
# date_off <-  NULL
# date_col <-  "mid_date"
# refine <- ""
# samp_data <- getODSExport(date_col = date_col,
#                           dateon = date_on,
#                           dateoff = date_off,
#                           select_str = select_str,
#                           where_str = where_str,
#                           dataset = dataset,
#                           apikey = apikey,
#                           refine = refine)
# 
# samp_data[[2]]

# out <- getODSAggregate(select_str, where_str, groupby_str, dataset)
