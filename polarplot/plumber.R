library("here")

source(here("polarplot", "import_ODS.R"))
source(here("polarplot", "polar_plot_api.r"))

library(plumber)
#source("polar_plot_api.r")
#* @apiTitle polar plot
#* @apiDescription Polar plot of pollution data


#* @param date_on the start date
#* @param date_off the end date
#* @param sensor_id the sensor_id
#* @param pollutant the pollutant
#* @get /polarplot
#* @serializer png list(type='image/png')
function(date_on, date_off, sensor_id, pollutant){
    if(dates_ok(date_on, date_off)){
        joined_tbl <- get_data(date_on, date_off, sensor_id)
        if(is.data.frame(joined_tbl) & 
           nrow(joined_tbl > 24) &
       pollutant %in% c("pm2.5", "pm10")){
         out <- polar_plot(joined_tbl, pollutant)
        } else {
            out = "No data to plot: check sensor ID and pollutant name"
        }
    } else {
        out = "Cannot plot: problem with dates"
    }
    
    return(out)
        
}
