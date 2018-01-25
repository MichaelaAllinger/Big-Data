#----------------------------#
# Install and load R Packages
#----------------------------#
install.packages(c("sparklyr", "dplyr", "ggplot2", "lubridate", "magrittr", "microbenchmark", "shiny", "leaflet"))
library(sparklyr)
library(dplyr)  
library(ggplot2)    
library(lubridate)  #adjust date and time
library(magrittr)   #piping (%>%)
library(microbenchmark) #benchmark code
library(shiny)      #shiny app
library(leaflet)    #plotting an interactive map

#----------------------------#
# Spark
#----------------------------#
#install spark
spark_install(version = "2.2.0", hadoop_version = "2.7")

#connect to spark
sc <- spark_connect(master = "local", version="2.2.0", hadoop_version="2.7")

#----------------------------#
# sparklyr native utilites
#----------------------------#
#explore available sparklyr native utilities
ls("package:sparklyr", pattern = "^sdf")  #spark data framework
ls("package:sparklyr", pattern = "^ft")   #feature transformation
ls("package:sparklyr", pattern = "^ml")   #machine learning algorithm

#----------------------------#
# data import
#----------------------------#
#import data with schema into Spark Data Frame
taxi_1 <- spark_read_csv(sc = sc, 
                         infer_schema = FALSE, 
                         name = "taxi_tbl", 
                         path = "data.csv", 
                         delimiter = ',',
                             columns = list(
                               id = "integer",
                               dropoff_latitude = "double",
                               dropoff_longitude = "double",
                               extra_charge = "double",
                               fare_amount = "double",
                               improvement_surcharge = "double",
                               mta_tax = "double",
                               passenger_count = "double",
                               payment_type = "double",
                               pickup_latitude = "double",
                               pickup_longitude = "double",
                               ratecodeid = "double",
                               store_and_fwd_flag = "string",
                               tip_amount = "double",
                               tolls_amount = "double",
                               total_amount = "double",
                               dropoff_datetime = "string",
                               pickup_datetime = "string",
                               trip_distance_miles = "double",
                               vendorid = "double"))

#----------------------------#
# check data
#----------------------------#
# list of all data frames stored in spark
src_tbls(sc) 

#check dataframe
class(taxi_1)

schema <- sdf_schema(taxi_1) %>% #check data types
  lapply(function(x) do.call(data_frame, x)) %>% #transform data types to data frame for more readability
  bind_rows()

schema

glimpse(taxi_1) #examine structure of data

print(taxi_1, n=5, width=Inf) #n = numbers of rows / width = numbers of columns (Inf = alle)

#----------------------------#
# Preprocessing
#----------------------------#
#select relevant columns
taxi_2 <- taxi_1 %>%
            select(dropoff_latitude, dropoff_longitude, extra_charge, fare_amount, improvement_surcharge, 
               mta_tax, passenger_count, pickup_latitude, pickup_longitude, ratecodeid, tip_amount, 
               tolls_amount, total_amount, dropoff_datetime, pickup_datetime, trip_distance_miles, vendorid)

glimpse(taxi_2)

#mutate columns
taxi_3 <- taxi_2 %>%
  mutate(trip_distance_km = trip_distance_miles * 1.60934, 
         tip_percentage = tip_amount/(total_amount - tip_amount)*100,
         amount = total_amount - tip_amount,
         pickup_day = as.double(dayofmonth(pickup_datetime)),
         pickup_hour = as.double(hour(pickup_datetime)),
         dropoff_day = as.double(dayofmonth(dropoff_datetime)),
         dropoff_hour = as.double(hour(dropoff_datetime))) %>%
  ft_binarizer("passenger_count", "group_ride", threshold = 1) %>% #create new variable to check for group_ride (ride with more than 1 passenger)
  select(-c(trip_distance_miles, pickup_datetime, dropoff_datetime)) %>% #eliminate columns
  arrange(pickup_day, pickup_hour)

glimpse(taxi_3)
print(taxi_3, n=10)

#remove outlier/missing values
taxi_4 <- taxi_3 %>%
  filter(tip_percentage<=100) %>%
  filter(improvement_surcharge>=0)

taxi_r <- taxi_4 %>%
  collect() #move data from spark to R
summary(taxi_r)

#----------------------------#
# Visualization in R Shiny
#----------------------------#

#select and collect relevant columns for plotting
taxi_map_dropoff <- taxi_4 %>%
  select(dropoff_latitude, dropoff_longitude, group_ride, tip_percentage) %>% collect

taxi_map_pickup <- taxi_4 %>%
  select(pickup_latitude, pickup_longitude, group_ride, tip_percentage) %>% collect

taxi_map_filtered_pickup <- taxi_4 %>%
  select(pickup_latitude, pickup_longitude, group_ride, tip_percentage) %>% 
  filter(tip_percentage > 50) %>%
  collect() 

scatterplot_data <- taxi_4 %>%
  select(tip_percentage, trip_distance_km, extra_charge,fare_amount, amount, tolls_amount, ratecodeid ) %>%
  collect()

#set color palette for plotting of the filtered map
pal = colorFactor(c("navy", "red"), domain=c(1,0))

# Define server logic ----
server = function(input, output) {
  
  output$pickup_Map <- renderLeaflet({
    leaflet(taxi_map_pickup) %>%
      addTiles() %>%  ##add graphic elements and layers to the map widget
      setView(-74.00, 40.71, zoom = 12) %>%
      addMarkers(~pickup_longitude,
                 ~pickup_latitude,
                 clusterOptions = markerClusterOptions())
  })  
  
  output$dropoff_Map <- renderLeaflet({
    leaflet(taxi_map_dropoff) %>%
      addTiles() %>%  
      setView(-74.00, 40.71, zoom = 12) %>%
      addMarkers(~dropoff_longitude,
                 ~dropoff_latitude,
                 clusterOptions = markerClusterOptions())
  })
  
  output$filtered_pickup_Map <- renderLeaflet({
    leaflet(taxi_map_filtered_pickup) %>%
      addTiles() %>%  
      setView(-74.00, 40.71, zoom = 12) %>%
      addCircleMarkers(~pickup_longitude,
                       ~pickup_latitude,
                       color=~pal(group_ride),
                       stroke = FALSE, fillOpacity = 0.5,
                       label=~paste0("tip percentage:", as.character(round(tip_percentage, digits=0)), "%")
      ) %>%
      addLegend(colors=c("navy", "red"), labels=c("group ride", "single ride"))
  })
  
  selectedData <- reactive({
    scatterplot_data[,c(input$xcol, input$ycol)]
  })
  
  output$scatterplots <- renderPlot({
    plot(selectedData())
  })
  
}  

# Define UI ----
ui <- fluidPage(
  # Application title
  titlePanel("NYC Taxi Rides"),

  # Showthe ggplot
  mainPanel(
    tabsetPanel(
      #tabPanel("ML",plotOutput("plot")),
      tabPanel("Map",
               fluidPage(
                 fluidRow(
                   column(6, leafletOutput("pickup_Map"),"Pickup Locations"),
                   column(6, leafletOutput("dropoff_Map"), "Dropoff Locations")
                 )
               )
      ),
      tabPanel("Filtered Map",
               fluidPage(
                 fluidRow(
                   column(6, leafletOutput("filtered_pickup_Map"), "Pickup Locations with Tip Percentage > 50%")
                 )
               )
      ),
      tabPanel("Scatterplot", 
               pageWithSidebar(
                 headerPanel("Scatterplots of Variables"),
                 sidebarPanel(
                   selectInput("xcol", "X Variable", names(scatterplot_data)),
                   selectInput("ycol", "Y Variable", names(scatterplot_data),
                               selected=names(scatterplot_data)[[2]])
                 ),
                 mainPanel(
                   plotOutput("scatterplots"))                     
               )
      )
    )
  )
)


# Run App
shinyApp(ui = ui, server = server)

#----------------------------#
# EDA
#----------------------------#

#calculate mean of passenger_count, tip_percentage, total_amount-tip_amount and trip_distance_km
taxi_4 %>% summarize(passenger_count_mean=mean(passenger_count),
                     tip_percentage_mean=mean(tip_percentage),
                     amount_mean=mean(total_amount-tip_amount),
                     trip_distance_km_mean=mean(trip_distance_km))

#calculate mean of tip_percentage grouped by ratecodeid
taxi_4 %>% group_by(ratecodeid) %>%
  summarize(tip_percentage_mean=mean(tip_percentage)) %>%
  arrange(desc(tip_percentage_mean))

#calculate mean of trip_distance_km grouped by ratecodeid
taxi_4 %>% group_by(ratecodeid) %>%
  summarize(trip_distance_km_mean=mean(trip_distance_km)) %>%
  arrange(desc(trip_distance_km_mean))

#count amount of factor levels ratecodeid 
taxi_4 %>% count(ratecodeid, sort=TRUE) 

#--------------------------------------#
# Machine Learning
# regression to predict tip_percentage
#--------------------------------------#
#eliminate columns 
taxi_5 <- na.omit(taxi_4)

#split data into 70% train/ 30% test 
partitions <- taxi_5 %>%
  sdf_partition(train = 0.7, test = 0.3)

sdf_dim(partitions$train)
sdf_dim(partitions$test)

#extract feature columns
feature_colnames <- partitions$train %>%
  select(-c(tip_percentage, passenger_count, tip_amount, total_amount, dropoff_day, dropoff_day, fare_amount)) %>%
  colnames() 

#lineare regression 
linear_regression <- partitions$train %>%
  ml_linear_regression(response="tip_percentage", features=feature_colnames, na.rm=TRUE)
options(scipen=999)
summary(linear_regression)
pred_lr <- sdf_predict(linear_regression, partitions$test)

residuals_lr <- pred_lr %>%
  mutate(residuals_lr = prediction - tip_percentage)
rmse_lr <- residuals_lr %>% 
  summarize(rmse=sqrt(mean(residuals_lr^2)))

#gradient boosted trees
gradient_boosted_trees <- partitions$train %>%
  ml_gradient_boosted_trees(response="tip_percentage", features=feature_colnames)
summary(gradient_boosted_trees)
pred_gbt <- sdf_predict(gradient_boosted_trees, partitions$test)

residuals_gbt <- pred_gbt %>%
  mutate(residuals_gbt = prediction - tip_percentage)
rmse_gbt <- residuals_gbt %>% 
  summarize(rmse=sqrt(mean(residuals_gbt^2)))

#random forest
random_forest <- partitions$train %>%
  ml_random_forest(response="tip_percentage", features = feature_colnames)
summary(random_forest)
pred_rf <- sdf_predict(random_forest, partitions$test)

residuals_rf <- pred_rf %>%
  mutate(residuals_rf = prediction - tip_percentage)
rmse_rf <- residuals_rf %>% 
  summarize(rmse=sqrt(mean(residuals_rf^2)))

# end spark session
spark_disconnect(sc)
