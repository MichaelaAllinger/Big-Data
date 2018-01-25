#----------------------------#
# Install and load R Packages
#----------------------------#
install.packages(c("httr", "jsonlite"))
library(httr)
library(jsonlite)

#----------------------------#
# Request
#----------------------------#
#define url and path to API
url  <- "https://data.cityofnewyork.us"
path <- "/resource/2yzn-sicd.json?$limit=50000&payment_type=1" 

#Execute an API call with GET 
raw_result <- GET(url = url, path = path)

#----------------------------#
# Response
#----------------------------#
#check result
class(raw_result)
names(raw_result)
raw_result$status_code 
"
- check if the call worked network-wise (server recieved our request)
- BUT: don't know yet if it was valid for the API or found any data
- see https://en.wikipedia.org/wiki/List_of_HTTP_status_codes for possible codes"
head(raw_result$content)

raw_content <- rawToChar(raw_result$content) #translate unicode into text (contains JSON file)

nchar(raw_content) #check how large raw_content is in terms of characters
substr(raw_content, 1, 1000) #look at first 100 characters

data <- fromJSON(raw_content) #parse JSON file into dataframe

write.csv(data, "taxi2015.csv")

#source: https://www.r-bloggers.com/accessing-apis-from-r-and-a-little-r-programming/