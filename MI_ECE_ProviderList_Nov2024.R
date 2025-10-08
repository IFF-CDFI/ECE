# I am getting data from 6 different sources, gsq_nov24, lara_cap, lara_activ (age ranges, license status), cdc, gsrp, hs  
# update on Sep 


# if(!require(leaflet)) install.packages("leaflet") 
library(readxl)
library(dplyr)
library(stringr)
library(fuzzyjoin)
library(stringdist)
library(tidyverse)
library(data.table) # %ilike%
library(tidygeocoder)
library(usethis)
library(leaflet)
library(sf)
library(tigris)
options(tigris_use_cache = TRUE)

save.image("E:/2024_Nov27_Received/Processed Data/miProvNov2024.RData")
load("E:/2024_Nov27_Received/Processed Data/miProvNov2024.RData")

## GSQ data from MI DSA 
# 7917 prov
gsq_nov24 = read_excel("E:/2024_Nov27_Received/Raw Data/IFF GSQ Data 11.26.24.xlsx", col_names = T, trim_ws = T)
names(gsq_nov24)
# [1] "Record ID"            "Region"               "Status"               "Status Reason"        "Status Date"          "Status Note"          "Referral Status"     
# [8] "Reason"               "Referral Status Date" "Referral Status Note" "County"               "Provider Type"        "Program Types"        "License"             
# [15] "Business Name"        "ProviderID"           "Contact...17"         "First Name"           "Last Name"            "Address"              "City"                
# [22] "State"                "Zip"                  "Latitude"             "Longitude"            "Phone"                "Primary Email"        "Login Email"         
# [29] "Expired"              "Expired Date"         "Contact...31"         "Cell Phone for Text"  "Publish Rating" 

unique(gsq_nov24$Status) # all providers seem to be in active status
# [1] "Active"

# manually correct this lic no.
gsq_nov24[gsq_nov24$License=="tribalc1194076", "License"] <- "TribalC1194076"



## LARA/CCLB data
# 7896 prov - more updated list from Nov 27, 2024 
lara_cap = read_excel("E:/2024_Nov27_Received/Raw Data/MiLEAP CCLB Active Accounts (Capacity) - 20241127.xlsx", col_names = T, trim_ws = T)
names(lara_cap)
# [1] "License Number"               "Facility Name: Facility Name" "License Type"                 "Facility address"             "County"                      
# [6] "Capacity"                     "Effective Date"               "Expiration Date"              "License Status"

table(lara_cap$`License Status`, exclude = NULL)
# Active = 7896
length(unique(lara_cap$`License Number`)) # 7896 of 7896 unique

# Function to clean and format the address
format_address <- function(address) {
  # Replace the first <br> with ", "
  address <- str_replace(address, "<br>", ", ")
  # Remove the second occurrence of ", , <br>,"
  address <- str_replace(address, ", , <br>,", "")
  # Remove "United States"
  address <- str_replace(address, "United States", "")
  # Remove any remaining occurrence of ", , <br>,"
  address <- str_replace(address, ", , <br>,", "")
  # Remove any remaining occurrence of ", , <br>US,"
  address <- str_replace(address, ", , <br>US,", "")
  # Trim any leading or trailing whitespace
  address <- str_trim(address)
  return(address)
}

# Apply the formatting function to the address column
lara_cap <- lara_cap %>%
  mutate(formatted_address = sapply(`Facility address`, format_address))


# check inner join between lara_cap and gsq - 7891 of 7896 matched (5 not matched)
tmp = inner_join(lara_cap, gsq_nov24, join_by(x$"License Number" == y$License))

# join lara_cap to gsq = 7917
gsq_laraCap = left_join(gsq_nov24[, c("License", "Business Name", "Provider Type", "Publish Rating", "Program Types", "Address", "City", 
                                      "Zip", "County", "Latitude", "Longitude" )]
                        , lara_cap[, c("License Number", "Capacity")]
                        , join_by(x$License == y$"License Number"))


# 7938 prov of which 28 closed during Nov 15-27, 2024
lara_activ = read_excel("E:/2024_Nov27_Received/Raw Data/MiLEAP CCLB Active Accounts - 20241113.xlsx", col_names = T, trim_ws = T)
names(lara_activ)
# [1] "Business License"                 "Facility Name"                    "Facility Address"                 "City"                            
# [5] "Facility County"                  "Zip/Postal Code"                  "License Type"                     "Age Ranges"                      
# [9] "Business License Effective Date"  "Business License Expiration Date" "Business License Status"          "Start Month"                     
# [13] "End Month"                        "Full or Part Time Care"           "Minimum Age"                      "Maximum Age"                     
# [17] "Facility Close Date" 

# Function to extract street address
format_address <- function(address) {
  # Replace the first <br> with ", "
  address <- str_split(address, ",")[[1]][1]
  # Trim any leading or trailing whitespace
  address <- str_trim(address)
  return(address)
}

# Apply the formatting function to the address column
lara_activ <- lara_activ %>%
  mutate(formatted_address = sapply(`Facility Address`, format_address),
         formatted_address = paste(formatted_address, City, `Zip/Postal Code`, sep = ", "),
         formatted_address = sub(",\\s*(\\d{5})$", ", MI \\1", formatted_address))


# check inner join between lara_activ and gsq_laraCap - 7901 of 7938 matched (37 not matched)
tmp = inner_join(lara_activ, gsq_laraCap, join_by(x$`Business License` == y$License))

# join lara_activ to gsq_laraCap - 7917 prov
gsq_laraCapActiv = left_join(gsq_laraCap, lara_activ[, c("Business License", "Business License Status", "Age Ranges", 
                                                         "Full or Part Time Care", "Minimum Age", "Maximum Age")]
                             , join_by(x$License == y$"Business License"))

# lara_activ that didn't join - 37 - most are closed/suspended/revoked except for 5 original - matches the laraCap_nojoin
laraActiv_nojoin = lara_activ[!lara_activ$`Business License` %in% gsq_laraCap$License, c("Business License", "Facility Name", "formatted_address", "Business License Status", "Age Ranges", 
                                                                                         "Full or Part Time Care", "Minimum Age", "Maximum Age")]

# 5 prov that are in both LARA_CAP & LARA_ACTIV but not in the GSQ data. We should not attach the no-join data from LARA_ACTIV because they do not have 
# any capacity information. The 37 prov in laraActiv_nojoin are all closed/suspended/revoked licenses except the 5 original that are already in laraCap_nojoin 
# No need to append laraActiv_nojoin but join its attributes to laraCap_nojoin
# lara_cap that didn't join - 5
laraCap_nojoin = lara_cap[!lara_cap$`License Number` %in% gsq_nov24$License, c("License Number", "Facility Name: Facility Name", "formatted_address", "Capacity")]

# get attributes from lara_activ
laraCap_nojoin <- laraCap_nojoin %>%
  left_join(lara_activ[, c("Business License", "License Type", "Business License Status", "Age Ranges", "Full or Part Time Care", "Minimum Age", "Maximum Age")]
            , join_by(x$`License Number` == y$`Business License`)) 

# check the join
head(laraCap_nojoin)


#### CDC from Nov 2024 - 4101 prov
# some error here: "Count of Children Age 6" is that actual total Child Count. Do not use the field, "Total Child Count" 
cdc = read_excel("E:/2024_Nov27_Received/Raw Data/CDC Data Report - IFF Data as of 11.13.24.xlsx", col_names = T, trim_ws = T)
names(cdc)
# [1] "Provider ID"                "Registered License Number"  "Provider Type"              "Provider/Organization Name" "Other Last Name"           
# [6] "DHS Person ID"              "Address Line 1"             "Address Line 2"             "Zip +4"                     "County"                    
# [11] "Count of Children Age 0"    "Count of Children Age 1"    "Count of Children Age 2"    "Count of Children Age 3"    "Count of Children Age 4"   
# [16] "Count of Children Age 5"    "Count of Children Age 6"    "Total Child Count"

length(unique(cdc$`Registered License Number`)) # 4097

# convert the count fields to numeric
cdc = cdc %>% mutate_at(c("Count of Children Age 0","Count of Children Age 1","Count of Children Age 2","Count of Children Age 3","Count of Children Age 4","Count of Children Age 5","Count of Children Age 6"), as.numeric)

# calculate children in 0-2 and 3-5 ages 
cdc$CapSTSub02 = apply(cdc[, c("Count of Children Age 0","Count of Children Age 1","Count of Children Age 2")], 1, sum, na.rm=T)
cdc$CapSTSub35 = apply(cdc[, c("Count of Children Age 3","Count of Children Age 4","Count of Children Age 5")], 1, sum, na.rm=T)

# clean up address fields
cdc1 = cdc %>%
  mutate(StreetAddress = trimws(`Address Line 1`),
         City = str_extract(trimws(str_to_title(`Address Line 2`)), "^[^,]+"),
         Zip = str_sub(trimws(`Address Line 2`), -5),
         formatted_address = paste(StreetAddress, City, Zip, sep = ", "),
         formatted_address = sub(",\\s*(\\d{5})$", ", MI \\1", formatted_address)) %>%
  select(-c(`Address Line 1`, `Address Line 2`))

# check dupes in cdc1$License - found 4
dup = cdc1[duplicated(cdc1$`Registered License Number`),]
# remove row with "369 W MORRELL ST" because the other address matches the one in gsq_laraCapActiv
# remove row with CapSTSub35 = 100 because it is larger than the total capacity in gsq_laraCapActiv
# remove row with "2946 SUTTON RD" because the other address matches the one in gsq_laraCapActiv
# remove row with CapSTSub35 = 2 because the other row matches the total capacity in gsq_laraCapActiv
index = cdc1$StreetAddress %in% c("369 W MORRELL ST", "2946 SUTTON RD") 
tmp = cdc1[index,] # ensure that the rows are correct
cdc1 = cdc1[!index,]
index = (cdc1$`Registered License Number` == "DC730409849" & cdc1$CapSTSub35 == 100) | (cdc1$`Registered License Number` == "DF820384793" & cdc1$CapSTSub35 == 2)
tmp = cdc1[index,] # ensure that the rows are correct
cdc1 = cdc1[!index,]
# final cdc1 rows = 4097

# check inner join between cdc and gsq_laraCapActiv - 3815 of 4097 matched (282 not matched)
tmp = inner_join(cdc1, gsq_laraCapActiv, join_by(x$"Registered License Number" == y$"License"))

# final join with cdc = 7921 prov
cdc_join = left_join(gsq_laraCapActiv, cdc1[,c("Registered License Number", "CapSTSub02", "CapSTSub35")], join_by(x$"License" == y$"Registered License Number")) 

# cdc that didn't join - 282
cdc_nojoin = cdc1[!cdc1$`Registered License Number` %in% gsq_laraCapActiv$License
                  , c("Registered License Number", "Provider/Organization Name", "Provider Type", "formatted_address", "CapSTSub02", "CapSTSub35")]


##### GSRP from Jan 2025 - 1535 prov but only 872 had enrollment estimates
# revised version in a different tab of the same excel file
# gsrp = read_excel("E:/2024_Nov27_Received/Raw Data/GSRP Jan 25 2025 Fall for IFF 040225.xlsx", sheet = "Final for IFF 040225 (2)", col_names = T, trim_ws = T)
# names(gsrp)
# # [1] "Sort Code"                            "Fiscal Agent"                         "License #"                           
# # [4] "Tribal #"                             "Sub-Contractor/Sub-Recipient"         "Facility Name"                       
# # [7] "Street Address"                       "City"                                 "Zip Code"                            
# # [10] "County"                               "Total Accepted Funds"                 "Total Accepted Children to be Served"
# 
# # copy the tribal lic id to the license id column 
# gsrp <- gsrp %>%
#   mutate(`License #` = ifelse(is.na(`License #`), `Tribal #`, `License #`))

#### using GSRP data from Aug 2024 because the above dataset does not have enrollment estimates per license ID - the enrollment is only per sub-recipient 
gsrp_aug2024 = read_excel("E:/2024_Aug22_Received/Raw Data/GSRP May 11 2024 Spring for IFF (08.29.24).xlsx", col_names = T, trim_ws = T)
names(gsrp_aug2024)
# [1] "License #"         "Actual Enrollment" "Fiscal Agent"      "Site Name"         "Street Address"    "City"             
# [7] "Zip Code"          "County" 

# 1313 GSRP providers
table(is.na(gsrp_aug2024$`Actual Enrollment`))
# FALSE 
# 1313 

# trim spaces in lic id
gsrp_aug2024$`License #` = trimws(gsrp_aug2024$`License #`)

length(unique(gsrp_aug2024$`License #`)) # 1312 unique lic ids of 1313 prov
length(unique(gsrp_aug2024$`Street Address`)) # 1285 of 1313 addresses

# check which lic id is duplicate
dup = gsrp_aug2024[duplicated(gsrp_aug2024$`License #`),]
# remove the row with address = "450 n Hibbard" because the other one has "450 N Hibbard"
gsrp_aug2024 = gsrp_aug2024[!gsrp_aug2024$`Street Address` == "450 n Hibbard",]

# check inner join between gsrp_aug2024 and cdc_join - 1265 of 1312 prov matched - 47 not joined
tmp = inner_join(gsrp_aug2024, cdc_join, join_by(x$"License #" == y$License))

# final join with gsrp_aug2024 - 7917 
gsrp_cdc_join = left_join(cdc_join, gsrp_aug2024[,c("License #", "Actual Enrollment")], join_by(x$License == y$"License #")) 
# check the join
table(is.na(gsrp_cdc_join$"Actual Enrollment")) # 1265 are not NA => 1265 gsrp prov joined

# GSRP lic ids that did not join - 47
gsrp_aug2024$`formatted_address` = paste0(gsrp_aug2024$`Street Address`, ", ", gsrp_aug2024$City, ", MI ", gsrp_aug2024$`Zip Code`) 
gsrp_nojoin = gsrp_aug2024[!gsrp_aug2024$`License #` %in% cdc_join$License, c("License #", "Site Name", "formatted_address", "Actual Enrollment")]


############ HeadStart data - 777 prov - no license ids
hs = read_excel("E:/2024_Nov27_Received/Raw Data/Head Start Centers Report IFF 11-01-24.xlsx", col_names = T, trim_ws = T)
names(hs)
# [1] "Grantee Name"    "Program/r/nType" "Program"         "Center Name"     "Address Line 1"  "Address Line 2"  "City"            "State"           "ZIP"             "ZIP+4"           "County"         
# [12] "Total Slots"
names(hs)[2] = "ProgramType"

# the EHS and HS slot data are in rows - need them as different columns for the same center 
length(unique(hs$`Address Line 1`)) # 576 of 777 are unique => multiple rows for the same street address but for different EHS, HS programs 

hs_wide <- hs %>% # 596
  pivot_wider(
    # need to include center name because there are different center names with different slots at the same address
    # maybe don't need program names because they seem to be many duplicates
    id_cols = c("Center Name", "Address Line 1", "City", "ZIP", "County"), 
    names_from = c("ProgramType"),
    values_from = "Total Slots",
    values_fill = 0,
    values_fn = sum # merging the slots from duplicate program names
  )

# make these manual corrections in hs_wide - need to call up and confirm?
# Allocate 17 to HS to the center, "Bright Beginnings" and delete the center, "BRIGHT BEGINNINGS" - no response to call
hs_wide[hs_wide$`Center Name` == "Bright Beginnings", "HS"] <- 17
hs_wide = hs_wide[!hs_wide$`Center Name` == "BRIGHT BEGINNINGS",]
# Delete "Livingword EHS" because there is a "Living Word EHS" at the same address with the same no. of seats - also confirmed via phone-call
hs_wide = hs_wide[!hs_wide$`Center Name` == "Livingword EHS",]

## join program name to hs_wide - have to do this step because including it as id_col led to duplicate rows
# using this to avoid the duplicate program names: multiple = "first" 
# 594 records
hs1 = left_join(hs_wide, hs[, c("Program", "Address Line 1")], join_by(x$"Address Line 1" == y$"Address Line 1"), multiple = "first") 

# some more manual corrections after checking the map
hs1[hs1$`Address Line 1`== "M-20 5210 One Mile Red", "Address Line 1"] = "5210 W 1 Mile Rd"
hs1[hs1$`Address Line 1`== "244 S Benzonia Blvd", "Address Line 1"] = "244 S Benzie Blvd"
# other home bases too - keep?
# hs1 = hs1[!hs1$`Center Name` == "Home Base",] # "1148 Harper" 

names(hs1)
# [1] "Center Name"    "Address Line 1" "City"           "ZIP"            "County"        
# [6] "HS"             "EHS"            "AIAN EHS"       "AIAN HS"        "Migrant HS"    
# [11] "Migrant EHS"    "Program"   

length(unique(hs1$"Address Line 1")) # 576 of 594

# check duplicates - 18
hsDup = hs1[duplicated(hs1$"Address Line 1"),]

# merge on address - center names can be different - separating them by commas
hs1_merged = hs1 %>%
  group_by(`Address Line 1`) %>%
  summarise(
    CenterName = paste(`Center Name`, collapse = ", "),
    across(c("HS","EHS","AIAN EHS","AIAN HS","Migrant HS","Migrant EHS"), ~ sum(.x, na.rm=TRUE)),
    .groups = "drop"
  )

# joining the additional fields - 576 prov (unique address)
hs2 = left_join(hs1_merged, hs1[,c("Address Line 1","City","ZIP","County","Program")], join_by(x$`Address Line 1` == y$`Address Line 1`), multiple = "first")

# check duplicates - 0
hsDup = hs2[duplicated(hs2$`Address Line 1`),]

### try joining based on uniq id (street address (6 char), city (4 char) and zip (5 char))
# excluding the center name from the equation because HS data is using generic names instead of the actual provider names for the centers
# create uniq ids
hs2$uniqId = paste0(substr(hs2$`Address Line 1`,1,6), substr(hs2$City,1,4), substr(hs2$ZIP,1,5))
gsrp_cdc_join$uniqId = paste0(substr(gsrp_cdc_join$Address,1,6), substr(gsrp_cdc_join$City,1,4), substr(gsrp_cdc_join$Zip,1,5))
################## check why some uniq ids not matching - should have only 4 char from zip 

# check inner join between hs2 (unique address) and gsrp_cdc_join - 501 of 576 matched - 75 not matched
tmp = inner_join(hs2, gsrp_cdc_join, join_by(x$uniqId == y$uniqId)) 
length(unique(tmp$uniqId)) # 397 of 501 unique addresses 


# final join with hs2 - 7917 prov & 28 fields
names(gsrp_cdc_join)[2] = "Facility Name"
hs_gsrp_cdc_join = left_join(gsrp_cdc_join, hs2[,c("uniqId","CenterName","Program","HS","EHS","AIAN EHS","AIAN HS","Migrant HS","Migrant EHS")], join_by(x$uniqId == y$uniqId)) 
hs_gsrp_cdc_join$uniqId <- NULL

# check the join
table(is.na(hs_gsrp_cdc_join$CenterName)) # 501 are not NA => 501 gsrp prov joined

# HS that did not join - 179
hs2$`formatted_address` = paste0(hs2$`Address Line 1`, ", ", hs2$City, ", MI ", hs2$ZIP) 
hs_nojoin = hs2[!hs2$uniqId %in% gsrp_cdc_join$uniqId, c("CenterName","formatted_address","HS","EHS","AIAN EHS","AIAN HS","Migrant HS","Migrant EHS")]


######### append the nojoin data & geocode
## need to match names of the nojoin files with each other & with the base file 

names(hs_gsrp_cdc_join)
# [1] "License"                 "Facility Name"           "Provider Type"           "Publish Rating"         
# [5] "Program Types"           "Address"                 "City"                    "Zip"                    
# [9] "County"                  "Latitude"                "Longitude"               "Capacity"               
# [13] "Business License Status" "Age Ranges"              "Full or Part Time Care"  "Minimum Age"            
# [17] "Maximum Age"             "CapSTSub02"              "CapSTSub35"              "Actual Enrollment"      
# [21] "CenterName"              "Program"                 "HS"                      "EHS"                    
# [25] "AIAN EHS"                "AIAN HS"                 "Migrant HS"              "Migrant EHS" 

# lara_cap that didn't join - 5
names(laraCap_nojoin) 
# [1] "License Number"               "Facility Name: Facility Name" "formatted_address"            "Capacity"                    
# [5] "License Type"                 "Business License Status"      "Age Ranges"                   "Full or Part Time Care"      
# [9] "Minimum Age"                  "Maximum Age" 
names(laraCap_nojoin)[c(1,2,5)] = c("License", "Facility Name", "Provider Type")
# update the Provider Types to match the GSQ ones
laraCap_nojoin <- laraCap_nojoin %>%
  mutate(`Provider Type` = recode(`Provider Type`, 
                         "Family Home" = "Licensed Family Homes",  # Replace "Family Home" with "Licensed Family Homes"
                         "Center" = "Licensed Centers"))


# lara_activ that didn't join - 37 - but no need to append this

# cdc that didn't join - 282
names(cdc_nojoin) 
# [1] "Registered License Number"  "Provider/Organization Name" "Provider Type"             
# [4] "formatted_address"          "CapSTSub02"                 "CapSTSub35" 
names(cdc_nojoin)[1:3] = c("License", "Facility Name", "Provider Type")
# recode Provider Type: LD = center, LG = group home, RF = family home
cdc_nojoin <- cdc_nojoin %>%
  mutate(`Provider Type` = recode(`Provider Type`, 
                                  "RF" = "Licensed Family Homes",  
                                  "LD" = "Licensed Centers",
                                  "LG" = "Licensed Group Homes"
  ))

# GSRP lic ids that did not join - 47
names(gsrp_nojoin) 
# [1] "License #"         "Site Name"         "formatted_address" "Actual Enrollment"
names(gsrp_nojoin)[1] = "License"
# GSRP does not have Provider Type. Get info. from License # - has only centers
index = grepl("DC", gsrp_nojoin$License)
gsrp_nojoin[index, "Provider Type"] = "Licensed Centers"


# HS that did not join - 179
names(hs_nojoin) 
# [1] "CenterName"        "formatted_address" "HS"                "EHS"              
# [5] "AIAN EHS"          "AIAN HS"           "Migrant HS"        "Migrant EHS" 
names(hs_nojoin)[1] = "Facility Name"

# list of all nojoin files - 511
all_nojoin = dplyr::bind_rows(laraCap_nojoin, cdc_nojoin, gsrp_nojoin, hs_nojoin)
# check fieldnames
names(all_nojoin)
# [1] "License"                 "Facility Name"           "formatted_address"       "Capacity"               
# [5] "Provider Type"           "Business License Status" "Age Ranges"              "Full or Part Time Care" 
# [9] "Minimum Age"             "Maximum Age"             "CapSTSub02"              "CapSTSub35"             
# [13] "Site Name"               "Actual Enrollment"       "HS"                      "EHS"                    
# [17] "AIAN EHS"                "AIAN HS"                 "Migrant HS"              "Migrant EHS" 



############## Do geocoding using Geocodio
# For geocodio API - one-time
usethis::edit_r_environ()
CENSUS_API_KEY='5296ee302db4c2942eb520d01ac55f84c98f6d29'
GEOCODIO_API_KEY='311ccde68138767912de670c011de2738201118'
# ☐ Modify C:/Users/prao/OneDrive - IFF/Documents/.Renviron.
# ☐ Restart R for changes to take effect.

### apply geocoding function to all_nojoin
length(unique(all_nojoin$formatted_address)) # 513 of 513
all_nojoin_address <- data.frame(address = all_nojoin$formatted_address)
all_nojoin_address_geo = all_nojoin_address %>% 
  geocode(address, method = "geocodio", full_results = TRUE)
# check how many have an accuracy score of < 0.8
index = all_nojoin_address_geo$accuracy < 0.8; table(index) # 26 of 513 (5%)
# join lat-lon to all_nojoin
all_nojoin_geo = left_join(all_nojoin, all_nojoin_address_geo[, c("address", "lat", "long", "accuracy", "accuracy_type")], join_by(x$formatted_address == y$address))

names(all_nojoin_geo)
# [1] "License"                 "Facility Name"           "formatted_address"      
# [4] "Capacity"                "Provider Type"           "Business License Status"
# [7] "Age Ranges"              "Full or Part Time Care"  "Minimum Age"            
# [10] "Maximum Age"             "CapSTSub02"              "CapSTSub35"             
# [13] "Site Name"               "Actual Enrollment"       "HS"                     
# [16] "EHS"                     "AIAN EHS"                "AIAN HS"                
# [19] "Migrant HS"              "Migrant EHS"             "lat"                    
# [22] "long"                    "accuracy"                "accuracy_type"


######### append the geocoded nojoin data to the base file

# base file
names(hs_gsrp_cdc_join)
# [1] "License"                 "Facility Name"           "Provider Type"           "Publish Rating"         
# [5] "Program Types"           "Address"                 "City"                    "Zip"                    
# [9] "County"                  "Latitude"                "Longitude"               "Capacity"               
# [13] "Business License Status" "Age Ranges"              "Full or Part Time Care"  "Minimum Age"            
# [17] "Maximum Age"             "CapSTSub02"              "CapSTSub35"              "Actual Enrollment"      
# [21] "CenterName"              "Program"                 "HS"                      "EHS"                    
# [25] "AIAN EHS"                "AIAN HS"                 "Migrant HS"              "Migrant EHS" 

# match fieldnames with all_nojoin_geo
names(hs_gsrp_cdc_join)[10:11] = c("lat", "long")

# 8430 prov & 32 fields
mi_prov_2024 = dplyr::bind_rows(hs_gsrp_cdc_join, all_nojoin_geo)

# check fieldnames
# "License","Facility Name","Address","City","County","State","Zip","Provider Type","AgeGroup","Publish Rating",
# "Cap02","Cap35","Cap05","CapSTSub02","CapSTSub35","CapSTSub05","EHS","HS","CapHS05","AIAN EHS","AIAN HS",
# "CapAianHS05","Migrant EHS","Migrant HS","CapMigrHS05","Total Accepted Children to be Served","lat","long",
# "accuracy","accuracy_type"

######### calculate missing fields
# "AgeGroup","Cap02","Cap35","Cap05","CapSTSub05","CapHS05","CapAianHS05","CapMigrHS05"

# In the fields, "Minimum Age" & "Maximum Age", 1) replace "Birth" with "0 years", 2) replace "2 ½ years" with "2.5 years", 
# and 3) replace "Six Months" with "6 months"

# replace the fields, "birth", "2 ½ years", "six months" so that they can be converted into months 
mi_prov_2024 <- mi_prov_2024 %>%
  mutate(across(c(`Minimum Age`, `Maximum Age`), ~ case_when(
    str_to_lower(.) == "birth" ~ "0 years",
    str_to_lower(.) == "2 ½ years" ~ "2.5 years",
    str_to_lower(.) == "six months" ~ "6 months",
    TRUE ~ .
  )))

# Function to convert `Minimum Age` & `Maximum Age` to monthsFrom & monthsTo
convert_to_months <- function(duration) {
  # Extract years and months
  years <- str_extract(duration, "\\d+(\\.\\d+)?(?=\\s*years)") %>% 
    as.numeric() %>% 
    replace_na(0) * 12  # Convert years to months
  
  months <- str_extract(duration, "\\d+(?=\\s*months)") %>% 
    as.numeric() %>% 
    replace_na(0)  # Extract months
  
  return(years + months)  # Return total months
}

# Apply the function to the `Minimum Age` & `Maximum Age` columns in the dataframe, mi_prov_2024
mi_prov_2024 <- mi_prov_2024 %>%
  mutate(monthsFrom = sapply(`Minimum Age`, convert_to_months),
         monthsTo = sapply(`Maximum Age`, convert_to_months))


# confirm
tmp = mi_prov_2024[, c("Minimum Age", "Maximum Age", "monthsFrom", "monthsTo")]

# Add age group multipliers
# classify 0-36 months as 0-2, 36-72 months as 3-5, 0-72 months as 0-5
mi_prov_2024 = mi_prov_2024 %>%
  mutate(AgeGroup = case_when(
    monthsFrom >= 0 & monthsTo < 36 ~ "0 to 2",
    monthsFrom >= 36 & monthsTo <= 72 ~ "3 to 5",
    monthsFrom >= 0 & monthsTo <= 72 ~ "0 to 5",
    is.na(monthsFrom) & is.na(monthsTo) ~ "0 to 5",
    monthsFrom == 0 & monthsTo == 0 ~ "0 to 5",
    TRUE ~ "0 to 5" # for every other condition
    )) %>%
  mutate(Cap05 = Capacity) %>%
  mutate(Cap02 = case_when(
    AgeGroup == "0 to 5" & `Provider Type` %in% c("Licensed Centers", "Center") ~ Capacity * .31,
    AgeGroup == "0 to 5" & `Provider Type` %in% c("Licensed Group Homes", "Licensed Family Homes", "Family Home") ~ Capacity * .46,
    AgeGroup == "0 to 2" ~ Capacity,
    AgeGroup == "3 to 5" ~ 0
  )) %>%
  mutate(Cap35 = case_when(
    AgeGroup == "0 to 5" & `Provider Type` %in% c("Licensed Centers", "Center") ~ Capacity * .69,
    AgeGroup == "0 to 5" & `Provider Type` %in% c("Licensed Group Homes", "Licensed Family Homes", "Family Home") ~ Capacity * .54,
    AgeGroup == "0 to 2" ~ 0,
    AgeGroup == "3 to 5" ~ Capacity
  ))

# confirm
tmp = mi_prov_2024[, c("Minimum Age", "Maximum Age", "monthsFrom", "monthsTo", "Provider Type", "AgeGroup", "Cap02", "Cap35", "Cap05")]
table(tmp$AgeGroup)
# 0 to 2    0 to 5    3 to 5 
# 552       7394      484
tmp1 = tmp[tmp$AgeGroup == "3 to 5",]
table(tmp1$`Minimum Age`, tmp1$`Maximum Age`)
#         4 years 5 years 6 years
# 3 years       8     262     131
# 4 years       0      78       5
table(tmp1$monthsFrom, tmp1$monthsTo)
#     48  60  72
# 36   8 262 131
# 48   0  78   5

# calculate fields, "CapSTSub05","CapHS05","CapAianHS05","CapMigrHS05"
mi_prov_2024 = mi_prov_2024 %>%
  mutate(CapSTSub05 = CapSTSub02 + CapSTSub35,
         CapHS05 = EHS + HS,
         CapAianHS05 = `AIAN EHS` + `AIAN HS`,
         CapMigrHS05 = `Migrant EHS` + `Migrant HS`)

# confirm
tmp = mi_prov_2024[, c("CapHS05", "EHS", "HS", "CapAianHS05", "AIAN EHS", "AIAN HS", "CapMigrHS05", "Migrant EHS", "Migrant HS")]


###################################
# data filtering - happening on GSQ & LARA_ACTIV fields

######## filter out school age, inactive licenses, min age gte 6 yrs
### remove school-age program types in GSQ 
table(mi_prov_2024$`Program Types` %ilike% "School-age") # 522 if %in% "School-age"
# 552

# remove 1, 2, 5 to remove school-age
unique(mi_prov_2024[mi_prov_2024$`Program Types` %ilike% "School-age", "Program Types"])
# 1 School-age                                   
# 2 School-age,Faith-based                       
# 3 GSRP-Great Start Readiness Program,School-age
# 4 Early Head Start,School-age                  
# 5 School-age,Montessori

# 8430 prov -> removed 526 programs -> 7904 prov 
# actual enrollment is the GSRP enrollment
mi_prov_2024 = mi_prov_2024 %>% filter(! `Program Types` %in% c("School-age", "School-age,Montessori", "School-age,Faith-based"))

############ check for duplicate license IDs & merge them
length(unique(mi_prov_2024$License)) # 7715 of 7904 (incl. 179 blank) unique
dup=mi_prov_2024[duplicated(mi_prov_2024$License),] # 189 
table(is.na(mi_prov_2024$License)) # 179 NAs
tmp = mi_prov_2024[mi_prov_2024$License %in% dup$License,] # 201
index = is.na(tmp$License)
tmp = tmp[!index,] # 22 after removing the blank License IDs
# merge these 22 rows based on license IDs
dupLic = unique(tmp$License) # 11 Lic Ids

# Function to check if a string is title case
is_title_case <- function(x) {
  words <- str_split(x, "\\s+")[[1]]
  all(str_detect(words, "^[A-Z][a-z]+.*$"))
}

# Function to pick the preferred address
preferred_address <- function(addresses) {
  addresses <- addresses[!is.na(addresses)]
  if (length(addresses) == 0) return(NA)
  
  title_case_flags <- sapply(addresses, is_title_case)
  
  if (any(title_case_flags)) {
    candidates <- addresses[title_case_flags]
    candidates[which.max(nchar(candidates))]
  } else {
    addresses[which.max(nchar(addresses))]
  }
}

# Merge rows on licence ids 
dupLic_merged <- tmp %>%
  group_by(License) %>%
  summarise(across(everything(), ~ {
    if (cur_column() == "formatted_address") {
      preferred_address(.x)
    } else if (is.numeric(.x)) {
      # Replace NA with 0 and then take max (or sum, or any other aggregation)
      x <- ifelse(is.na(.x), 0, .x)
      max(x)
    } else {
      .x[which(!is.na(.x))[1]]
    }
  }))

dupLic_merged$formatted_address = str_to_title(dupLic_merged$formatted_address)

# remove the duplicate license rows from the prov data and append these rows from dupLic_merged
mi_prov_noDups = mi_prov_2024[!mi_prov_2024$License %in% dupLic,] # 7882
mi_prov_noDups = rbind(mi_prov_noDups, dupLic_merged) # 7893

# check for duplicate addresses
length(unique(mi_prov_noDups$Address)) # 7311 of 7893 (incl. 474 formatted_address from nojoin) unique
length(unique(mi_prov_noDups$formatted_address)) # 502

# filtering out based on lara_activ fields
# remove providers with licenses that are "Closed", "Suspended", and "Revoked"
# 7893 prov -> removed 11 non-active licenses -> 7882 prov
mi_prov_noDups = mi_prov_noDups %>% filter(! `Business License Status` %in% c("Closed", "Suspended", "Revoked"))


### remove providers with "Minimum Age" gte 6 years
table(mi_prov_noDups$`Minimum Age`)
# 0 years   1 years  10 years  11 years  12 years 18 months   2 years 2.5 years   3 years 
# 4792       137        11        10         1        61        80      1105       715 
# 4 years   5 years  6 months   6 years   7 years   8 years   9 years 
# 193       184        14        42         8         9         6 

# 7882 prov -> removed 87 progs for ages 6 & above -> 7795 prov
mi_prov_noDups = mi_prov_noDups %>% filter(! `Minimum Age` %in% c("6 years", "7 years", "8 years", "9 years", "10 years", "11 years", "12 years"))
table(is.na(mi_prov_noDups$License)) # 
# FALSE  TRUE 
# 7616   179 
length(unique(mi_prov_noDups$License)) # 7616 of 7795 unique because there is one NA
# 7617

############ formatted_address

# copy the "formatted_address" to the "Address" column if the "Address" column is blank (as in nojoins)
mi_prov_2024 <- mi_prov_noDups %>%
  mutate(Address = ifelse(is.na(Address), formatted_address, Address))

## need to parse the full address strings (from nojoin data) to be consistent with the other addresses 
table(is.na(mi_prov_2024$City)) # 503

# Function to split address into components
split_address_if_city_blank <- function(df) {
  df %>%
    mutate(
      # Check if city is blank or only whitespace
      city_blank = str_trim(City) == "" | is.na(City),
      # Split the address into parts
      address_parts = str_split_fixed(Address, ",", 3),
      # Extract street, city, and zip based on city status
      street = str_trim(address_parts[, 1]),
      city_parsed = str_trim(address_parts[, 2]),
      zip = as.numeric(str_extract(str_trim(address_parts[, 3]), "\\d{5}")),
      # Update if city is blank
      City = if_else(city_blank, city_parsed, City),
      Address = if_else(city_blank, street, Address),
      Zip = if_else(city_blank, zip, Zip)
    ) %>%
    select(-c(city_blank, city_parsed, street, zip, address_parts))  # Remove temporary columns
}

# Apply the function
mi_prov_2024 <- split_address_if_city_blank(mi_prov_2024)

# confirm
table(is.na(mi_prov_2024$City)) # all 7795 recs have city names now except DC630391231 from laraCap that never had a city name in the first place 

# add the field, "State"
# this has all the fields before the subset based on standard fields
mi_prov_2024$State = rep("MI", length(mi_prov_2024$License))

######### apply final fieldnames
fieldNames = c("LicNo","ProvName","Address","City","County","State","Zip","LicType","AgeGroup","ProvQual",
               "Cap02","Cap35","Cap05","CapSTSub02","CapSTSub35","CapSTSub05","CapEHS02","CapHS35","CapHS05",
               "CapAianEHS02","CapAianHS35","CapAianHS05","CapMigrEHS02","CapMigrHS35","CapMigrHS05","CapSTSubPreK","Lat","Lon","Accuracy","Accuracy_Type")

# confirm the order of the fieldnames for the final file
names(mi_prov_2024)
# match the order of the fields to the fieldNames
prov_final = mi_prov_2024[, c("License","Facility Name","Address","City","County","State","Zip","Provider Type","AgeGroup","Publish Rating",
                                    "Cap02","Cap35","Cap05","CapSTSub02","CapSTSub35","CapSTSub05","EHS","HS","CapHS05",
                                    "AIAN EHS","AIAN HS","CapAianHS05","Migrant EHS","Migrant HS","CapMigrHS05","Actual Enrollment","lat","long","accuracy","accuracy_type")]

names(prov_final) = fieldNames

############## get quality estimates
# function to calculate the quality supply estimates
inVar = c("Cap02", "Cap35", "Cap05", "CapSTSub02", "CapSTSub35", "CapSTSub05", "CapEHS02", "CapHS35", "CapHS05","CapAianEHS02", "CapAianHS35", "CapAianHS05", "CapMigrEHS02","CapMigrHS35", "CapMigrHS05",  "CapSTSubPreK")
outVar = paste0(rep("Q", length(inVar)), inVar)

calcSlot <- function(inVar, outVar, prov_final) {
  for (i in seq_along(inVar)) {
    # Check if input column exists
    if (!(inVar[i] %in% names(prov_final))) {
      warning(paste("Input column", inVar[i], "not found in prov_final. Skipping."))
      next  # Skip this iteration if input column is missing
    }
    
    # Create the output column if it doesn't exist
    if (!(outVar[i] %in% names(prov_final))) {
      prov_final[[outVar[i]]] <- NA  # Initialize with NA
    }
    
    # Perform conditional assignment
    condition_met <- prov_final$ProvQual %in% 3:5
    prov_final[[outVar[i]]] <- ifelse(
      condition_met,
      prov_final[[inVar[i]]],
      prov_final[[outVar[i]]]  # Keep existing values for unmatched rows
    )
  }
  return(prov_final)  # Return updated dataframe
}

# Apply function
prov_final <- calcSlot(inVar, outVar, prov_final) # 7795 x 46 

# Convert all numeric fields with NA to 0
prov_final <- prov_final %>%
  mutate_if(is.numeric, ~replace(., is.na(.), 0))

# update AgeGroup if subsidy estimates do not match the current AgeGroup value
prov_final <- prov_final %>%
  mutate(AgeGroup = case_when(
    Cap02 > 0 & (CapSTSub35 > 0 | CapHS35 > 0 | CapAianHS35 > 0 | CapMigrHS35 > 0 | CapSTSubPreK > 0) ~ "0 to 5",
    Cap35 > 0 & (CapSTSub02 > 0 | CapEHS02 > 0 | CapAianEHS02 > 0 | CapMigrEHS02 > 0) ~ "0 to 5",
    (CapSTSub35 > 0 & CapSTSub02 > 0) | (CapHS35 > 0 & CapEHS02 > 0) | (CapAianHS35 > 0 & CapAianEHS02 > 0) | (CapMigrHS35 > 0 & CapMigrEHS02 > 0) ~ "0 to 5",
    CapSTSub35 > 0 | CapHS35 > 0 | CapAianHS35 > 0 | CapMigrHS35 > 0 | CapSTSubPreK > 0 ~ "3 to 5",
    CapSTSub02 > 0 | CapEHS02 > 0 | CapAianEHS02 > 0 | CapMigrEHS02 > 0 ~ "0 to 2",
    TRUE ~ AgeGroup  # Keep the original AgeGroup for all other cases
  ))

# check AgeGroup of these recs, TribalC1178273 (matches) and these 2 (TribalC1190431, TribalC1190621) have 0 capacity and AgeGroup 0-2

### 11 records do not have Lat-Lon
tmp = prov_final[prov_final$Lat == 0,]

# geocode the records in tmp
tmp = tmp %>%
  geocode(street = Address, city = City, state = State, postalcode = Zip, method = "geocodio",
          lat = Lat, long = Lon, full_results = TRUE)

# check geocoding accuracy
index = tmp$accuracy < 0.8; table(index) # all 11 recs have higher accuracy 

# Join the lat/long results from Geocodio back to the original dataset
names(tmp) # which fieldnames to subset
tmp1 = tmp[,c("LicNo", "Lat...47", "Lon...48", "accuracy", "accuracy_type")]
names(tmp1)[2:5] = c("Lat", "Lon", "Accuracy", "Accuracy_Type")

prov_final1 = left_join(prov_final, tmp1, by = "LicNo") 

# copy the new lat-lon & accuracy values to the current columns 
prov_final1 = prov_final1 %>%
  mutate(Lat = ifelse(Lat.x==0, Lat.y, Lat.x),
         Lon = ifelse(Lon.x==0, Lon.y, Lon.x),
         Accuracy = ifelse(Accuracy.x==0, Accuracy.y, Accuracy.x),
         Accuracy_Type = ifelse(is.na(Accuracy_Type.x), Accuracy_Type.y, Accuracy_Type.x)) %>%
  select(-c(Lat.x,Lat.y,Lon.x,Lon.y,Accuracy.x,Accuracy.y,Accuracy_Type.x,Accuracy_Type.y))

# confirm that there are no more blank coods
tmp = prov_final1[prov_final1$Lat == 0,]


# after plotting prov_final1, found two weird points
# DC820017593 = 8646 Fullerton Ave, Detroit, MI 48238 = 42.38167317382239, -83.15571711740216 - originally lat-lon = 33.87036, -117.9242 b'cos address = "8646 Fullerton  Rooms 109 111"
# DC630017626 = 3595 N Adams Rd, Bloomfield Hills, MI 48304 = 42.570035126168186, -83.207849684891 - originally lat-lon = 33.83608, -81.16372 b'cos address = "3595 N Adams Road  Harlan Elementary Sc"

prov_final1[prov_final1$LicNo == "DC820017593" & !is.na(prov_final1$LicNo), c("Address", "Lat", "Lon")] = list("8646 Fullerton Ave", 42.38167317382239, -83.15571711740216)
prov_final1[prov_final1$LicNo == "DC630017626" & !is.na(prov_final1$LicNo), c("Address", "Lat", "Lon")] = list("3595 N Adams Rd", 42.570035126168186, -83.207849684891)

# after manually geocoding the 20 records with accuracy of less than .8 
# and additional 6 records that had place or street center as the accuracy type.
corrected_prov = read.csv("E:/2024_Nov27_Received/Processed Data/miProv_Nov2024.csv", sep = ",")

# 25 recs with low accuracy - no blank addresses
tmp = prov_final1[prov_final1$Accuracy < 0.8 & !is.na(prov_final1$Accuracy), c("LicNo", "Address", "Lat", "Lon", "Accuracy", "Accuracy_Type")]

# corrected_prov has 21 recs (4 recs less) - no blank addresses
tmp1 = corrected_prov[corrected_prov$Address %in% tmp$Address, c("LicNo", "Address", "Lat", "Lon", "Accuracy", "Accuracy_Type")]
# remove these 21 recs from prov_final1
prov_final2 = prov_final1[!prov_final1$Address %in% tmp1$Address,]
# append these 21 recs from corrected_prov
corrected_prov1 = corrected_prov[corrected_prov$Address %in% tmp$Address,]
# 7795 rows & 46 var (after ensuring that the vars are matching)
prov_final2 = dplyr::bind_rows(prov_final2, corrected_prov1)

# Rows in tmp1 that are not in tmp - need to manually get lat-lon values for these
index <- !tmp$Address %in% tmp1$Address
tmp2 = tmp[index,]
# LicNo       Address                 Lat   Lon Accuracy Accuracy_Type        
# <chr>       <chr>                   <dbl> <dbl>  <dbl>         <chr>                
# 1 DC030298292 8 North St             42.5 -85.8     0.71 nearest_rooftop_match
# 2 DC090314551 204 S. MANITOU         43.9 -84.0     0.7  street_center        
# 3 DC390383740 13300 S. 14TH STREET   42.1 -85.6     0.7  street_center        
# 4 DC570305943 251 E. Russell Street  44.3 -85.2     0.78 range_interpolation 

# # manual geocoding of remaining 4 - county names are missing - is that causing low geocoding accuracies?
# DC030298292: 8 North St, Fennville, MI 49408 = 42.589503246097834, -86.07626011841177
# DC090314551: 204 S Manitou St, Pinconning, MI 48650 = 43.85644758880911, -83.96258050482791
# DC390383740: 13300 S 14th St, Schoolcraft, MI 49087 = 42.12503439347564, -85.62913304727904
# DC570305943: 251 Russell Rd, Lake City, MI 49651 = 44.33839817609473, -85.21044116623732
prov_final2[prov_final2$LicNo == "DC030298292" & !is.na(prov_final2$LicNo), c("Address", "Lat", "Lon", "Accuracy", "Accuracy_Type")] = list("8 North St", 42.589503246097834, -86.07626011841177, 1, "Google")
prov_final2[prov_final2$LicNo == "DC090314551" & !is.na(prov_final2$LicNo), c("Address", "Lat", "Lon", "Accuracy", "Accuracy_Type")] = list("204 S Manitou St", 43.85644758880911, -83.96258050482791, 1, "Google")
prov_final2[prov_final2$LicNo == "DC390383740" & !is.na(prov_final2$LicNo), c("Address", "Lat", "Lon", "Accuracy", "Accuracy_Type")] = list("13300 S 14th St", 42.12503439347564, -85.62913304727904, 1, "Google")
prov_final2[prov_final2$LicNo == "DC570305943" & !is.na(prov_final2$LicNo), c("Address", "Lat", "Lon", "Accuracy", "Accuracy_Type")] = list("251 Russell Rd", 44.33839817609473, -85.21044116623732, 1, "Google")

# check that there are no blank lat values
table(is.na(prov_final2$Lat)) # FALSE 7795 = false

# final provider feature class
# Convert to sf object
prov_sf <- st_as_sf(prov_final2, coords = c("Lon", "Lat"), crs = "EPSG:4326")
# 7795 features with 44 fields and geometry type Point
st_write(prov_sf, "E:/2024_Nov27_Received/Final Data/Nov2024.gdb", layer="mi_prov", driver="OpenFileGDB", append=FALSE)
st_write(prov_sf, "E:/2024_Nov27_Received/Final Data/Nov2024_new.gpkg", layer="mi_prov_new", append=FALSE)
write.csv(prov_sf, "E:/2024_Nov27_Received/Final Data/miProv_Nov2024.csv", row.names = F)


# filter out the DRWP prov from prov_sf = 5098 - 2697 points removed instead of 2810 - not sure what's happening here - abandoning this approach
# instead export the drwp_prov data with lat-lon values (that Nicholas updated; E:\2024_Nov27_Received\Final Data\MIProvidersNov2024.gpkg\main.drwp_prov) to csv 
# fix the 16 blank lic ids there and then append to the prov_sf where the prov in the 7 counties have been filtered out in ArcPro
# make sure that the exported csv files have lat-lon values before doing the manual matches


## read the csv files 
drwp_prov_updated = read.csv("E:/2024_Nov27_Received/Final Data/drwp_prov.csv", sep=",") # 2774
mi_prov_minus_7county = read.csv("E:/2024_Nov27_Received/Final Data/miProv_Nov2024_7county_removed.csv") # 4985
# append them
mi_prov_updated = rbind(mi_prov_minus_7county, drwp_prov_updated) # 7759
#left_join to get lat-lon
mi_prov_updated1 = left_join(mi_prov_updated, prov_final2[,c("LicNo", "Lat", "Lon")], by = "LicNo")

#check to see how many lat values are blank
table(is.na(mi_prov_updated1$Lat)) # 158 are blank
index = is.na(mi_prov_updated1$Lat)
blank_lic = mi_prov_updated1[index,] # 158
noblank_lic = mi_prov_updated1[!index,] # 7601

blank_lic_geo = blank_lic %>%
   geocode(street = Address, city = City, state = State, postalcode = Zip, method = "geocodio", lat = Lat, long = Lon, full_results = TRUE)

blank_lic1 = blank_lic_geo[,c(1:42,51:52,47:48)]
names(blank_lic1)[43:46] = c( "Accuracy", "Accuracy_Type", "Lat", "Lon")

# 7759 rows & 46 var (after ensuring that the vars are matching)
final_joined = dplyr::bind_rows(noblank_lic, blank_lic1)

# final provider file - 7759
final_joined <- st_as_sf(final_joined, coords = c("Lon", "Lat"), crs = "EPSG:4326")
# Writing 7759 features with 44 fields and geometry type Point
st_write(final_joined, "E:/2024_Nov27_Received/Final Data/Nov2024.gdb", layer="mi_prov_new", driver="OpenFileGDB", append=FALSE)
st_write(final_joined, "E:/2024_Nov27_Received/Final Data/Nov2024_new.gpkg", layer="mi_prov_new", append=FALSE)
write.csv(final_joined, "E:/2024_Nov27_Received/Final Data/miProv_Nov2024.csv", row.names = F)

# visualize the points
ggplot() + 
  geom_sf(data = final_joined, fill=NA, color="red", size=4)  

final_df = final_joined
st_geometry(final_df) = NULL

## data quality checks
check_outliers_and_NAs <- function(df) {
  # Only consider numeric columns
  numeric_cols <- df[, sapply(df, is.numeric), drop = FALSE]
  
  # Function to count outliers and NAs for one column
  analyze_col <- function(x) {
    na_count <- sum(is.na(x))
    
    # Use IQR method: outliers = values outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR]
    q1 <- quantile(x, 0.25, na.rm = TRUE)
    q3 <- quantile(x, 0.75, na.rm = TRUE)
    iqr <- q3 - q1
    lower <- q1 - 1.5 * iqr
    upper <- q3 + 1.5 * iqr
    outlier_count <- sum(x < lower | x > upper, na.rm = TRUE)
    
    return(c(Outliers = outlier_count, NAs = na_count))
  }
  
  # Apply to all numeric columns
  results <- t(sapply(numeric_cols, analyze_col))
  results <- as.data.frame(results)
  return(results)
}

check_outliers_and_NAs(final_df)

# check histogram & boxplots also




# summary stats
summary(final_df[,c(11:42)])

# totals
apply(final_df[c(11:42)], 2, median, na.rm=T)


# # previous version 
# E:\2024_Nov27_Received\Final Data\MIProvidersNov2024.gpkg\main.mi_prov; main.drwp_prov; main.dtw_prov  

#### replace relevant recs in prov_final2 with 7-county corrected data
# 2785 prov in the 7 counties
drwp_prov = sf::st_read(dsn = "E:/2024_Nov27_Received/Final Data/MIProvidersNov2024.gpkg", layer="drwp_prov") # 2785 prov; NAD83 - EPSG:4269
# 16 lic ids are blank (HS data), 19 of 2785 addresses are not unique, 102 rows have no County name 
# blank fields: CapAianEHS02  CapAianHS35  CapAianHS05 CapMigrEHS02  CapMigrHS35  CapMigrHS05
# 102 rows have blank County field so the subset must have been a spatial one

# # read MI counties and subset them to the 7 counties
# # cb = TRUE to get simplified geometries - also removes the parts of tracts that are in water 
# # these layers have EPSG 4269 (NAD83)
# mi_counties = counties(state="MI", cb=TRUE, year=2024, class="sf") # 83
# drwp_county = mi_counties[mi_counties$NAME %in% c('Wayne', 'Oakland', 'Macomb', 'Washtenaw', 'Livingston', 'Monroe', 'St. Clair'), ]
# drwp_county = drwp_county[, c("GEOID", "NAMELSAD", "geometry")]
# # remove the word County in the County field
# drwp_county$NAMELSAD = gsub("County", "", drwp_county$NAMELSAD)
# drwp_county$NAMELSAD = trimws(drwp_county$NAMELSAD)
# st_crs(drwp_county) # EPSG = 4269
# 
# # filter out the 7-county prov from the MI prov df (prov_final2)
# # transform drwp_county to same projection as prov_sf
# drwp_county = st_transform(drwp_county, crs="EPSG:4326")
# 
# # points intersecting with drwp counties = 2810 whereas the de-duplicated drwp list has 2785
# points_to_remove <- st_intersection(prov_sf, drwp_county)
# # table(points_to_remove$NAMELSAD)
# # Livingston     Macomb     Monroe    Oakland  St. Clair  Washtenaw      Wayne 
# #      104        455         96        738         76        256       1085 
# # table(points_to_remove$County)
# # Livingston     Macomb     Monroe    Oakland  St. Clair  Washtenaw      Wayne 
# #      98        437         89        715         75        252       1029 
# # populate county names
# points_to_remove$County <- points_to_remove$NAMELSAD
# 
# # filter out the DRWP prov from prov_sf = 5098 - 2697 points removed instead of 2810 - not sure what's happening here - abandoning this approach
# prov_sf = prov_sf[!prov_sf$County %in% c('Wayne', 'Oakland', 'Macomb', 'Washtenaw', 'Livingston', 'Monroe', 'St. Clair'), ]
# 
# # instead export the drwp_prov data (that Nicholas updated; E:\2024_Nov27_Received\Final Data\MIProvidersNov2024.gpkg\main.drwp_prov) to csv
# # fix the 16 blank lic ids there and then append to the prov_sf
# # meanwhile spatially filter out the prov in the drwp counties from prov_sf in ArcPro
