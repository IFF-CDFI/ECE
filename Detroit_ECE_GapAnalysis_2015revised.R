# 1: Load necessary libraries 
library(tidyverse)  # Includes dplyr, tidyr, ggplot2
library(readxl)
library(openxlsx)
library(sf)
library(fuzzyjoin)
library(tigris)
library(areal)
options(scipen=999, tigris_use_cache = TRUE)



# 1: Define field vectors in the beginning
dmd_fields = c('Dmd02', 'Dmd35', 'Dmd05', 'DmdSTSub02', 'DmdSTSub35', 'DmdSTSub05', 'DmdEHS02', 'DmdHS35', 'DmdHS05', 'DmdSTSubPreK')
sup_fields = c('Cap02', 'Cap35', 'Cap05')
qsup_fields = c('QCap02', 'QCap35', 'QCap05')
gap_fields = c('Gap02', 'Gap35', 'Gap05')
qgap_fields = c('QGap02', 'QGap35', 'QGap05')


# 2: load files & save workspace
load("Z:/Research and Evaluation/ArcGIS/Data/MI/Wayne County/Detroit/2015 Detroit ECE revised/ece2015.RData")
save.image("Z:/Research and Evaluation/ArcGIS/Data/MI/Wayne County/Detroit/2015 Detroit ECE revised/ece2015.RData")
#previous workspace
#load("C:/Users/prao/OneDrive - IFF/General - Detroit ECE Study/Data/Quantitative/2015 Reanalysis/ece2023_2015.RData")

# Load Detroit provider data
# 382 prov; WGS84 - EPSG:4326
prov2015 = st_read("Z:/Research and Evaluation/ArcGIS/Data/MI/Wayne County/Detroit/2015 Detroit ECE revised/DetroitECE2015.gdb", layer="DtwSup2015") %>%
  st_transform(crs = "ESRI:102690") %>% 
  st_make_valid()

# Reclassify the Quality Rating to a numeric quality score and standardize column names
# only 90 of 382 providers are high quality!!
sup2015 = prov2015 %>%
  mutate(ProvQual = case_when(
    PrfScore > 25 ~ 1,
    TRUE ~ 0)) %>%
  select(
  LicNo = "LicnsNo",
  ProvName = "PrvdrName",
  Address,
  City,
  Zip,
  LicType = "PrvdrType1",
  ProvQual,
  Cap02,
  Cap35,
  Cap05 = "CapTot"
) %>%
  # Calculate quality capacity 
  mutate(
    # if high quality provider, calculate quality capacity = capacity  
    QCap02 = if_else(ProvQual == 1, Cap02, 0),
    QCap35 = if_else(ProvQual == 1, Cap35, 0),
    QCap05 = if_else(ProvQual == 1, Cap05, 0)
  )

# Load tract-level demand data for Detroit - 297 tracts; WGS84
tractDmd2015 = st_read("Z:/Research and Evaluation/ArcGIS/Data/MI/Wayne County/Detroit/2015 Detroit ECE revised/DetroitECE2015.gdb", layer="DtwTractDmd2015") %>%
  st_transform(crs = "ESRI:102690") %>% 
  st_make_valid() %>%
  # standardize column names
  select(
    TractID = "FIPS",
    Dmd02 = "ESRI_Age0_2",
    Dmd35 = "ESRI_Age3_5",
    Dmd05 = "ESRI_Age0_5",
    DmdSTSub02 = "state_subsidy_demand_0_2",
    DmdSTSub35 = "state_subsidy_demand_3_5",
    DmdSTSub05 = "state_subsidy_demand_0_5",
    DmdEHS02 = "early_head_start_demand",
    DmdHS35 = "head_start_demand",
    DmdHS05 = "federal_subsidy_demand_HS_EHS",
    DmdSTSubPreK = "state_pk4_demand",
    geometry = "SHAPE"
    ) %>%
  mutate(across(everything(), ~replace_na(., 0))) # Replace NA with 0 (for interpolation to work)

# Use st_drop_geometry() for creating a dataframe
dtwTractDmd_df <- st_drop_geometry(tractDmd2015)


# 3: Load the three Detroit geography layers - all in WGS84
# Transform to the projection system recommended by D3 - ESRI:102690 - NAD 1983 SPCS Michigan South (Feet)
# ENSURE THAT ALL SF LAYERS HAVE A FIELD CALLED GEOMETRY INSTEAD OF SHAPE FOR INTERPOLATION TO WORK - HAPPENING IN THE NEW VERSION OF R 
# Downloaded City_of_Detroit_Zip_Code_Tabulation_Areas on Dec 14 2025 - has sliver polygons for zips 48225 and 48240 but using as is - 34 ZCTAs
dtwZcta <- load_spatial_layer(
  "Z:/Research and Evaluation/ArcGIS/Data/MI/Wayne County/Detroit/DetroitLayers.gdb",
  "zip_codes",
  keep_cols = c("zipcode", "Shape")
)

# The 54 Official Master Plan Neighborhoods based on the 2010 Census Tract boundary from City of Detroit Open Data Portal - doesn't include Belle Isle like last year's version
dtwNbd <- load_spatial_layer(
  "Z:/Research and Evaluation/ArcGIS/Data/MI/Wayne County/Detroit/DetroitLayers.gdb",
  "MasterPlanNbds",
  keep_cols = c("NHOOD", "Shape"),
  rename_map = c(Nbd = "NHOOD")
)

dtwCDist <- load_spatial_layer(
  "Z:/Research and Evaluation/ArcGIS/Data/MI/Wayne County/Detroit/DetroitLayers.gdb",
  "CityCouncilDists2023",
  keep_cols = c("Name", "Shape"),
  rename_map = c(CDist = "Name")
)


# 4: Function to load and standardize spatial layers
load_spatial_layer <- function(dsn, layer, keep_cols, rename_map = NULL, target_crs = "ESRI:102690") {
  layer_data <- st_read(dsn, layer, quiet = TRUE) %>%
    select(all_of(keep_cols))
  
  if (!is.null(rename_map)) {
    layer_data <- rename(layer_data, !!!rename_map)
  }
  
  # Rename Shape to geometry if it exists
  if ("Shape" %in% names(layer_data)) {
    layer_data <- rename(layer_data, geometry = Shape)
  }
  
  st_transform(layer_data, crs = target_crs) %>% st_make_valid()
}


# 5: Summarize Detroit point provider data to tracts 
# spatial join to associate provider point locations with tract polygons
dtwTractSupDmd <- st_join(sup2015, tractDmd2015[, c("TractID", dmd_fields)]) 

# Select only the columns that contain "Cap" in their names
cap_columns <- names(dtwTractSupDmd)[grepl("Cap", names(dtwTractSupDmd))]
# Summarize provider data by tract polygon - 200 of 297 tracts have prov
dtwTractSup <- dtwTractSupDmd %>%
  group_by(TractID) %>% # Group by polygon geometry
  summarise(
    prov = n(), # count no. of providers per tract
    qprov = sum(ProvQual == 1), # count no. of quality providers per tract
    across(all_of(cap_columns), ~sum(., na.rm = TRUE)) # Sum the "Cap" columns 
  )

dtwTractSup_df <- st_drop_geometry(dtwTractSup)


# 6: Create a reusable function for gap calculation
calculate_gaps <- function(demand_sf, supply_df, sf_id, 
                           df_id) {
  
  # Join and prepare data
  ece_data <- demand_sf %>%
    select(all_of(c(sf_id, dmd_fields))) %>%
    st_drop_geometry() %>%
    left_join(supply_df, by = setNames(sf_id, df_id)) %>%
    mutate(across(everything(), ~replace_na(., 0)))
  
  # Calculate gaps
  gap_data <- bind_cols(
    ece_data[, c(sf_id, "prov", "qprov")],
    ece_data[, sup_fields] - ece_data[, dmd_fields[1:3]],      # gaps
    ece_data[, qsup_fields] - ece_data[, dmd_fields[1:3]],     # quality gaps
    ece_data[, sup_fields],                                # supply
    ece_data[, qsup_fields],                               # quality supply
    ece_data[, dmd_fields]                                 # demand
  )
  
  # Set column names
  names(gap_data) <- c(sf_id, 'NumProv', 'NumQProv', 
                       gap_fields, qgap_fields, sup_fields, qsup_fields, dmd_fields)
  
  gap_data
}

# Calculate tract gaps
dtwTractEceGap <- calculate_gaps(
  tractDmd2015[, c("TractID", dmd_fields)],
  dtwTractSup_df,
  "TractID",
  "TractID"
)

# Join back to spatial data and transform
dtwTractEceGap <- tractDmd2015 %>%
  st_cast("MULTIPOLYGON") %>%
  select(TractID, geometry) %>%
  left_join(dtwTractEceGap, by = "TractID") %>%
  st_transform(crs = "EPSG:4326") %>%
  mutate(across(where(is.numeric), ~round(., 0))) 

# 7: Create function for processing geographic levels
process_geographic_level <- function(target_sf, target_id, geo_name, 
                                     source_demand, source_id,
                                     provider_data) {
  
  # Interpolate demand
  demand <- aw_interpolate(
    target_sf,
    tid = !!sym(target_id),
    source = source_demand,
    sid = !!sym(source_id),
    weight = "total",
    output = "sf",
    extensive = dmd_fields
  ) %>%
    select(!!sym(target_id), all_of(dmd_fields), geometry)
  
  # Spatial join for supply
  supply <- st_join(provider_data, target_sf) %>%
    group_by(!!sym(target_id)) %>%
    summarise(
      prov = n(),
      qprov = sum(ProvQual == 1),
      across(all_of(cap_columns), ~sum(., na.rm = TRUE))
    )
  
  supply_df <- st_drop_geometry(supply)
  
  # Calculate gaps
  gap_data <- calculate_gaps(demand, supply_df, target_id, geo_name)
  
  # Join to spatial and transform
  result <- target_sf %>%
    select(!!sym(target_id), geometry) %>%
    left_join(gap_data, by = setNames(geo_name, target_id)) %>%
    st_transform(crs = "EPSG:4326") %>%
    mutate(across(where(is.numeric), ~round(., 0)))
  
  result
}

# Process all geographic levels efficiently
# account for multipolygon issue if needed 
dtwNbdEceGap <- process_geographic_level(dtwNbd, "Nbd", "Nbd", 
                                         tractDmd2015, "TractID", sup2015) %>%
  st_cast("MULTIPOLYGON")


dtwZctaEceGap <- process_geographic_level(dtwZcta, "zipcode", "zipcode", 
                                          tractDmd2015, "TractID", sup2015) %>%
  rename(Zip = zipcode) %>%
  st_cast("MULTIPOLYGON")

dtwCDistEceGap <- process_geographic_level(dtwCDist, "CDist", "CDist", 
                                           tractDmd2015, "TractID", sup2015) %>%
  st_cast("MULTIPOLYGON")


# 8: Write outputs more efficiently using a function
write_gap_outputs <- function(tract, nbd, zcta, cdist, include_supply = TRUE) {
  
  suffix <- if (include_supply) "All" else ""
  gdb_name <- paste0("Detroit", suffix, "ECEGap2015.gdb")
  
  # Remove supply fields if needed
  if (!include_supply) {
    tract <- select(tract, -all_of(c(sup_fields, qsup_fields)))
    nbd <- select(nbd, -all_of(c(sup_fields, qsup_fields)))
    zcta <- select(zcta, -all_of(c(sup_fields, qsup_fields)))
    cdist <- select(cdist, -all_of(c(sup_fields, qsup_fields)))
  }
  
  # Write to GDB
  gdb_path <- paste0("Z:/Research and Evaluation/ArcGIS/Data/MI/Wayne County/Detroit/2015 Detroit ECE revised/", gdb_name)

  st_write(tract, dsn = gdb_path, layer = paste0(suffix, 'TractGap2015'), delete_layer = TRUE, quiet = TRUE)
  st_write(nbd, dsn = gdb_path, layer = paste0(suffix, 'NbdGap2015'), delete_layer = TRUE, quiet = TRUE)
  st_write(zcta, dsn = gdb_path, layer = paste0(suffix, 'ZctaGap2015'), delete_layer = TRUE, quiet = TRUE)
  st_write(cdist, dsn = gdb_path, layer = paste0(suffix, 'CDistGap2015'), delete_layer = TRUE, quiet = TRUE)
  
  # Write to Excel
  if (include_supply) {
    excel_data <- list(
      "Tract" = st_drop_geometry(tract),
      "Nbd" = st_drop_geometry(nbd),
      "Zcta" = st_drop_geometry(zcta),
      "CDist" = st_drop_geometry(cdist)
    )
    write.xlsx(excel_data,
                  file = "Z:/Research and Evaluation/ArcGIS/Data/MI/Wayne County/Detroit/2015 Detroit ECE revised/DetroitAllEceGaps_2015.xlsx")
  }
}

# Write both versions
write_gap_outputs(dtwTractEceGap, dtwNbdEceGap, dtwZctaEceGap, dtwCDistEceGap, TRUE)
write_gap_outputs(dtwTractEceGap, dtwNbdEceGap, dtwZctaEceGap, dtwCDistEceGap, FALSE)
