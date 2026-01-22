#################### Detroit ECE gap analysis using Nov 2024 data ######################

# 1: Load necessary libraries 
library(tidyverse)  # Includes dplyr, tidyr, ggplot2
library(readxl)
library(openxlsx)
library(sf)
library(fuzzyjoin)
library(tigris)
library(areal)
options(scipen=999, tigris_use_cache = TRUE)

# Load/save workspace
load("E:/2024_Nov27_Received/Processed Data/dtwEceGapNov2024.RData")
save.image("E:/2024_Nov27_Received/Processed Data/dtwEceGapNov2024.RData")

# 2: Define field vectors in the beginning
dmd_fields = c('Dmd02', 'Dmd35', 'Dmd05', 'DmdSTSub02', 'DmdSTSub35', 'DmdSTSub05', 'DmdEHS02', 'DmdHS35', 'DmdHS05', 'DmdSTSubPreK')
sup_fields = c('Cap02', 'Cap35', 'Cap05', 'CapSTSub02', 'CapSTSub35', 'CapSTSub05', 'CapEHS02', 'CapHS35', 'CapHS05', 'CapSTSubPreK')
qsup_fields = c('QCap02', 'QCap35', 'QCap05', 'QCapSTSub02', 'QCapSTSub35', 'QCapSTSub05', 'QCapEHS02', 'QCapHS35', 'QCapHS05', 'QCapSTSubPreK')
gap_fields = c('Gap02', 'Gap35', 'Gap05', 'STSubGap02', 'STSubGap35', 'STSubGap05', 'EHSGap02', 'HSGap35', 'EHSHSGap05', 'STSubPreKGap')
qgap_fields = c('QGap02', 'QGap35', 'QGap05', 'QSTSubGap02', 'QSTSubGap35', 'QSTSubGap05', 'QEHSGap02', 'QHSGap35', 'QEHSHSGap05', 'QSTSubPreKGap')

# 3: Function to load and standardize spatial layers
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

# Load Detroit layers without the river - all in WGS84
# Transform to the projection system recommended by D3 - ESRI:102690 - NAD 1983 SPCS Michigan South (Feet)
# ENSURE THAT ALL SF LAYERS HAVE A FIELD CALLED GEOMETRY INSTEAD OF SHAPE FOR INTERPOLATION TO WORK - HAPPENING IN THE NEW VERSION OF R 
dtw_bnd <- st_read("Z:/Research and Evaluation/ArcGIS/Data/MI/Wayne County/Detroit/DetroitLayers.gdb", 
                   "DetroitBoundary", quiet = TRUE) %>%
  select(OBJECTID, Shape) %>%
  st_transform(crs = "ESRI:102690") %>% 
  st_make_valid()

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

# Load provider data
# MI prov data was matched manually (in additional to script-based matches) at the state level 
# 7581 prov; WGS84 - EPSG:4326
prov <- st_read(dsn = "E:/2024_Nov27_Received/Final Data/Nov2024_new.gpkg", 
                layer = "mi_prov_new", quiet = TRUE) %>%
  st_transform(crs = "ESRI:102690") %>% 
  st_make_valid()

# Filter to Detroit and rename geometry in one step
# prov in Detroit City - 492 including 2 with no LicNo
dtw_prov <- st_filter(prov, dtw_bnd, .predicate = st_within) %>%
  rename(geometry = geom) 

rm(prov)  # Free memory

### Detroit tract-level ECE Demand for 2024
# IFF ECE Demand 2024 table - 18804 obs
iffDmd2024 <- read.csv("Z:/Research and Evaluation/ArcGIS/Data/1- All IFF States/Tables/ECE Demand/2024/demandData2024.csv") %>%
  mutate(CensTract = as.character(CensTract))

# 4: Pipeline the tract processing
mi_tracts <- tracts(state = "MI", cb = TRUE, year = 2024, class = "sf") # 2971 tracts in MI

miTractDmd <- mi_tracts %>%
  select(GEOID) %>%
  left_join(iffDmd2024, by = c("GEOID" = "CensTract")) %>%
  rename(Dmd02 = EsrAge02, Dmd35 = EsrAge35, Dmd05 = EsrAge05) %>%
  mutate(across(everything(), ~replace_na(., 0))) %>%  # Replace NA with 0 (for interpolation to work)
  st_transform(crs = "ESRI:102690") %>%
  st_make_valid() %>%
  mutate(orig_area = as.numeric(st_area(.)))

# remove the statewide and IFF footprint wide layers
rm(iffDmd2024, mi_tracts)

# Intersect MI tract demand data with Detroit boundary and apportion dmd_fields
dtwTractDmd <- st_intersection(miTractDmd, dtw_bnd) %>%
  mutate(
    new_area = as.numeric(st_area(.)),
    area_proportion = new_area / orig_area
  ) %>%
  mutate(across(all_of(dmd_fields), as.numeric)) %>%
  mutate(across(
    all_of(dmd_fields),
    ~if_else(area_proportion < 1, . * area_proportion, .),
    .names = "{.col}"
  )) %>%
  mutate(across(all_of(dmd_fields), ~round(., 0))) %>%
  mutate(
    Dmd05 = Dmd02 + Dmd35,
    DmdSTSub05 = DmdSTSub02 + DmdSTSub35,
    DmdHS05 = DmdEHS02 + DmdHS35      
  ) %>%
  rename(TractID = GEOID)

# check to see if these tracts  
# tractID 26163515700; area_prop = 0.53; orig Dmd05, Dmd02, Dmd35 = 78, 41, 37; new Dmd05, Dmd02, Dmd35 = 42, 22, 20 
# tractID 26163520800; area_prop = 0.66; orig = Dmd05, Dmd02, Dmd35 32, 16, 16; new = Dmd05, Dmd02, Dmd35 20, 10, 10

# 5: Use st_drop_geometry() for creating a dataframe
dtwTractDmd_df <- st_drop_geometry(dtwTractDmd)
# Summarize the demand data - these demand totals are slightly less than the DRWP demand totals for Detroit because some of the tracts were clipped and therefore apportioned 
dmdTotals <- colSums(dtwTractDmd_df[, dmd_fields], na.rm = TRUE)

### summarize Detroit point provider data to tracts 
# spatial join to associate provider point locations with tract polygons
dtwTractSupDmd <- st_join(dtw_prov, dtwTractDmd[, c("TractID", dmd_fields)])

# Select only the columns that contain "Cap" in their names
cap_columns <- names(dtwTractSupDmd)[grepl("Cap", names(dtwTractSupDmd))]
# Summarize provider data by tract polygon - 209 of 332 tracts have prov
dtwTractSup <- dtwTractSupDmd %>%
  group_by(TractID) %>% # Group by polygon geometry
  summarise(
    prov = n(), # count no. of providers per tract
    qprov = sum(ProvQual > 2), # count no. of quality providers per tract
    across(all_of(cap_columns), ~sum(., na.rm = TRUE)) # Sum the "Cap" columns 
  )

dtwTractSup_df <- st_drop_geometry(dtwTractSup)
# These ECE supply totals match with the DRWP supply totals for Detroit
supTotals <- colSums(dtwTractSup_df[, 2:23], na.rm = TRUE)

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
    ece_data[, sup_fields] - ece_data[, dmd_fields],      # gaps
    ece_data[, qsup_fields] - ece_data[, dmd_fields],     # quality gaps
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
  dtwTractDmd[, c("TractID", dmd_fields)],
  dtwTractSup_df,
  "TractID",
  "TractID"
)

# Join back to spatial data and transform
dtwTractEceGap <- dtwTractDmd %>%
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
      qprov = sum(ProvQual > 2),
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
                                         dtwTractDmd, "TractID", dtw_prov)


dtwZctaEceGap <- process_geographic_level(dtwZcta, "zipcode", "zipcode", 
                                          dtwTractDmd, "TractID", dtw_prov) %>%
  rename(Zip = zipcode) %>%
  st_cast("MULTIPOLYGON")

dtwCDistEceGap <- process_geographic_level(dtwCDist, "CDist", "CDist", 
                                           dtwTractDmd, "TractID", dtw_prov) %>%
  st_cast("MULTIPOLYGON")


# 8: Write outputs more efficiently using a function
write_gap_outputs <- function(tract, nbd, zcta, cdist, include_supply = TRUE) {
  
  suffix <- if (include_supply) "All" else ""
  gdb_name <- paste0("Detroit", suffix, "ECEGap2024.gdb")
  
  # Remove supply fields if needed
  if (!include_supply) {
    tract <- select(tract, -all_of(c(sup_fields, qsup_fields)))
    nbd <- select(nbd, -all_of(c(sup_fields, qsup_fields)))
    zcta <- select(zcta, -all_of(c(sup_fields, qsup_fields)))
    cdist <- select(cdist, -all_of(c(sup_fields, qsup_fields)))
  }
  
  # Write to GDB
  #gdb_path <- paste0("Z:/Research and Evaluation/ArcGIS/Data/MI/Wayne County/Detroit/2024 Detroit ECE/", gdb_name)
  gdb_path <- paste0("E:/2024_Nov27_Received/Processed Data/2024DetroitECEGap/", gdb_name)

  st_write(tract, dsn = gdb_path, layer = paste0(suffix, 'TractGap2024'), delete_layer = TRUE, quiet = TRUE)
  st_write(nbd, dsn = gdb_path, layer = paste0(suffix, 'NbdGap2024'), delete_layer = TRUE, quiet = TRUE)
  st_write(zcta, dsn = gdb_path, layer = paste0(suffix, 'ZctaGap2024'), delete_layer = TRUE, quiet = TRUE)
  st_write(cdist, dsn = gdb_path, layer = paste0(suffix, 'CDistGap2024'), delete_layer = TRUE, quiet = TRUE)
  
  # Write to Excel
  if (include_supply) {
    excel_data <- list(
      "Tract" = st_drop_geometry(tract),
      "Nbd" = st_drop_geometry(nbd),
      "Zcta" = st_drop_geometry(zcta),
      "CDist" = st_drop_geometry(cdist)
    )
    write.xlsx(excel_data,
               file = "E:/2024_Nov27_Received/Processed Data/2024DetroitECEGap/DetroitAllEceGaps_Nov2024.xlsx")
#               file = "Z:/Research and Evaluation/ArcGIS/Data/MI/Wayne County/Detroit/2024 Detroit ECE/DetroitAllEceGaps_Nov2024.xlsx")
  }
}

# Write both versions
write_gap_outputs(dtwTractEceGap, dtwNbdEceGap, dtwZctaEceGap, dtwCDistEceGap, TRUE)
write_gap_outputs(dtwTractEceGap, dtwNbdEceGap, dtwZctaEceGap, dtwCDistEceGap, FALSE)



#####################

## City level summaries 

dtwQProv <- filter(dtw_prov, ProvQual %in% c(3, 4, 5))

facSumm <- dtwQProv %>% # dtw_prov; dtwQProv
  group_by(LicType) %>%
  summarise(
    prov = n(),
    cdc = sum(CapSTSub05 > 0, na.rm = TRUE),
    gsrp = sum(CapSTSubPreK > 0, na.rm = TRUE),
    hsehs = sum(CapHS05 > 0, na.rm = TRUE)
  )


