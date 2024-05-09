tracts <- st_read("~/Documents/cloud_comp/final_project/tl_2023_42_tract/tl_2023_42_tract.shp")
glimpse(tracts)

tracts$geog <- st_as_text(tracts$geometry)
tracts$geometry <- NULL
write.csv(tracts, "tracts.csv", append = TRUE)

library(geojsonsf)

# Convert to GeoJSON
geojson_data <- sf_geojson(tracts)

# Split GeoJSON into features
features <- strsplit(gsub(pattern = "\\},\\{", replacement = "}\\n{", x = geojson_data), split = "\n")[[1]]

# Write features to a JSONL file
jsonl_path <- "output.jsonl"  
writeLines(features, jsonl_path)
