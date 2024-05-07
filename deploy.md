##Deploying instructions

```shell
gcloud functions deploy extract_census_data \
--gen2 \
--region=us-central1 \
--runtime=python312 \
--source=. \
--entry-point=extract_census_data \
--service-account='finalprojectbot@testing-404600.iam.gserviceaccount.com' \
--trigger-http



gcloud functions deploy prepare_census_data \
--gen2 \
--region=us-central1 \
--runtime=python312 \
--source=. \
--entry-point=prepare_census_data \
--service-account='finalprojectbot@testing-404600.iam.gserviceaccount.com' \
--trigger-http


gcloud functions deploy load_census_data \
--gen2 \
--region=us-central1 \
--runtime=python312 \
--source=. \
--entry-point=load_census_data \
--service-account='finalprojectbot@testing-404600.iam.gserviceaccount.com' \
--trigger-http



gcloud functions deploy prepare_block_group \
--gen2 \
--region=us-central1 \
--runtime=python312 \
--source=. \
--entry-point=prepare_block_group \
--service-account='finalprojectbot@testing-404600.iam.gserviceaccount.com' \
--trigger-http


gcloud functions deploy load_block_group \
--gen2 \
--region=us-central1 \
--runtime=python312 \
--source=. \
--entry-point=load_block_group \
--service-account='finalprojectbot@testing-404600.iam.gserviceaccount.com' \
--trigger-http


gcloud functions deploy extract_demo_data \
--gen2 \
--region=us-central1 \
--runtime=python312 \
--source=. \
--entry-point=extract_demo_data \
--service-account='finalprojectbot@testing-404600.iam.gserviceaccount.com' \
--trigger-http


gcloud functions deploy prepare_demo_data \
--gen2 \
--region=us-central1 \
--runtime=python312 \
--source=. \
--entry-point=prepare_demo_data \
--service-account='finalprojectbot@testing-404600.iam.gserviceaccount.com' \
--trigger-http


gcloud functions deploy load_demo_data \
--gen2 \
--region=us-central1 \
--runtime=python312 \
--source=. \
--entry-point=load_demo_data \
--service-account='finalprojectbot@testing-404600.iam.gserviceaccount.com' \
--trigger-http


ogr2ogr \-f "PostgreSQL" \-nln landmarks.points \-nlt MULTIPOLYGON \-t_srs EPSG:4326 \-lco GEOMETRY_NAME=geog \-lco GEOM_TYPE=GEOGRAPHY \-overwrite \PG:"host=localhost port=5432 dbname=musa509assign_2 user=avani password=sqlpassword" \Landmarks_AGOTrainingOnly.geojson


/Users/avani/Documents/cloud_comp/final_project/tl_2023_42_tract