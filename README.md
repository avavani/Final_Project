# MUSA 5090 Spring 2024 Final Project: Demolitions in Philadelphia

Slides: https://docs.google.com/presentation/d/1q9uNfhpkKNvTyr8I3gUdNr0a2W7AQ4nJ6-SCohFPnaw/edit?usp=sharing

In this project, I was interested in trying out some of the cloud functions that we learned in class. My goal was more to increase my familiarity with the interfaces of Google Cloud Storage, Cloud Function, Big Query, and Carto. To this end, I worked to create an ELT pipeline for 3 datasets. These scripts were deployed on Google Cloud Functions and a workflow was set in order to initiate the trigger of every 6 months. Finally, the datasets that were loaded were manipulated in BigQuery and modeled to create prediction for demolitions in Philadelphia. 

## About the Datasets
There are 3 datasets that I used during this project. The First is the Demolition data from Open Data Philly which contains geographic information about demolitions happening all over the city since 2008. The final prediction is supplemented by ACS data at the tract level, as well as a census tract map of Philadelphia. 

## Note About Block Group Function
An earlier iteration of this project was seeking to look at demolition predictions at the Block Group level. I wrote a data pipeline to extract, prepare, and load my block group data to Big Query. However, while my functions did work and I was able to load this information to Big Query, its geographic information kept getting corrupted. After trying many times, I was unable to use this dataset effectively in my queries and hence did not end up using it. However, in recognition of the work done, a folder with the scripts used for this pipeline is present in this repository. 

## Functions
In total, I uploaded 9 functions to the cloud though I only actually use 6 in my final analysis. These functions have all been tested and are arranged in a workflow as follows. The trigger for these workflow is set to automatically start every 6 months.

<img width="551" alt="Screenshot 2024-05-07 at 5 45 46 PM" src="https://github.com/avavani/Final_Project/assets/156615340/60bd1847-94c1-404e-8064-5382dc849ccb">

## Manually Loading Census Tract Shapefiles
To avoid a similar problem with the Census Tract Shapefile, I did my conversion of Census Tract Shapefiles through R locally and then uploaded it to Big Query Manually. The script for my local transformation is available in this repository. I was able to do this in R largely because I have a lot more experience with R than Python. Also being able to work in a local environment helped me wrap my head around the transformation better. I had to do a couple of tries to make this conversion work, but my final jsonl file was able to be read by Big Query pretty quickly after I set the schema through Big Query's gooey data upload interface.

## Modelling in Big Query
After these datasets were loaded to Big Query, I performed some SQL analysis to join the information across the 3 tables. These transformations included standardizing the census tract ids, filtering only observations from 2021, and joining the data to geographic information. After my final table was created, I saved it to the project dataset and then started modelling. I following instructions as outlined in the Google Cloud Big Query documentation. The final predictions were once again joined to geographic data and saved to the project dataset. For reference, all of my queries are stored as sql queries in the Queries folder of the repository.

## Visualizing in Carto
With my geographic predictions loaded in Big Query after the modelling, I now moved to visualizing this dataset in Carto. The visualization of my predictions look as such. Here, my model says that rate of demolitions can either increase of decrease in a census tract. Tracts highlighted in Red are those that experienced a growth in demolitions while tracks highlighted in blue experienced a decrease as compared to the previous year. Below, the first image shows my predictions while the second image shows the actual distribution of demolitions across Philadelphia.

<img width="1468" alt="Screenshot 2024-05-07 at 5 40 22 PM" src="https://github.com/avavani/Final_Project/assets/156615340/5af3d691-fa4b-45a6-b204-35951b96ca3c">
<img width="1470" alt="Screenshot 2024-05-07 at 5 42 53 PM" src="https://github.com/avavani/Final_Project/assets/156615340/ba0e9b62-2775-4a86-9cac-fcb16aadaf32">

## Moving Ahead
This project was largely a way for me to familiarize myself with cloud infrastructure. This was also my first python project, which meant that significant time was taken up by needing to troubleshoot python issues that a more experienced user may not have had. I choose to work with Python rather than R due to the lack of resources available on uploading R Docker image as functions—I decided that even in the worst case scenario it would be better to fail at something where I can refer to external resources to understand and mend the problems. I do not regret making this shift, though I have to acknowledge that it was incredibly difficult and frustrating. The modelling was also something I was and still am uncomfortable with. While I was able to predict using Big Query, I would not be comfortable using the paltform again without more preparation. In the future, I would want to improve my Python skills so that I can add in more datasets to my pipeline as well as do my modelling as a function.
