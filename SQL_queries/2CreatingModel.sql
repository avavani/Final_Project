CREATE OR REPLACE MODEL `final_project.demolition_prediction_2024`
OPTIONS(model_type='linear_reg', input_label_cols=['demolitions_2023']) AS
SELECT
  demolitions_2021,
  demolitions_2022,
  demolitions_2023,  
  Total_Population,
  White_Alone,
  Black_or_African_American_Alone,
  Asian_Alone,
  Moved_Same_County,
  Moved_Different_County_Same_State,
  Moved_From_Abroad,
  Total_Household_Income,
  Educational_Attainment,
  Median_Age,
  Number_of_Children,
  Family_Type,
  Marital_Status,
  Building_Type,
  Tenure_Status,
  Number_of_Vehicles,
  Number_of_Bedrooms,
  Condominium_Status,
  Year_Built,
  Heating_Fuel,
  Unit_Size,
  State,
  County,
  Census_Tract
FROM
  `final_project.censusdemo`;
