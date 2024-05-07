CREATE OR REPLACE TABLE `testing-404600.final_project.predictionsgeog` AS
WITH TractsFiltered AS (
  SELECT
    geog,
    CAST(
      IF(STRPOS(NAME, '.') = 0,
         CONCAT(NAME, '00'), 
         REPLACE(NAME, '.', '')
      ) AS STRING
    ) AS transformed_name
  FROM
    `final_project.tracts`
  WHERE
    COUNTYFP = '101'
),
JoinedPrediction AS (
  SELECT
    a.*,
    t.geog
  FROM
    `testing-404600.final_project.predictions` a
  JOIN
    TractsFiltered t
  ON
    a.Census_Tract = t.transformed_name
)

SELECT * FROM JoinedPrediction;