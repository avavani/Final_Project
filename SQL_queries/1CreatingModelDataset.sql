CREATE OR REPLACE TABLE final_project.censusdemo AS
WITH FilteredData AS (
  SELECT
    censustract,
    EXTRACT(YEAR FROM TIMESTAMP(start_date)) AS year
  FROM
    `final_project.demos`
  WHERE
    EXTRACT(YEAR FROM TIMESTAMP(start_date)) IN (2021, 2022, 2023)
),
DataAdjust AS (
  SELECT
    CAST(
      IF(STRPOS(censustract, '.') = 0,
         censustract || '00',
         REPLACE(censustract, '.', '')
      ) AS INT64
    ) AS adjusted_censustract,
    COUNTIF(year = 2021) AS demolitions_2021,
    COUNTIF(year = 2022) AS demolitions_2022,
    COUNTIF(year = 2023) AS demolitions_2023
  FROM
    FilteredData
  GROUP BY
    adjusted_censustract
),
JoinedACS AS (
  SELECT
    a.adjusted_censustract,
    a.demolitions_2021,
    a.demolitions_2022,
    a.demolitions_2023,
    b.*
  FROM
    DataAdjust a
  JOIN
    `final_project.acs2021` b
  ON
    CAST(a.adjusted_censustract AS STRING) = b.Census_Tract
),
TractsFiltered AS (
  SELECT
    *,
    CAST(
      IF(STRPOS(NAME, '.') = 0,
         CONCAT(NAME, '00'), 
         REPLACE(NAME, '.', '') 
      ) AS INT64
    ) AS transformed_name
  FROM
    `final_project.tracts`
  WHERE
    COUNTYFP = '101'
)
SELECT
  j.*,
  t.geog  
FROM
  JoinedACS j
JOIN
  TractsFiltered t
ON
  j.adjusted_censustract = t.transformed_name;