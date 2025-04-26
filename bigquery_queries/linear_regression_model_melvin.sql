CREATE OR REPLACE MODEL `is3107-group-8.MelvinML.rent_price_model`
OPTIONS (
  model_type = 'linear_reg',
  input_label_cols = ['rent'],
  data_split_method = 'AUTO_SPLIT'
) AS
SELECT
  CAST(propertyType AS STRING) AS propertyType,
  noOfBedRoom,
  CAST(rentYear AS STRING) AS rentYear,
  CAST(district AS STRING) AS district,
  distance_to_mrt,
  distance_to_bus_stop,
  num_bus_services,
  distance_to_mall,
  distance_to_school,
  distance_to_supermarket,
  distance_to_hawker,
  distance_to_healthcare,
  distance_to_activesg,
  distance_to_community_club,
  rent
FROM `is3107-group-8.MelvinML.final_hdb_features`
WHERE rent IS NOT NULL;

SELECT *
FROM ML.EVALUATE(MODEL `is3107-group-8.MelvinML.rent_price_model`);

