CREATE OR REPLACE MODEL `is3107-group-8.MelvinML.boosted_tree_regressor_rent_price_model`
OPTIONS (
  model_type = 'boosted_tree_regressor',
  input_label_cols = ['rent'],
  max_iterations = 50,
  learn_rate = 0.1
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
