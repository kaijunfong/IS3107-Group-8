CREATE OR REPLACE TABLE `is3107-group-8.MelvinML.final_hdb_features` AS
WITH

-- Distance to nearest MRT
mrt_distance AS (
  SELECT
    h.id,
    MIN(ST_DISTANCE(
      ST_GEOGPOINT(CAST(h.longitude AS FLOAT64), CAST(h.latitude AS FLOAT64)),
      ST_GEOGPOINT(CAST(m.LONGITUDE AS FLOAT64), CAST(m.LATITUDE AS FLOAT64))
    )) AS distance_to_mrt
  FROM `is3107-group-8.rental.HDB_URA_table` h
  JOIN `is3107-group-8.transport.mrt_lrt_combined` m
  ON TRUE
  GROUP BY h.id
),

-- Closest bus stop with metadata
bus_stop_distance AS (
  SELECT
    h.id AS hdb_id,
    b.BusStopCode,
    b.Description,
    b.RoadName,
    ST_DISTANCE(
      ST_GEOGPOINT(CAST(h.longitude AS FLOAT64), CAST(h.latitude AS FLOAT64)),
      ST_GEOGPOINT(CAST(b.Longitude AS FLOAT64), CAST(b.Latitude AS FLOAT64))
    ) AS distance_to_bus_stop,
    b.NumBusServices AS num_bus_services
  FROM `is3107-group-8.rental.HDB_URA_table` h
  JOIN `is3107-group-8.transport.bus_stop_summary` b
  ON TRUE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY h.id
    ORDER BY ST_DISTANCE(
      ST_GEOGPOINT(CAST(h.longitude AS FLOAT64), CAST(h.latitude AS FLOAT64)),
      ST_GEOGPOINT(CAST(b.Longitude AS FLOAT64), CAST(b.Latitude AS FLOAT64))
    )
  ) = 1
),

-- Distance to nearest mall
mall_distance AS (
  SELECT
    h.id,
    MIN(ST_DISTANCE(
      ST_GEOGPOINT(CAST(h.longitude AS FLOAT64), CAST(h.latitude AS FLOAT64)),
      ST_GEOGPOINT(CAST(m.longitude AS FLOAT64), CAST(m.latitude AS FLOAT64))
    )) AS distance_to_mall
  FROM `is3107-group-8.rental.HDB_URA_table` h
  JOIN `is3107-group-8.amenities.malls_table` m
  ON TRUE
  GROUP BY h.id
),

-- Distance to nearest school
school_distance AS (
  SELECT
    h.id,
    MIN(ST_DISTANCE(
      ST_GEOGPOINT(CAST(h.longitude AS FLOAT64), CAST(h.latitude AS FLOAT64)),
      ST_GEOGPOINT(CAST(s.longitude AS FLOAT64), CAST(s.latitude AS FLOAT64))
    )) AS distance_to_school
  FROM `is3107-group-8.rental.HDB_URA_table` h
  JOIN `is3107-group-8.school_data.schools` s
  ON TRUE
  GROUP BY h.id
),

-- Distance to nearest supermarket
supermarket_distance AS (
  SELECT
    h.id,
    MIN(ST_DISTANCE(
      ST_GEOGPOINT(CAST(h.longitude AS FLOAT64), CAST(h.latitude AS FLOAT64)),
      ST_GEOGPOINT(CAST(s.longitude AS FLOAT64), CAST(s.latitude AS FLOAT64))
    )) AS distance_to_supermarket
  FROM `is3107-group-8.rental.HDB_URA_table` h
  JOIN `is3107-group-8.amenities.supermarket_table` s
  ON TRUE
  GROUP BY h.id
),

-- Distance to nearest hawker centre
hawker_distance AS (
  SELECT
    h.id,
    MIN(ST_DISTANCE(
      ST_GEOGPOINT(CAST(h.longitude AS FLOAT64), CAST(h.latitude AS FLOAT64)),
      ST_GEOGPOINT(CAST(hc.longitude AS FLOAT64), CAST(hc.latitude AS FLOAT64))
    )) AS distance_to_hawker
  FROM `is3107-group-8.rental.HDB_URA_table` h
  JOIN `is3107-group-8.amenities.hawkerCentre_table` hc
  ON TRUE
  GROUP BY h.id
),

-- Distance to nearest healthcare
healthcare_distance AS (
  SELECT
    h.id,
    MIN(ST_DISTANCE(
      ST_GEOGPOINT(CAST(h.longitude AS FLOAT64), CAST(h.latitude AS FLOAT64)),
      ST_GEOGPOINT(CAST(hc.longitude AS FLOAT64), CAST(hc.latitude AS FLOAT64))
    )) AS distance_to_healthcare
  FROM `is3107-group-8.rental.HDB_URA_table` h
  JOIN `is3107-group-8.amenities.healthcare_table` hc
  ON TRUE
  GROUP BY h.id
),

-- Distance to nearest ActiveSG facility
activesg_distance AS (
  SELECT
    h.id,
    MIN(ST_DISTANCE(
      ST_GEOGPOINT(CAST(h.longitude AS FLOAT64), CAST(h.latitude AS FLOAT64)),
      ST_GEOGPOINT(CAST(a.longitude AS FLOAT64), CAST(a.latitude AS FLOAT64))
    )) AS distance_to_activesg
  FROM `is3107-group-8.rental.HDB_URA_table` h
  JOIN `is3107-group-8.amenities.activeSG_table` a
  ON TRUE
  GROUP BY h.id
),

-- Distance to nearest community club
cc_distance AS (
  SELECT
    h.id,
    MIN(ST_DISTANCE(
      ST_GEOGPOINT(CAST(h.longitude AS FLOAT64), CAST(h.latitude AS FLOAT64)),
      ST_GEOGPOINT(CAST(c.longitude AS FLOAT64), CAST(c.latitude AS FLOAT64))
    )) AS distance_to_community_club
  FROM `is3107-group-8.rental.HDB_URA_table` h
  JOIN `is3107-group-8.amenities.communityClub_table` c
  ON TRUE
  GROUP BY h.id
)

-- Final join across all CTEs
SELECT
  h.*,
  mrt.distance_to_mrt,
  bs.distance_to_bus_stop,
  bs.num_bus_services,
  bs.BusStopCode,
  bs.Description AS nearest_bus_stop_desc,
  bs.RoadName AS nearest_bus_stop_road,
  m.distance_to_mall,
  sc.distance_to_school,
  sm.distance_to_supermarket,
  hk.distance_to_hawker,
  hc.distance_to_healthcare,
  a.distance_to_activesg,
  cc.distance_to_community_club
FROM `is3107-group-8.rental.HDB_URA_table` h
LEFT JOIN mrt_distance mrt ON h.id = mrt.id
LEFT JOIN bus_stop_distance bs ON h.id = bs.hdb_id
LEFT JOIN mall_distance m ON h.id = m.id
LEFT JOIN school_distance sc ON h.id = sc.id
LEFT JOIN supermarket_distance sm ON h.id = sm.id
LEFT JOIN hawker_distance hk ON h.id = hk.id
LEFT JOIN healthcare_distance hc ON h.id = hc.id
LEFT JOIN activesg_distance a ON h.id = a.id
LEFT JOIN cc_distance cc ON h.id = cc.id;
