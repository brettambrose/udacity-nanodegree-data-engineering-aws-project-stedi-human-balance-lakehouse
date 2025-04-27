WITH data_validation AS (
-- Landing
SELECT '1- landing' AS zone_location,'customer_landing' AS table_name, COUNT(1) AS record_ct, 956 AS expected_ct FROM customer_landing
UNION ALL 
SELECT '1- landing', 'accelerometer_landing', COUNT(1), 81273 FROM accelerometer_landing
UNION ALL
SELECT '1- landing', 'step_trainer_landing', COUNT(1), 28680 FROM step_trainer_landing

-- Trusted
UNION ALL
SELECT '2- trusted', 'customer_trusted', COUNT(1), 482 FROM customer_trusted
UNION ALL 
SELECT '2- trusted', 'accelerometer_trusted', COUNT(1), 40981 FROM accelerometer_trusted
UNION ALL
SELECT '2- trusted', 'step_trainer_trusted', COUNT(1), 14460 FROM step_trainer_trusted

-- Curated
UNION ALL
SELECT '3- curated', 'customer_curated' AS table_name, COUNT(1), 482 FROM customer_curated
UNION ALL
SELECT '3- curated', 'machine_learning_curated', COUNT(1), 43681 FROM machine_learning_curated
)
SELECT 
 *,
 CASE 
  WHEN record_ct = expected_ct THEN 'TRUE'
  ELSE 'FALSE' 
 END AS rec_ct_match
FROM data_validation 
ORDER BY zone_location
;