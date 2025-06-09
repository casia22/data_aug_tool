CREATE TABLE `决策规划表_new` (
  `vehicle_vin_code` int NOT NULL,
  `data_type` varchar(50) DEFAULT NULL,
  `data_date` date NOT NULL,
  `create_time` datetime DEFAULT NULL,
  `timestamp` varchar(30) NOT NULL,
  `turn_light` varchar(20) DEFAULT NULL,
  `hazard_light` varchar(20) DEFAULT NULL,
  `p_id` int NOT NULL,
  `x` double DEFAULT NULL,
  `y` double DEFAULT NULL,
  `z` double DEFAULT NULL,
  `o_x` double DEFAULT NULL,
  `o_y` double DEFAULT NULL,
  `o_z` double DEFAULT NULL,
  `o_w` double DEFAULT NULL,
  `longitudinal_velocity_mps` double DEFAULT NULL,
  `lateral_velocity_mps` double DEFAULT NULL,
  `acceleration_mps2` double DEFAULT NULL,
  `heading_rate_rps` double DEFAULT NULL,
  `front_wheel_angle_rad` double DEFAULT NULL,
  PRIMARY KEY (`timestamp`,`p_id`,`data_date`),
  KEY `idx_date` (`data_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
PARTITION BY HASH(TO_DAYS(data_date))
PARTITIONS 32; 