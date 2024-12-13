import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node trip_summary
trip_summary_node1734057918586 = glueContext.create_dynamic_frame.from_options(
    connection_type = "postgresql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "stellantis.trip_summary",
        "connectionName": "sf-data-lake-ubi",
    },
    transformation_ctx = "trip_summary_node1734057918586"
)

# Script generated for node stellantis_contract
stellantis_contract_node1734056250317 = glueContext.create_dynamic_frame.from_options(
    connection_type = "postgresql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "notification_management.stellantis_contract",
        "connectionName": "sf-data-lake-ubi-jdbc",
    },
    transformation_ctx = "stellantis_contract_node1734056250317"
)

# Script generated for node TRIP_SUMMARY
TRIP_SUMMARY_node1734057920973 = glueContext.write_dynamic_frame.from_options(frame=trip_summary_node1734057918586, connection_type="snowflake", connection_options={"autopushdown": "on", "postactions": "BEGIN; MERGE INTO STELLANTIS.TRIP_SUMMARY USING STELLANTIS.TRIP_SUMMARY_temp_hz5bcc ON TRIP_SUMMARY.id = TRIP_SUMMARY_temp_hz5bcc.id WHEN MATCHED THEN UPDATE SET id = TRIP_SUMMARY_temp_hz5bcc.id, user_id = TRIP_SUMMARY_temp_hz5bcc.user_id, device_id = TRIP_SUMMARY_temp_hz5bcc.device_id, start_time_utc = TRIP_SUMMARY_temp_hz5bcc.start_time_utc, end_time_utc = TRIP_SUMMARY_temp_hz5bcc.end_time_utc, last_update_time_utc = TRIP_SUMMARY_temp_hz5bcc.last_update_time_utc, start_timezone = TRIP_SUMMARY_temp_hz5bcc.start_timezone, start_location_name = TRIP_SUMMARY_temp_hz5bcc.start_location_name, start_postal_code = TRIP_SUMMARY_temp_hz5bcc.start_postal_code, start_country = TRIP_SUMMARY_temp_hz5bcc.start_country, start_state = TRIP_SUMMARY_temp_hz5bcc.start_state, start_city = TRIP_SUMMARY_temp_hz5bcc.start_city, end_timezone = TRIP_SUMMARY_temp_hz5bcc.end_timezone, end_location_name = TRIP_SUMMARY_temp_hz5bcc.end_location_name, end_postal_code = TRIP_SUMMARY_temp_hz5bcc.end_postal_code, end_country = TRIP_SUMMARY_temp_hz5bcc.end_country, end_state = TRIP_SUMMARY_temp_hz5bcc.end_state, end_city = TRIP_SUMMARY_temp_hz5bcc.end_city, trip_type = TRIP_SUMMARY_temp_hz5bcc.trip_type, trip_client_type = TRIP_SUMMARY_temp_hz5bcc.trip_client_type, total_event_count = TRIP_SUMMARY_temp_hz5bcc.total_event_count, distance_miles = TRIP_SUMMARY_temp_hz5bcc.distance_miles, duration_millisecond = TRIP_SUMMARY_temp_hz5bcc.duration_millisecond, moving_duration = TRIP_SUMMARY_temp_hz5bcc.moving_duration, night_duration = TRIP_SUMMARY_temp_hz5bcc.night_duration, night_moving_duration = TRIP_SUMMARY_temp_hz5bcc.night_moving_duration, night_distance = TRIP_SUMMARY_temp_hz5bcc.night_distance, log_anonymized = TRIP_SUMMARY_temp_hz5bcc.log_anonymized, max_speed_mph = TRIP_SUMMARY_temp_hz5bcc.max_speed_mph, avg_speed_mph = TRIP_SUMMARY_temp_hz5bcc.avg_speed_mph, speed_point_count = TRIP_SUMMARY_temp_hz5bcc.speed_point_count, stopped_point_count = TRIP_SUMMARY_temp_hz5bcc.stopped_point_count, high_speed_point_count = TRIP_SUMMARY_temp_hz5bcc.high_speed_point_count, trips_in_homearea = TRIP_SUMMARY_temp_hz5bcc.trips_in_homearea, event_distribution = TRIP_SUMMARY_temp_hz5bcc.event_distribution, cloud_drive_score = TRIP_SUMMARY_temp_hz5bcc.cloud_drive_score, trip_safety_score = TRIP_SUMMARY_temp_hz5bcc.trip_safety_score, trip_safety_rating = TRIP_SUMMARY_temp_hz5bcc.trip_safety_rating, safety_score_version = TRIP_SUMMARY_temp_hz5bcc.safety_score_version, gps_probe_polyline = TRIP_SUMMARY_temp_hz5bcc.gps_probe_polyline, gps_probe_speed = TRIP_SUMMARY_temp_hz5bcc.gps_probe_speed, gps_probe_time = TRIP_SUMMARY_temp_hz5bcc.gps_probe_time, gps_probe_predicted = TRIP_SUMMARY_temp_hz5bcc.gps_probe_predicted, trip_label = TRIP_SUMMARY_temp_hz5bcc.trip_label, driver_passenger_mode = TRIP_SUMMARY_temp_hz5bcc.driver_passenger_mode, driver_passenger_confidence = TRIP_SUMMARY_temp_hz5bcc.driver_passenger_confidence, transportation_mode = TRIP_SUMMARY_temp_hz5bcc.transportation_mode, transportation_confidence = TRIP_SUMMARY_temp_hz5bcc.transportation_confidence, start_location_lat = TRIP_SUMMARY_temp_hz5bcc.start_location_lat, start_location_lon = TRIP_SUMMARY_temp_hz5bcc.start_location_lon, end_location_lat = TRIP_SUMMARY_temp_hz5bcc.end_location_lat, end_location_lon = TRIP_SUMMARY_temp_hz5bcc.end_location_lon, trip_end_state = TRIP_SUMMARY_temp_hz5bcc.trip_end_state, event_safety_details = TRIP_SUMMARY_temp_hz5bcc.event_safety_details, event_safety_distribution = TRIP_SUMMARY_temp_hz5bcc.event_safety_distribution, is_matched = TRIP_SUMMARY_temp_hz5bcc.is_matched, channel_client_id = TRIP_SUMMARY_temp_hz5bcc.channel_client_id, trip_selection_status = TRIP_SUMMARY_temp_hz5bcc.trip_selection_status WHEN NOT MATCHED THEN INSERT VALUES (TRIP_SUMMARY_temp_hz5bcc.id, TRIP_SUMMARY_temp_hz5bcc.user_id, TRIP_SUMMARY_temp_hz5bcc.device_id, TRIP_SUMMARY_temp_hz5bcc.start_time_utc, TRIP_SUMMARY_temp_hz5bcc.end_time_utc, TRIP_SUMMARY_temp_hz5bcc.last_update_time_utc, TRIP_SUMMARY_temp_hz5bcc.start_timezone, TRIP_SUMMARY_temp_hz5bcc.start_location_name, TRIP_SUMMARY_temp_hz5bcc.start_postal_code, TRIP_SUMMARY_temp_hz5bcc.start_country, TRIP_SUMMARY_temp_hz5bcc.start_state, TRIP_SUMMARY_temp_hz5bcc.start_city, TRIP_SUMMARY_temp_hz5bcc.end_timezone, TRIP_SUMMARY_temp_hz5bcc.end_location_name, TRIP_SUMMARY_temp_hz5bcc.end_postal_code, TRIP_SUMMARY_temp_hz5bcc.end_country, TRIP_SUMMARY_temp_hz5bcc.end_state, TRIP_SUMMARY_temp_hz5bcc.end_city, TRIP_SUMMARY_temp_hz5bcc.trip_type, TRIP_SUMMARY_temp_hz5bcc.trip_client_type, TRIP_SUMMARY_temp_hz5bcc.total_event_count, TRIP_SUMMARY_temp_hz5bcc.distance_miles, TRIP_SUMMARY_temp_hz5bcc.duration_millisecond, TRIP_SUMMARY_temp_hz5bcc.moving_duration, TRIP_SUMMARY_temp_hz5bcc.night_duration, TRIP_SUMMARY_temp_hz5bcc.night_moving_duration, TRIP_SUMMARY_temp_hz5bcc.night_distance, TRIP_SUMMARY_temp_hz5bcc.log_anonymized, TRIP_SUMMARY_temp_hz5bcc.max_speed_mph, TRIP_SUMMARY_temp_hz5bcc.avg_speed_mph, TRIP_SUMMARY_temp_hz5bcc.speed_point_count, TRIP_SUMMARY_temp_hz5bcc.stopped_point_count, TRIP_SUMMARY_temp_hz5bcc.high_speed_point_count, TRIP_SUMMARY_temp_hz5bcc.trips_in_homearea, TRIP_SUMMARY_temp_hz5bcc.event_distribution, TRIP_SUMMARY_temp_hz5bcc.cloud_drive_score, TRIP_SUMMARY_temp_hz5bcc.trip_safety_score, TRIP_SUMMARY_temp_hz5bcc.trip_safety_rating, TRIP_SUMMARY_temp_hz5bcc.safety_score_version, TRIP_SUMMARY_temp_hz5bcc.gps_probe_polyline, TRIP_SUMMARY_temp_hz5bcc.gps_probe_speed, TRIP_SUMMARY_temp_hz5bcc.gps_probe_time, TRIP_SUMMARY_temp_hz5bcc.gps_probe_predicted, TRIP_SUMMARY_temp_hz5bcc.trip_label, TRIP_SUMMARY_temp_hz5bcc.driver_passenger_mode, TRIP_SUMMARY_temp_hz5bcc.driver_passenger_confidence, TRIP_SUMMARY_temp_hz5bcc.transportation_mode, TRIP_SUMMARY_temp_hz5bcc.transportation_confidence, TRIP_SUMMARY_temp_hz5bcc.start_location_lat, TRIP_SUMMARY_temp_hz5bcc.start_location_lon, TRIP_SUMMARY_temp_hz5bcc.end_location_lat, TRIP_SUMMARY_temp_hz5bcc.end_location_lon, TRIP_SUMMARY_temp_hz5bcc.trip_end_state, TRIP_SUMMARY_temp_hz5bcc.event_safety_details, TRIP_SUMMARY_temp_hz5bcc.event_safety_distribution, TRIP_SUMMARY_temp_hz5bcc.is_matched, TRIP_SUMMARY_temp_hz5bcc.channel_client_id, TRIP_SUMMARY_temp_hz5bcc.trip_selection_status); DROP TABLE IF EXISTS STELLANTIS.TRIP_SUMMARY_temp_hz5bcc; COMMIT;", "dbtable": "TRIP_SUMMARY_temp_hz5bcc", "connectionName": "Snowflake connection - Corp", "preactions": "CREATE TABLE IF NOT EXISTS STELLANTIS.TRIP_SUMMARY (id string, user_id string, device_id string, start_time_utc timestamp, end_time_utc timestamp, last_update_time_utc timestamp, start_timezone string, start_location_name string, start_postal_code string, start_country string, start_state string, start_city string, end_timezone string, end_location_name string, end_postal_code string, end_country string, end_state string, end_city string, trip_type string, trip_client_type int, total_event_count int, distance_miles double, duration_millisecond bigint, moving_duration int, night_duration int, night_moving_duration int, night_distance double, log_anonymized boolean, max_speed_mph double, avg_speed_mph double, speed_point_count int, stopped_point_count int, high_speed_point_count int, trips_in_homearea boolean, event_distribution string, cloud_drive_score string, trip_safety_score double, trip_safety_rating int, safety_score_version string, gps_probe_polyline string, gps_probe_speed array, gps_probe_time array, gps_probe_predicted array, trip_label string, driver_passenger_mode string, driver_passenger_confidence double, transportation_mode string, transportation_confidence double, start_location_lat double, start_location_lon double, end_location_lat double, end_location_lon double, trip_end_state string, event_safety_details string, event_safety_distribution string, is_matched boolean, channel_client_id string, trip_selection_status string); DROP TABLE IF EXISTS STELLANTIS.TRIP_SUMMARY_temp_hz5bcc; CREATE TABLE IF NOT EXISTS STELLANTIS.TRIP_SUMMARY_temp_hz5bcc (id string, user_id string, device_id string, start_time_utc timestamp, end_time_utc timestamp, last_update_time_utc timestamp, start_timezone string, start_location_name string, start_postal_code string, start_country string, start_state string, start_city string, end_timezone string, end_location_name string, end_postal_code string, end_country string, end_state string, end_city string, trip_type string, trip_client_type int, total_event_count int, distance_miles double, duration_millisecond bigint, moving_duration int, night_duration int, night_moving_duration int, night_distance double, log_anonymized boolean, max_speed_mph double, avg_speed_mph double, speed_point_count int, stopped_point_count int, high_speed_point_count int, trips_in_homearea boolean, event_distribution string, cloud_drive_score string, trip_safety_score double, trip_safety_rating int, safety_score_version string, gps_probe_polyline string, gps_probe_speed array, gps_probe_time array, gps_probe_predicted array, trip_label string, driver_passenger_mode string, driver_passenger_confidence double, transportation_mode string, transportation_confidence double, start_location_lat double, start_location_lon double, end_location_lat double, end_location_lon double, trip_end_state string, event_safety_details string, event_safety_distribution string, is_matched boolean, channel_client_id string, trip_selection_status string);", "sfDatabase": "TELEMATICS", "sfSchema": "STELLANTIS"}, transformation_ctx="TRIP_SUMMARY_node1734057920973")

# Script generated for node STELLANTIS_CONTRACT
STELLANTIS_CONTRACT_node1734056254672 = glueContext.write_dynamic_frame.from_options(frame=stellantis_contract_node1734056250317, connection_type="snowflake", connection_options={"autopushdown": "on", "postactions": "BEGIN; MERGE INTO STELLANTIS.STELLANTIS_CONTRACT USING STELLANTIS.STELLANTIS_CONTRACT_temp_gtd7cp ON STELLANTIS_CONTRACT.subscription_id = STELLANTIS_CONTRACT_temp_gtd7cp.subscription_id WHEN MATCHED THEN UPDATE SET subscription_id = STELLANTIS_CONTRACT_temp_gtd7cp.subscription_id, contract_id = STELLANTIS_CONTRACT_temp_gtd7cp.contract_id, vin = STELLANTIS_CONTRACT_temp_gtd7cp.vin, device_id = STELLANTIS_CONTRACT_temp_gtd7cp.device_id, from_time = STELLANTIS_CONTRACT_temp_gtd7cp.from_time, to_time = STELLANTIS_CONTRACT_temp_gtd7cp.to_time, create_time = STELLANTIS_CONTRACT_temp_gtd7cp.create_time, update_time = STELLANTIS_CONTRACT_temp_gtd7cp.update_time, status = STELLANTIS_CONTRACT_temp_gtd7cp.status, data_exist = STELLANTIS_CONTRACT_temp_gtd7cp.data_exist WHEN NOT MATCHED THEN INSERT VALUES (STELLANTIS_CONTRACT_temp_gtd7cp.subscription_id, STELLANTIS_CONTRACT_temp_gtd7cp.contract_id, STELLANTIS_CONTRACT_temp_gtd7cp.vin, STELLANTIS_CONTRACT_temp_gtd7cp.device_id, STELLANTIS_CONTRACT_temp_gtd7cp.from_time, STELLANTIS_CONTRACT_temp_gtd7cp.to_time, STELLANTIS_CONTRACT_temp_gtd7cp.create_time, STELLANTIS_CONTRACT_temp_gtd7cp.update_time, STELLANTIS_CONTRACT_temp_gtd7cp.status, STELLANTIS_CONTRACT_temp_gtd7cp.data_exist); DROP TABLE IF EXISTS STELLANTIS.STELLANTIS_CONTRACT_temp_gtd7cp; COMMIT;", "dbtable": "STELLANTIS_CONTRACT_temp_gtd7cp", "connectionName": "Snowflake connection - Corp", "preactions": "CREATE TABLE IF NOT EXISTS STELLANTIS.STELLANTIS_CONTRACT (subscription_id string, contract_id string, vin string, device_id string, from_time timestamp, to_time timestamp, create_time timestamp, update_time timestamp, status string, data_exist boolean); DROP TABLE IF EXISTS STELLANTIS.STELLANTIS_CONTRACT_temp_gtd7cp; CREATE TABLE IF NOT EXISTS STELLANTIS.STELLANTIS_CONTRACT_temp_gtd7cp (subscription_id string, contract_id string, vin string, device_id string, from_time timestamp, to_time timestamp, create_time timestamp, update_time timestamp, status string, data_exist boolean);", "sfDatabase": "TELEMATICS", "sfSchema": "STELLANTIS"}, transformation_ctx="STELLANTIS_CONTRACT_node1734056254672")

job.commit()