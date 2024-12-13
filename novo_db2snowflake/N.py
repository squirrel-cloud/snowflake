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

# Script generated for node NOVO_METADATA_DICTIONARY
NOVO_METADATA_DICTIONARY_node1733724995760 = glueContext.create_dynamic_frame.from_options(
    connection_type = "sqlserver",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "dbo.NOVO_METADATA_DICTIONARY",
        "connectionName": "sf-NovoDB-UAT",
    },
    transformation_ctx = "NOVO_METADATA_DICTIONARY_node1733724995760"
)

# Script generated for node NOVO_BILLING
NOVO_BILLING_node1733710900324 = glueContext.create_dynamic_frame.from_options(
    connection_type = "sqlserver",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "dbo.NOVO_BILLING",
        "connectionName": "sf-NovoDB-UAT",
    },
    transformation_ctx = "NOVO_BILLING_node1733710900324"
)

# Script generated for node NOVO_PRODUCT
NOVO_PRODUCT_node1733724547440 = glueContext.create_dynamic_frame.from_options(
    connection_type = "sqlserver",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "dbo.NOVO_PRODUCT",
        "connectionName": "sf-NovoDB-UAT",
    },
    transformation_ctx = "NOVO_PRODUCT_node1733724547440"
)

# Script generated for node NOVO_POLICY
NOVO_POLICY_node1733723415901 = glueContext.create_dynamic_frame.from_options(
    connection_type = "sqlserver",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "dbo.NOVO_POLICY",
        "connectionName": "sf-NovoDB-UAT",
    },
    transformation_ctx = "NOVO_POLICY_node1733723415901"
)

# Script generated for node NOVO_VEHICLE
NOVO_VEHICLE_node1733724233860 = glueContext.create_dynamic_frame.from_options(
    connection_type = "sqlserver",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "dbo.NOVO_VEHICLE",
        "connectionName": "sf-NovoDB-UAT",
    },
    transformation_ctx = "NOVO_VEHICLE_node1733724233860"
)

# Script generated for node NOVO_METADATA_DICTIONARY
NOVO_METADATA_DICTIONARY_node1733809207022 = ApplyMapping.apply(frame=NOVO_METADATA_DICTIONARY_node1733724995760, mappings=[("gid", "bigint", "gid", "bigint"), ("product_id", "bigint", "product_id", "bigint"), ("type", "string", "type", "string"), ("name", "string", "name", "string"), ("description", "string", "description", "string")], transformation_ctx="NOVO_METADATA_DICTIONARY_node1733809207022")

# Script generated for node NOVO_PRODUCT
NOVO_PRODUCT_node1733801650810 = ApplyMapping.apply(frame=NOVO_PRODUCT_node1733724547440, mappings=[("product_id", "bigint", "product_id", "bigint"), ("name", "string", "name", "string"), ("state", "string", "state", "string"), ("carrier", "string", "carrier", "string"), ("description", "string", "description", "string")], transformation_ctx="NOVO_PRODUCT_node1733801650810")

# Script generated for node NOVO_VEHICLE
NOVO_VEHICLE_node1733808450720 = ApplyMapping.apply(frame=NOVO_VEHICLE_node1733724233860, mappings=[("gid", "bigint", "gid", "bigint"), ("vehicle_id", "string", "vehicle_id", "string"), ("vin", "string", "vin", "string"), ("policy_id", "string", "policy_id", "string"), ("metadata_dictionary_vehicle_primary_use_type", "bigint", "metadata_dictionary_vehicle_primary_use_type", "bigint"), ("metadata_dictionary_vehicle_ownership_type", "bigint", "metadata_dictionary_vehicle_ownership_type", "bigint"), ("vehicle_dictionary_id", "bigint", "vehicle_dictionary_id", "bigint"), ("is_ride_sharing", "string", "is_ride_sharing", "string"), ("vehicle_created_at", "timestamp", "vehicle_created_at", "timestamp"), ("vehicle_deleted_at", "timestamp", "vehicle_deleted_at", "timestamp"), ("vehicle_deleted_ind", "string", "vehicle_deleted_ind", "string"), ("garage_address_line1", "string", "garage_address_line1", "string"), ("garage_address_line2", "string", "garage_address_line2", "string"), ("garage_address_city", "string", "garage_address_city", "string"), ("garage_address_county", "string", "garage_address_county", "string"), ("garage_address_state", "string", "garage_address_state", "string"), ("garage_address_zipcode", "string", "garage_address_zipcode", "string"), ("garage_address_country", "string", "garage_address_country", "string"), ("event_id", "string", "event_id", "string")], transformation_ctx="NOVO_VEHICLE_node1733808450720")

# Script generated for node NOVO_BILLING
NOVO_BILLING_node1733714026709 = glueContext.write_dynamic_frame.from_options(frame=NOVO_BILLING_node1733710900324, connection_type="snowflake", connection_options={"autopushdown": "on", "postactions": "BEGIN; MERGE INTO insight.novo_billing USING insight.novo_billing_temp_mkymw5 ON novo_billing.account_id = novo_billing_temp_mkymw5.account_id AND novo_billing.billing_id = novo_billing_temp_mkymw5.billing_id AND novo_billing.metadata_dictionary_billing_type = novo_billing_temp_mkymw5.metadata_dictionary_billing_type WHEN MATCHED THEN UPDATE SET gid = novo_billing_temp_mkymw5.gid, account_id = novo_billing_temp_mkymw5.account_id, account_reference = novo_billing_temp_mkymw5.account_reference, billing_id = novo_billing_temp_mkymw5.billing_id, metadata_dictionary_billing_type = novo_billing_temp_mkymw5.metadata_dictionary_billing_type, policy_term_id = novo_billing_temp_mkymw5.policy_term_id, policy_term_effective_date = novo_billing_temp_mkymw5.policy_term_effective_date, policy_term_expiration_date = novo_billing_temp_mkymw5.policy_term_expiration_date, item_id = novo_billing_temp_mkymw5.item_id, posted_at = novo_billing_temp_mkymw5.posted_at, processed_at = novo_billing_temp_mkymw5.processed_at, comments = novo_billing_temp_mkymw5.comments, amount = novo_billing_temp_mkymw5.amount, load_date = novo_billing_temp_mkymw5.load_date WHEN NOT MATCHED THEN INSERT VALUES (novo_billing_temp_mkymw5.gid, novo_billing_temp_mkymw5.account_id, novo_billing_temp_mkymw5.account_reference, novo_billing_temp_mkymw5.billing_id, novo_billing_temp_mkymw5.metadata_dictionary_billing_type, novo_billing_temp_mkymw5.policy_term_id, novo_billing_temp_mkymw5.policy_term_effective_date, novo_billing_temp_mkymw5.policy_term_expiration_date, novo_billing_temp_mkymw5.item_id, novo_billing_temp_mkymw5.posted_at, novo_billing_temp_mkymw5.processed_at, novo_billing_temp_mkymw5.comments, novo_billing_temp_mkymw5.amount, novo_billing_temp_mkymw5.load_date); DROP TABLE IF EXISTS insight.novo_billing_temp_mkymw5; COMMIT;", "dbtable": "novo_billing_temp_mkymw5", "connectionName": "Snowflake connection - Corp", "preactions": "CREATE TABLE IF NOT EXISTS insight.novo_billing (gid bigint, account_id string, account_reference string, billing_id string, metadata_dictionary_billing_type bigint, policy_term_id string, policy_term_effective_date timestamp, policy_term_expiration_date timestamp, item_id bigint, posted_at timestamp, processed_at timestamp, comments string, amount string, load_date string); DROP TABLE IF EXISTS insight.novo_billing_temp_mkymw5; CREATE TABLE IF NOT EXISTS insight.novo_billing_temp_mkymw5 (gid bigint, account_id string, account_reference string, billing_id string, metadata_dictionary_billing_type bigint, policy_term_id string, policy_term_effective_date timestamp, policy_term_expiration_date timestamp, item_id bigint, posted_at timestamp, processed_at timestamp, comments string, amount string, load_date string);", "sfDatabase": "novo_services", "sfSchema": "insight"}, transformation_ctx="NOVO_BILLING_node1733714026709")

# Script generated for node NOVO_POLICY
NOVO_POLICY_node1733723453276 = glueContext.write_dynamic_frame.from_options(frame=NOVO_POLICY_node1733723415901, connection_type="snowflake", connection_options={"autopushdown": "on", "postactions": "BEGIN; MERGE INTO insight.NOVO_POLICY USING insight.NOVO_POLICY_temp_3bv6mu ON NOVO_POLICY.policy_id = NOVO_POLICY_temp_3bv6mu.policy_id WHEN MATCHED THEN UPDATE SET gid = NOVO_POLICY_temp_3bv6mu.gid, policy_id = NOVO_POLICY_temp_3bv6mu.policy_id, product_id = NOVO_POLICY_temp_3bv6mu.product_id, metadata_dictionary_status_type = NOVO_POLICY_temp_3bv6mu.metadata_dictionary_status_type, effective_date = NOVO_POLICY_temp_3bv6mu.effective_date, expiration_date = NOVO_POLICY_temp_3bv6mu.expiration_date, inception_date = NOVO_POLICY_temp_3bv6mu.inception_date, rewrite_indicator = NOVO_POLICY_temp_3bv6mu.rewrite_indicator, metadata_dictionary_rewrite_indicator_type = NOVO_POLICY_temp_3bv6mu.metadata_dictionary_rewrite_indicator_type, prior_insurance_indicator = NOVO_POLICY_temp_3bv6mu.prior_insurance_indicator, prior_insurance_classification = NOVO_POLICY_temp_3bv6mu.prior_insurance_classification, agent_id = NOVO_POLICY_temp_3bv6mu.agent_id, metadata_dictionary_pay_plan = NOVO_POLICY_temp_3bv6mu.metadata_dictionary_pay_plan, metadata_dictionary_quote_method = NOVO_POLICY_temp_3bv6mu.metadata_dictionary_quote_method, endorsement_number = NOVO_POLICY_temp_3bv6mu.endorsement_number, term_number = NOVO_POLICY_temp_3bv6mu.term_number, term_in_months = NOVO_POLICY_temp_3bv6mu.term_in_months, full_term_premium = NOVO_POLICY_temp_3bv6mu.full_term_premium, total_prorated_premium = NOVO_POLICY_temp_3bv6mu.total_prorated_premium, ubi_opt_in = NOVO_POLICY_temp_3bv6mu.ubi_opt_in, event_id = NOVO_POLICY_temp_3bv6mu.event_id WHEN NOT MATCHED THEN INSERT VALUES (NOVO_POLICY_temp_3bv6mu.gid, NOVO_POLICY_temp_3bv6mu.policy_id, NOVO_POLICY_temp_3bv6mu.product_id, NOVO_POLICY_temp_3bv6mu.metadata_dictionary_status_type, NOVO_POLICY_temp_3bv6mu.effective_date, NOVO_POLICY_temp_3bv6mu.expiration_date, NOVO_POLICY_temp_3bv6mu.inception_date, NOVO_POLICY_temp_3bv6mu.rewrite_indicator, NOVO_POLICY_temp_3bv6mu.metadata_dictionary_rewrite_indicator_type, NOVO_POLICY_temp_3bv6mu.prior_insurance_indicator, NOVO_POLICY_temp_3bv6mu.prior_insurance_classification, NOVO_POLICY_temp_3bv6mu.agent_id, NOVO_POLICY_temp_3bv6mu.metadata_dictionary_pay_plan, NOVO_POLICY_temp_3bv6mu.metadata_dictionary_quote_method, NOVO_POLICY_temp_3bv6mu.endorsement_number, NOVO_POLICY_temp_3bv6mu.term_number, NOVO_POLICY_temp_3bv6mu.term_in_months, NOVO_POLICY_temp_3bv6mu.full_term_premium, NOVO_POLICY_temp_3bv6mu.total_prorated_premium, NOVO_POLICY_temp_3bv6mu.ubi_opt_in, NOVO_POLICY_temp_3bv6mu.event_id); DROP TABLE IF EXISTS insight.NOVO_POLICY_temp_3bv6mu; COMMIT;", "dbtable": "NOVO_POLICY_temp_3bv6mu", "connectionName": "Snowflake connection - Corp", "preactions": "CREATE TABLE IF NOT EXISTS insight.NOVO_POLICY (gid bigint, policy_id string, product_id bigint, metadata_dictionary_status_type bigint, effective_date timestamp, expiration_date timestamp, inception_date timestamp, rewrite_indicator string, metadata_dictionary_rewrite_indicator_type bigint, prior_insurance_indicator string, prior_insurance_classification string, agent_id string, metadata_dictionary_pay_plan bigint, metadata_dictionary_quote_method bigint, endorsement_number int, term_number int, term_in_months int, full_term_premium decimal, total_prorated_premium decimal, ubi_opt_in string, event_id string); DROP TABLE IF EXISTS insight.NOVO_POLICY_temp_3bv6mu; CREATE TABLE IF NOT EXISTS insight.NOVO_POLICY_temp_3bv6mu (gid bigint, policy_id string, product_id bigint, metadata_dictionary_status_type bigint, effective_date timestamp, expiration_date timestamp, inception_date timestamp, rewrite_indicator string, metadata_dictionary_rewrite_indicator_type bigint, prior_insurance_indicator string, prior_insurance_classification string, agent_id string, metadata_dictionary_pay_plan bigint, metadata_dictionary_quote_method bigint, endorsement_number int, term_number int, term_in_months int, full_term_premium decimal, total_prorated_premium decimal, ubi_opt_in string, event_id string);", "sfDatabase": "novo_services", "sfSchema": "insight"}, transformation_ctx="NOVO_POLICY_node1733723453276")

# Script generated for node NOVO_METADATA_DICTIONARY
NOVO_METADATA_DICTIONARY_node1733724998396 = glueContext.write_dynamic_frame.from_options(frame=NOVO_METADATA_DICTIONARY_node1733809207022, connection_type="snowflake", connection_options={"autopushdown": "on", "postactions": "BEGIN; MERGE INTO insight.NOVO_METADATA_DICTIONARY USING insight.NOVO_METADATA_DICTIONARY_temp_esgzin ON NOVO_METADATA_DICTIONARY.product_id = NOVO_METADATA_DICTIONARY_temp_esgzin.product_id AND NOVO_METADATA_DICTIONARY.type = NOVO_METADATA_DICTIONARY_temp_esgzin.type AND NOVO_METADATA_DICTIONARY.name = NOVO_METADATA_DICTIONARY_temp_esgzin.name WHEN MATCHED THEN UPDATE SET gid = NOVO_METADATA_DICTIONARY_temp_esgzin.gid, product_id = NOVO_METADATA_DICTIONARY_temp_esgzin.product_id, type = NOVO_METADATA_DICTIONARY_temp_esgzin.type, name = NOVO_METADATA_DICTIONARY_temp_esgzin.name, description = NOVO_METADATA_DICTIONARY_temp_esgzin.description WHEN NOT MATCHED THEN INSERT VALUES (NOVO_METADATA_DICTIONARY_temp_esgzin.gid, NOVO_METADATA_DICTIONARY_temp_esgzin.product_id, NOVO_METADATA_DICTIONARY_temp_esgzin.type, NOVO_METADATA_DICTIONARY_temp_esgzin.name, NOVO_METADATA_DICTIONARY_temp_esgzin.description); DROP TABLE IF EXISTS insight.NOVO_METADATA_DICTIONARY_temp_esgzin; COMMIT;", "dbtable": "NOVO_METADATA_DICTIONARY_temp_esgzin", "connectionName": "Snowflake connection - Corp", "preactions": "CREATE TABLE IF NOT EXISTS insight.NOVO_METADATA_DICTIONARY (gid VARCHAR, product_id VARCHAR, type string, name string, description string); DROP TABLE IF EXISTS insight.NOVO_METADATA_DICTIONARY_temp_esgzin; CREATE TABLE IF NOT EXISTS insight.NOVO_METADATA_DICTIONARY_temp_esgzin (gid VARCHAR, product_id VARCHAR, type string, name string, description string);", "sfDatabase": "novo_services", "sfSchema": "insight"}, transformation_ctx="NOVO_METADATA_DICTIONARY_node1733724998396")

# Script generated for node NOVO_PRODUCT
NOVO_PRODUCT_node1733724552894 = glueContext.write_dynamic_frame.from_options(frame=NOVO_PRODUCT_node1733801650810, connection_type="snowflake", connection_options={"autopushdown": "on", "postactions": "BEGIN; MERGE INTO insight.NOVO_PRODUCT USING insight.NOVO_PRODUCT_temp_v7pt57 ON NOVO_PRODUCT.product_id = NOVO_PRODUCT_temp_v7pt57.product_id WHEN MATCHED THEN UPDATE SET product_id = NOVO_PRODUCT_temp_v7pt57.product_id, name = NOVO_PRODUCT_temp_v7pt57.name, state = NOVO_PRODUCT_temp_v7pt57.state, carrier = NOVO_PRODUCT_temp_v7pt57.carrier, description = NOVO_PRODUCT_temp_v7pt57.description WHEN NOT MATCHED THEN INSERT VALUES (NOVO_PRODUCT_temp_v7pt57.product_id, NOVO_PRODUCT_temp_v7pt57.name, NOVO_PRODUCT_temp_v7pt57.state, NOVO_PRODUCT_temp_v7pt57.carrier, NOVO_PRODUCT_temp_v7pt57.description); DROP TABLE IF EXISTS insight.NOVO_PRODUCT_temp_v7pt57; COMMIT;", "dbtable": "NOVO_PRODUCT_temp_v7pt57", "connectionName": "Snowflake connection - Corp", "preactions": "CREATE TABLE IF NOT EXISTS insight.NOVO_PRODUCT (product_id bigint, name string, state string, carrier string, description string); DROP TABLE IF EXISTS insight.NOVO_PRODUCT_temp_v7pt57; CREATE TABLE IF NOT EXISTS insight.NOVO_PRODUCT_temp_v7pt57 (product_id bigint, name string, state string, carrier string, description string);", "sfDatabase": "novo_services", "sfSchema": "insight"}, transformation_ctx="NOVO_PRODUCT_node1733724552894")

# Script generated for node NOVO_VEHICLE
NOVO_VEHICLE_node1733724268329 = glueContext.write_dynamic_frame.from_options(frame=NOVO_VEHICLE_node1733808450720, connection_type="snowflake", connection_options={"autopushdown": "on", "postactions": "BEGIN; MERGE INTO insight.NOVO_VEHICLE USING insight.NOVO_VEHICLE_temp_ofkcay ON NOVO_VEHICLE.policy_id = NOVO_VEHICLE_temp_ofkcay.policy_id AND NOVO_VEHICLE.vehicle_id = NOVO_VEHICLE_temp_ofkcay.vehicle_id AND NOVO_VEHICLE.event_id = NOVO_VEHICLE_temp_ofkcay.event_id WHEN MATCHED THEN UPDATE SET gid = NOVO_VEHICLE_temp_ofkcay.gid, vehicle_id = NOVO_VEHICLE_temp_ofkcay.vehicle_id, vin = NOVO_VEHICLE_temp_ofkcay.vin, policy_id = NOVO_VEHICLE_temp_ofkcay.policy_id, metadata_dictionary_vehicle_primary_use_type = NOVO_VEHICLE_temp_ofkcay.metadata_dictionary_vehicle_primary_use_type, metadata_dictionary_vehicle_ownership_type = NOVO_VEHICLE_temp_ofkcay.metadata_dictionary_vehicle_ownership_type, vehicle_dictionary_id = NOVO_VEHICLE_temp_ofkcay.vehicle_dictionary_id, is_ride_sharing = NOVO_VEHICLE_temp_ofkcay.is_ride_sharing, vehicle_created_at = NOVO_VEHICLE_temp_ofkcay.vehicle_created_at, vehicle_deleted_at = NOVO_VEHICLE_temp_ofkcay.vehicle_deleted_at, vehicle_deleted_ind = NOVO_VEHICLE_temp_ofkcay.vehicle_deleted_ind, garage_address_line1 = NOVO_VEHICLE_temp_ofkcay.garage_address_line1, garage_address_line2 = NOVO_VEHICLE_temp_ofkcay.garage_address_line2, garage_address_city = NOVO_VEHICLE_temp_ofkcay.garage_address_city, garage_address_county = NOVO_VEHICLE_temp_ofkcay.garage_address_county, garage_address_state = NOVO_VEHICLE_temp_ofkcay.garage_address_state, garage_address_zipcode = NOVO_VEHICLE_temp_ofkcay.garage_address_zipcode, garage_address_country = NOVO_VEHICLE_temp_ofkcay.garage_address_country, event_id = NOVO_VEHICLE_temp_ofkcay.event_id WHEN NOT MATCHED THEN INSERT VALUES (NOVO_VEHICLE_temp_ofkcay.gid, NOVO_VEHICLE_temp_ofkcay.vehicle_id, NOVO_VEHICLE_temp_ofkcay.vin, NOVO_VEHICLE_temp_ofkcay.policy_id, NOVO_VEHICLE_temp_ofkcay.metadata_dictionary_vehicle_primary_use_type, NOVO_VEHICLE_temp_ofkcay.metadata_dictionary_vehicle_ownership_type, NOVO_VEHICLE_temp_ofkcay.vehicle_dictionary_id, NOVO_VEHICLE_temp_ofkcay.is_ride_sharing, NOVO_VEHICLE_temp_ofkcay.vehicle_created_at, NOVO_VEHICLE_temp_ofkcay.vehicle_deleted_at, NOVO_VEHICLE_temp_ofkcay.vehicle_deleted_ind, NOVO_VEHICLE_temp_ofkcay.garage_address_line1, NOVO_VEHICLE_temp_ofkcay.garage_address_line2, NOVO_VEHICLE_temp_ofkcay.garage_address_city, NOVO_VEHICLE_temp_ofkcay.garage_address_county, NOVO_VEHICLE_temp_ofkcay.garage_address_state, NOVO_VEHICLE_temp_ofkcay.garage_address_zipcode, NOVO_VEHICLE_temp_ofkcay.garage_address_country, NOVO_VEHICLE_temp_ofkcay.event_id); DROP TABLE IF EXISTS insight.NOVO_VEHICLE_temp_ofkcay; COMMIT;", "dbtable": "NOVO_VEHICLE_temp_ofkcay", "connectionName": "Snowflake connection - Corp", "preactions": "CREATE TABLE IF NOT EXISTS insight.NOVO_VEHICLE (gid bigint, vehicle_id string, vin string, policy_id string, metadata_dictionary_vehicle_primary_use_type VARCHAR, metadata_dictionary_vehicle_ownership_type VARCHAR, vehicle_dictionary_id VARCHAR, is_ride_sharing string, vehicle_created_at timestamp, vehicle_deleted_at timestamp, vehicle_deleted_ind string, garage_address_line1 string, garage_address_line2 string, garage_address_city string, garage_address_county string, garage_address_state string, garage_address_zipcode string, garage_address_country string, event_id string); DROP TABLE IF EXISTS insight.NOVO_VEHICLE_temp_ofkcay; CREATE TABLE IF NOT EXISTS insight.NOVO_VEHICLE_temp_ofkcay (gid bigint, vehicle_id string, vin string, policy_id string, metadata_dictionary_vehicle_primary_use_type VARCHAR, metadata_dictionary_vehicle_ownership_type VARCHAR, vehicle_dictionary_id VARCHAR, is_ride_sharing string, vehicle_created_at timestamp, vehicle_deleted_at timestamp, vehicle_deleted_ind string, garage_address_line1 string, garage_address_line2 string, garage_address_city string, garage_address_county string, garage_address_state string, garage_address_zipcode string, garage_address_country string, event_id string);", "sfDatabase": "novo_services", "sfSchema": "insight"}, transformation_ctx="NOVO_VEHICLE_node1733724268329")

NOVO_RATING_SOURCE = glueContext.create_dynamic_frame.from_options(
    connection_type = "sqlserver",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "dbo.NOVO_RATING",
        "connectionName": "sf-NovoDB-UAT",
    },
    transformation_ctx = "NOVO_RATING_SOURCE"
)

NOVO_RATING_TARGET = glueContext.write_dynamic_frame.from_options(
    frame=NOVO_RATING_SOURCE, 
    connection_type="snowflake", 
    connection_options={
        "autopushdown": "on", 
        "postactions": 
            "BEGIN; MERGE INTO insight.NOVO_RATING USING insight.NOVO_RATING_temp ON NOVO_RATING.policy_id = NOVO_RATING_temp.policy_id AND NOVO_RATING.event_id = NOVO_RATING_temp.event_id AND NOVO_RATING.package = NOVO_RATING_temp.package AND NOVO_RATING.payment_plan = NOVO_RATING_temp.payment_plan WHEN MATCHED THEN UPDATE SET type = NOVO_RATING_temp.type, selected = NOVO_RATING_temp.selected, request_link = NOVO_RATING_temp.request_link, response_link = NOVO_RATING_temp.response_link, roc_link = NOVO_RATING_temp.roc_link, total_premium = NOVO_RATING_temp.total_premium, rating_at = NOVO_RATING_temp.rating_at, request_at = NOVO_RATING_temp.request_at WHEN NOT MATCHED THEN INSERT VALUES (NOVO_RATING_temp.policy_id, NOVO_RATING_temp.event_id, NOVO_RATING_temp.type, NOVO_RATING_temp.package, NOVO_RATING_temp.payment_plan, NOVO_RATING_temp.selected, NOVO_RATING_temp.request_link, NOVO_RATING_temp.response_link, NOVO_RATING_temp.roc_link, NOVO_RATING_temp.total_premium, NOVO_RATING_temp.rating_at, NOVO_RATING_temp.request_at); DROP TABLE IF EXISTS insight.NOVO_RATING_temp; COMMIT;", 
        "dbtable": "NOVO_RATING_temp", 
        "connectionName": "Snowflake connection - Corp", 
        "preactions": 
            "CREATE TABLE IF NOT EXISTS insight.NOVO_RATING (policy_id string, event_id string, type string, package string, payment_plan string, selected string, request_link string, response_link string, roc_link string, total_premium float, rating_at TIMESTAMP_NTZ(9), request_at TIMESTAMP_NTZ(9)); DROP TABLE IF EXISTS insight.NOVO_RATING_temp; CREATE TABLE IF NOT EXISTS insight.NOVO_RATING_temp (policy_id string, event_id string, type string, package string, payment_plan string, selected string, request_link string, response_link string, roc_link string, total_premium float, rating_at TIMESTAMP_NTZ(9), request_at TIMESTAMP_NTZ(9));", 
        "sfDatabase": "novo_services", "sfSchema": "insight"
    }, 
    transformation_ctx="NOVO_RATING_TARGET"
)

job.commit()