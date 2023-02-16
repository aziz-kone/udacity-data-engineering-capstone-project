CREATE_FACT_TAXI_TRIPS = """
CREATE TABLE fact_trips (
    VendorID BIGINT, 
	pickup_datetime TIMESTAMP WITHOUT TIME ZONE, 
	dropoff_datetime TIMESTAMP WITHOUT TIME ZONE, 
	passenger_count FLOAT(53), 
	trip_distance FLOAT(53), 
	"RatecodeID" FLOAT(53), 
	store_and_fwd_flag TEXT, 
	"PULocationID" BIGINT, 
	"DOLocationID" BIGINT, 
	payment_type FLOAT(53), 
	fare_amount FLOAT(53), 
	extra FLOAT(53), 
	mta_tax FLOAT(53), 
	tip_amount FLOAT(53), 
	tolls_amount FLOAT(53), 
	improvement_surcharge FLOAT(53), 
	total_amount FLOAT(53), 
	congestion_surcharge FLOAT(53), 
	taxi_type TEXT
)

"""

CREATE_DIM_LOCATION = """
CREATE TABLE dim_location (
LocationID INTEGER,
Borough VARCHAR(255),
Zone VARCHAR(255),
service_zone VARCHAR(255)
)"""


CREATE_DIM_TIME = """
CREATE TABLE dim_time (
	date DATE, 
	year INTEGER, 
	quarter INTEGER, 
	month INTEGER, 
	day INTEGER, 
	week INTEGER, 
	is_weekend BOOLEAN
)
"""

CREATE_DIM_PAYMENT_TYPE = """
CREATE TABLE dim_payment_type (
	"TypeId" TEXT, 
	"TypeLabel" TEXT
)
"""


CREATE_ROLE_PLAYING_VIEWS ="""
CREATE OR REPLACE VIEW dim_time_pickup as select * from dim_time;
CREATE OR REPLACE VIEW dim_time_dropoff as select * from dim_time;
CREATE OR REPLACE VIEW dim_location_pickup as select * from dim_location;
CREATE OR REPLACE VIEW dim_location_dropoff as select * from dim_location;
"""



