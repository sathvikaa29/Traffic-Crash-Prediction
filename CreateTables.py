import pandas as pd
from sqlalchemy import create_engine
import psycopg2

db_user = 'postgres'
db_password = 'admin'
db_host = 'localhost'
db_port = '5432'
db_name = 'CrashAnalytics'

engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

create_table_query = """
CREATE TABLE IF NOT EXISTS crashes (
    crash_record_id VARCHAR(128) PRIMARY KEY,
    crash_date TIMESTAMP,
    weather_condition VARCHAR(50),
    first_crash_type VARCHAR(50),
    posted_speed_limit INTEGER,
    crash_hour INTEGER,
    crash_day_of_week INTEGER,
    crash_month INTEGER,
    traffic_control_device VARCHAR(50),
    roadway_surface_cond VARCHAR(50),
    road_defect VARCHAR(50),
    lighting_condition VARCHAR(50),
    beat_of_occurrence VARCHAR(50)
);
"""

with engine.connect() as connection:
    connection.execute(create_table_query)
    print("crashes Table created successfully.")
    
    
create_table_query = """
CREATE TABLE IF NOT EXISTS vehicles (
    crash_record_id VARCHAR(128) REFERENCES crashes(crash_record_id),
    vehicle_id VARCHAR(50) PRIMARY KEY,
    make VARCHAR(100),
    model VARCHAR(100),
    vehicle_year INTEGER,
    vehicle_defect VARCHAR(100),
    exceed_speed_limit_i VARCHAR(100),
    num_passengers INTEGER,
    travel_direction VARCHAR(50)
);
"""

with engine.connect() as connection:
    connection.execute(create_table_query)
    print("Table 'vehicles' created successfully.")
    
    
create_table_query = """
CREATE TABLE IF NOT EXISTS people (
    crash_record_id VARCHAR(128) REFERENCES crashes(crash_record_id),
    person_id VARCHAR(50) PRIMARY KEY,
    age INTEGER,
    sex VARCHAR(10),
    injury_classification VARCHAR(50),
    physical_condition VARCHAR(50),
    hospital VARCHAR(50)
);
"""


with engine.connect() as connection:
    connection.execute(create_table_query)
    print("Table 'people' created successfully.")
    
    