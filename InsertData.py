db_user = 'postgres'
db_password = 'admin'
db_host = 'localhost'
db_port = '5432'
db_name = 'CrashAnalytics'

engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')


##Inserting data into crashes table
crashes = pd.read_csv("Crashes-Crashes.csv")
crashes_sampled = crashes[["CRASH_RECORD_ID","CRASH_DATE","WEATHER_CONDITION","FIRST_CRASH_TYPE",
                          "POSTED_SPEED_LIMIT","CRASH_HOUR","CRASH_DAY_OF_WEEK","CRASH_MONTH",
                          "TRAFFIC_CONTROL_DEVICE","ROADWAY_SURFACE_COND","ROAD_DEFECT",
                          "LIGHTING_CONDITION","BEAT_OF_OCCURRENCE"]]

crashes_sampled.columns = [col.lower() for col in crashes_sampled.columns]
crashes_sampled['crash_date'] = pd.to_datetime(crashes_sampled['crash_date'])

# Insert data SQL query template
insert_query_template = """
INSERT INTO crashes (
    crash_record_id, crash_date, weather_condition, first_crash_type, 
    posted_speed_limit, crash_hour, crash_day_of_week, crash_month, 
    traffic_control_device, roadway_surface_cond, road_defect, 
    lighting_condition, beat_of_occurrence
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

data_to_insert = crashes_sampled.values.tolist()

try:
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    cursor = conn.cursor()
    cursor.executemany(insert_query_template, data_to_insert)
    conn.commit()
    cursor.close()
    conn.close()
    print("Data inserted successfully.")
except Exception as e:
    print(f"An error occurred: {e}")
    
    
## Insert data into vehichles table
vehicles = pd.read_csv("Crashes-Vehicles.csv")
vehicles_sampled = vehicles[["CRASH_RECORD_ID","VEHICLE_ID","MAKE","MODEL","VEHICLE_YEAR","VEHICLE_DEFECT",
                            "EXCEED_SPEED_LIMIT_I","NUM_PASSENGERS","TRAVEL_DIRECTION"]]

vehicles_sampled.columns = [col.lower() for col in vehicles_sampled.columns]

vehicles_sampled['vehicle_id'] = vehicles_sampled['vehicle_id'].astype(str).str.replace('\.0', '', regex=True)
vehicles_sampled['vehicle_year'] = pd.to_numeric(vehicles_sampled['vehicle_year'], errors='coerce').fillna(0).astype(int)
vehicles_sampled['num_passengers'] = pd.to_numeric(vehicles_sampled['num_passengers'], errors='coerce').fillna(0).astype(int)

insert_query_template = """
INSERT INTO vehicles (
    crash_record_id, vehicle_id, make, model, vehicle_year, vehicle_defect, 
    exceed_speed_limit_i, num_passengers, travel_direction
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (vehicle_id) DO NOTHING;
"""

data_to_insert = vehicles_sampled.values.tolist()

with engine.connect() as connection:
    connection.execute(create_table_query)
    print("Table 'vehicles' created successfully.")

try:
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    cursor = conn.cursor()
    cursor.executemany(insert_query_template, data_to_insert)
    conn.commit()
    cursor.close()
    conn.close()
    print("Data inserted successfully into 'vehicles' table.")
except Exception as e:
    print(f"An error occurred: {e}")
    
    
## Insert Data into people table
people = pd.read_csv("Crashes-People.csv")
people_sampled = people[["CRASH_RECORD_ID","PERSON_ID","AGE","SEX","INJURY_CLASSIFICATION",
                        "PHYSICAL_CONDITION","INJURY_CLASSIFICATION","HOSPITAL"]]

people_sampled.columns = [col.lower() for col in people_sampled.columns]

people_sampled['person_id'] = people_sampled['person_id'].astype(str).str.replace(r'\.0$', '', regex=True)

people_sampled['age'] = pd.to_numeric(people_sampled['age'], errors='coerce').fillna(0).astype(int)

insert_query_template = """
INSERT INTO people (
    crash_record_id, person_id, age, sex, injury_classification, 
    physical_condition, hospital
) VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (person_id) DO NOTHING;
"""

data_to_insert = people_sampled[['crash_record_id', 'person_id', 'age', 'sex', 'injury_classification', 'physical_condition', 'hospital']].values.tolist()


with engine.connect() as connection:
    connection.execute(create_table_query)
    print("Table 'people' created successfully.")

try:
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )
    cursor = conn.cursor()
    cursor.executemany(insert_query_template, data_to_insert)
    conn.commit()
    cursor.close()
    conn.close()
    print("Data inserted successfully into 'people' table.")
except Exception as e:
    print(f"An error occurred: {e}")