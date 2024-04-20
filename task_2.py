import json
import os
import pandas as pd
import logger
import mysql.connector

log = logger.Logger("log/task_2.log", "Task_2")

# reading configuration file
log.logger.debug("READING LOG FILES")
try:
    with open('config.json', 'r') as json_file:
        config = json.load(json_file)
    log.logger.debug(f"Configuration Loaded: {config}")
except FileNotFoundError as fe:
    log.logger.critical("CONFIG FILE NOT FOUND")
    log.logger.error(fe)
    exit()
except Exception as e:
    log.logger.critical("FACING ISSUE TO READ CONFIG")
    log.logger.error(e)
    exit()

# modifying dictionary to use mysql config only
config = config['mysql_config']
log.logger.debug(f"CONFIGURATION FILE: {config}")

# Creating Database Connection
try:
    log.logger.debug("CONNECTING TO DATABASE")
    conn = mysql.connector.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database=config['database'],
        port=config['port'],
        charset="latin1"
    )
    cursor = conn.cursor()
    log.logger.info("DATABASE CONNECTED")
except ConnectionRefusedError as e:
    log.logger.critical("CONNECTION REFUSED BY SERVER.")
    log.logger.error(e)
    print("ERROR: CONNECTION REFUSED BY SERVER")
    exit()

# finding csv files from assets folder
csv_files = []

# Iterate over all files in the directory
for filename in os.listdir("assets"):
    # Check if the file ends with ".csv" extension
    if filename.endswith(".csv"):
        csv_files.append(filename)

log.logger.debug(f"Total {len(csv_files)} files are found on assets folder.")
print(f"Total {len(csv_files)} files are found on assets folder.")


for filename in csv_files:
    log.logger.debug(f"WORKING ON: {filename}")

    table_name = filename[:-4]
    location = "assets/" + filename

    log.logger.debug(f"table_name : {table_name}, location= {location}")

    # convertinf file into datframe
    try:
        df = pd.read_csv(location)
        log.logger.debug("converted to dataframe")
    except UnicodeError:
        df = pd.read_csv(location, encoding='latin1')
        log.logger.debug("Converted to dataframe using latin1")
    except Exception as e:
        log.logger.critical("Unable to convert file in data frame")
        log.logger.error(e)
        exit()

    # Check if the first column name is "ID" and reomve the column
    if df.columns[0] == "ID":
        # Drop the entire ID column
        df.drop(columns=['ID'], inplace=True)

    # Convert all fields to string
    df = df.astype(str)

    # adding created_by row
    df['created_by'] = 'Kinjal'

    # fetching fields from table
    cursor.execute(f"DESCRIBE `{table_name}`")
    fields = [f"`{row[0]}`" for row in cursor.fetchall() if row[0] not in ['id', 'created_at', 'updated_at']]
    log.logger.debug(f"fields: {fields}")

    # converting data into list of tupple
    data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]

    # truncating table to remove previous data
    cursor.execute(f"TRUNCATE TABLE `{table_name}`")
    conn.commit()

    # Insert DataFrame into MySQL table
    insert_query = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({', '.join(['%s'] * len(fields))})"
    log.logger.debug(f"INSERT QUERY: {insert_query}")
    # log.logger.debug(f"VALUES: {data_tuples}")

    log.logger.debug("Inserting data to table.")
    cursor.executemany(insert_query, data_tuples)

    # # fetching last id
    # Execute the query to get the last ID from the table
    cursor.execute(f"SELECT MAX(id) FROM `{table_name}`")
    last_id = cursor.fetchone()[0]

    log.logger.debug(f"INSERTED {last_id} records on {table_name} Table")
    print(f"INFO: INSERTED {last_id} records on {table_name} Table")
    conn.commit()
    log.logger.debug(f"DATA INSERTION COMPLETED ON {table_name}")


# closing database connection
conn.close()
log.logger.debug("CONNECTION CLOSED")
