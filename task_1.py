import pandas as pd
import mysql.connector
import logger

# log
log = logger.Logger('log/task_1.log', "TASK 1")
log.logger.info("Program Started")

# Read the CSV file into a DataFrame
data = pd.read_csv('data_dictionary.csv')
log.logger.info("csv converted into dataframe")

# filtering data to remove id column
data = filtered_df = data[data['Field'] != 'ID']

# Convert all field names and table names to lower case, replace "-", and " " with "_"
data['Field'] = data['Field'].str.lower().str.replace('-', '_').str.replace(' ', '_')
data['Table'] = data['Table'].str.lower().str.replace('-', '_').str.replace(' ', '_')

log.logger.info("CONNECTING TO DATABASE")

# Establish a connection to MySQL
connection = mysql.connector.connect(
    host='db.bhatol.in',
    port='13306',
    user='kinjal',
    password='12345',
    database='project'
)

# Create a cursor object
cursor = connection.cursor()

# Group the data by table
grouped_data = data.groupby('Table')

# Iterate through each group
for table_name, group in grouped_data:
    fields = group[['Field']].values.flatten()

    # Create table if it doesn't exist
    create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ("

    # Add an auto-incrementing id column
    create_table_query += "`id` INT AUTO_INCREMENT PRIMARY KEY, "

    # Add other columns from your data
    for field in fields:
        create_table_query += f"`{field}` VARCHAR(255), "

    # Add additional columns
    create_table_query += "`created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
    create_table_query += "`updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
    create_table_query += "`created_by` VARCHAR(255))"

    log.logger.debug(f"INSERT QUERY: {create_table_query}")
    cursor.execute(create_table_query)
    connection.commit()
    print(f"INFO: {table_name} is created")
    log.logger.info(f"QUERY COMMITTED.")


# Close connection
connection.close()
log.logger.info("CONNECTION CLOSED")
print("INFO: PROGRAM COMPLETED")
