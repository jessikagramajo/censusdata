import pymysql.cursors
import os
import pandas as pd
from dotenv import load_dotenv, dotenv_values

# loading variables from .env file
load_dotenv()
#create mod_all_states as a new table within a new db
#Establish a connection to the database
conn = pymysql.connect(
    host='localhost',
    user='root',
    password=os.getenv("password"),
    database='census',
    cursorclass=pymysql.cursors.DictCursor
    )

# Create a cursor object
cursor = conn.cursor()

#create db
#conn.cursor().execute('CREATE DATABASE IF NOT EXISTS census')

#Create the table if it doesn't exist
create_table_query = """
CREATE TABLE IF NOT EXISTS CensusData (
    Idx INT,
    AttainedEd VARCHAR(100),
    Race VARCHAR(100),
    TotalIncome INT,
    HispanicInd VARCHAR(100),
    Sex VARCHAR(100),
    MaritalStatus VARCHAR(100),
    State VARCHAR(100)
    )
    """
cursor.execute(create_table_query)

#data
df = pd.read_csv("mod_census_data_all.csv")
# Insert data into the table
for _, row in df.iterrows():
    insert_query = "INSERT INTO CensusData (Idx, AttainedEd, Race, TotalIncome, HispanicInd, Sex, MaritalStatus, State) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(insert_query, (row['Idx'], row['AttainedEd'], row['Race'], row['TotalIncome'], row['HispanicInd'], row['Sex'], row['MaritalStatus'], row['State']))

# Commit the changes
conn.commit()

# Close the connection
conn.close()