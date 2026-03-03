import pandas as pd

#-------------------------
# 1 Normalize column names
#-------------------------
#Dictionary to rename columns ('old_name': 'new_name')
rename_dict = {'Sensor_ID': 'Sensor_ID',
        'Timestamp': 'Timestamp',
        'Voltage (V)': 'Voltage',
        'Current (A)': 'Current',
        'Temperature (°C)': 'Temperature',
        'Power (W)': 'Power',
        'Humidity (%)': 'Humidity',
        'Vibration (m/s²)': 'Vibration',
        'Equipment_ID': 'Equipment_ID',
        'Operational Status': 'Operational_Status',
        'Fault Status': 'Fault_Status',
        'Failure Type': 'Failure_Type',
        'Last Maintenance Date': 'Last_Maintenance_Date',
        'Maintenance Type': 'Maintenance_Type',
        'Failure History': 'Failure_History',
        'Repair Time (hrs)': 'Repair_Time',
        'Maintenance Costs (USD)': 'Maintenance_Costs',
        'Ambient Temperature (°C)': 'Ambient_Temperature',
        'Ambient Humidity (%)': 'Ambient_Humidity',
        'External Factors': 'External_Factors',
        'X': 'X',
        'Y': 'Y',
        'Z': 'Z',
        'Equipment Relationship': 'Equipment_Relationship',
        'Equipment Criticality': 'Equipment_Criticality',
        'Fault Detected': 'Fault_Detected',
        'Predictive Maintenance Trigger': 'Predictive_Maintenance_Trigger'} 

# -------------------------
# 2. Expected Schema
# -------------------------

EXPECTED_COLUMNS = [
    "Sensor_ID",
    "Timestamp",
    "Voltage",
    "Current",
    "Temperature",
    "Power",
    "Humidity",
    "Vibration",
    "Equipment_ID",
    "Operational_Status",
    "Fault_Status",
    "Failure_Type",
    "Last_Maintenance_Date",
    "Maintenance_Type",
    "Failure_History",
    "Repair_Time",
    "Maintenance_Costs",
    "Ambient_Temperature",
    "Ambient_Humidity",
    "External_Factors",
    "X",
    "Y",
    "Z",
    "Equipment_Relationship",
    "Equipment_Criticality",
    "Fault_Detected",
    "Predictive_Maintenance_Trigger"
]

#-----------------------------------------
# 3             Ingestion 
#-----------------------------------------

def ingest_file(file_path, engine, if_exists="append"):
    """Read the CSV file, normalize column names and inserts into 
    new data to existing database connected with engine"""

    #Read data
    file_path = file_path
    df = pd.read_csv(file_path)
    
    #Rename data with names in rename_dict
    df = df.rename(columns=rename_dict)

    #Validate expected schema
    missing = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    
    #Reorder columns to match expected columns
    df = df[EXPECTED_COLUMNS]

    #Insert data into database
    df.to_sql(

    )
    # Create engine (same path you used for database creation)
    engine = create_engine("sqlite:///../database/sensor_database.db")

    # Insert dataframe into table
    sensor_data_renamed.to_sql(
        "transformers", #Name of SQL table to ingest data
        engine, #Connection or engine
        if_exists="append", #Append new data
        index=False 
    )
    
    print(f"Ingested {len(df)} rows successfully.")