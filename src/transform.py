import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from pathlib import Path
from database import get_engine


#-------------------------
#1 Load data from Database
#-------------------------

def load_data(engine):
    """Load full data from transformers table into a pd.Dataframe
    """
    query = "SELECT * FROM transformers;" #Query all variables
    df = pd.read_sql(query, engine)

    return df

#--------------------------------------------
#2 Create summary dataframe for visualization
#--------------------------------------------
def create_summary(df):
    numeric_cols = df.select_dtypes(include=[np.number])
    summary = pd.DataFrame({
        "min": numeric_cols.min(),
        "max": numeric_cols.max(),
        "mean": numeric_cols.mean(),
        "std": numeric_cols.std()
    })

    return summary

#----------------------------------------
#3 Transform data and produce scaled data
#----------------------------------------

def transform_dataframe(df):
    """
    Scaled dataframe for Machine Learning and Analytics.
    Keeps categorical variables untouched.
    Returns dataframe with numeric columns scaled.
    """

    #Drop columns
    drop_columns = ["id", "Sensor_ID", "Timestamp", "Last_Maintenance_Date"]
    df = df.copy()
    df = df.drop(columns=drop_columns, errors="ignore")
    


    #Convert categorical columns
    categorical_cols=["Equipment_ID", "Operational_Status", "Fault_Status",
                      "Failure_Type", "Last_Maintenance_Date", "Maintenance_Type",
                       "Failure_History", "Repair_Time", "Ambient_Humidity",
                        "External_Factors", "X", "Y", "Z",
                        "Equipment_Relationship", "Equipment_Criticality",
                        "Fault_Detected", "Predictive_Maintenance_Trigger"
    ]
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].astype("category")

    #Identify numeric columns
    numeric_cols = df.select_dtypes(include="number").columns


    #Scale numeric columns using standardscaler
    scaler = StandardScaler()
    df_scaled_numeric = pd.DataFrame(scaler.fit_transform(df[numeric_cols]),
                             columns=numeric_cols,
                             index=df.index
    )
    
    #Merge categorical and scaled numeric columns 
    df_sc = pd.concat(
        [df_scaled_numeric, df.drop(columns=numeric_cols)],
        axis=1
    )

    return df_sc

#---------------------------------------------------------------
#4 Mini transformation pipeline for Machine Learning and Analytics
#---------------------------------------------------------------  

def get_clean_data(engine):
    df = load_data(engine)
    clean_df = transform_dataframe(df)
    return clean_df



