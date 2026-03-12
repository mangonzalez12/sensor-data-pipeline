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
#2 Drop non useful variables
#--------------------------------------------

def clean_dataframe(df):
    """
    Dataframe for summary with no scaling of numeric variables
    Keeps categorical variables untouched.
    """

    #Drop columns
    drop_columns = ["id", "Sensor_ID", "Timestamp", "Last_Maintenance_Date", "Equipment_ID", "X", "Y", "Z", "Fault_Status"]
    df_clean = df.drop(columns=drop_columns)

    # Fill nan values as Functional for failure_Type
    df_clean["Failure_Type"] = df_clean["Failure_Type"].fillna("Functional")
    
    
    return df_clean



#----------------------------------------
#3 Transform data and produce scaled data
#----------------------------------------

def transform_dataframe(df):
    """
    Scaled dataframe for machine learning and analytics.
    Keeps categorical variables untouched.
    Returns dataframe with numeric columns scaled.
    """

    #Drop columns
    drop_columns = ["id", "Sensor_ID", "Timestamp", "Last_Maintenance_Date", "Equipment_ID", "X", "Y", "Z", "Fault_Status"]
    df = df.drop(columns=drop_columns)
    
    df = df.copy()
    # Fill nan values as Functional for failure_Type
    df["Failure_Type"] = df["Failure_Type"].fillna("Functional")

    #Convert categorical columns
    categorical_cols=["Operational_Status", "Failure_Type",
                        "Maintenance_Type", "Failure_History", 
                        "External_Factors",
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
    ml_df = pd.concat(
        [df_scaled_numeric, df.drop(columns=numeric_cols)],
        axis=1
    )
    print("ml_df type:", type(ml_df))


    return ml_df

#---------------------------------------------------------------
#4 Mini transformation pipeline for Machine Learning and Analytics
#---------------------------------------------------------------  

def get_clean_data(engine):
    df = load_data(engine)
    ml_df = transform_dataframe(df)

    return ml_df



