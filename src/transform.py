import pandas as pd
from pathlib import Path
from database import get_engine
from transform import get_clean_data

#-------------------------
#1 Load data from Database
#-------------------------

def load_data(engine):
    """Load full data from transformers table into a Dataframe
    """
    query = "SELECT * FROM transformers;"
    df = pd.read_sql(query, engine)

    return df

def transform_dataframe(df):
    """Clean and engineer features
    """

    

#Mini transformation pipeline
def get_clean_data(df):
    df = load_data(engine)#
    clean_df = transform_dataframe(df)

    return clean_df




#-------------------------
#2 Main use of Transform Data
#-------------------------
engine = get_engine()
df_clean = get_clean_data(engine)



# Store processed data into directory
#Create directory if it does not exist
dir_path = Path("../data/processed")
dir_path.mkdir(parents=True, exist_ok=True)

# Full file path
file_path = dir_path / "processed_sensor_data.csv"

# Save file in directory
df_clean.to_csv(file_path, index=False)