from database import get_engine, create_tables
from ingest import ingest_file
from transform import get_clean_data
from visualization import plot_voltage_temperature

def main():

    # 1. Setup DB
    engine = get_engine()
    create_tables(engine)

    # 2. Ingest new file
    ingest_file("../data/raw/new_sensor_maintenance_data.csv", engine)

    # 3. Transform
    df = get_clean_data(engine)

    # 4. Visualize
    plot_voltage_temperature(df)


if __name__ == "__main__":
    main()