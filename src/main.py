from data_agregation import (
    create_agregate_tables,
    agregate_dim_city,
    agregate_dim_station,
    agregate_fact_station_statements,
)
from data_consolidation import (
    create_consolidate_tables,
    consolidate_city_data,
    consolidate_station_data,
    consolidate_station_statement_data,
)
from data_ingestion import (
    get_realtime_bicycle_data,
    get_commune_data,
)
from data_visualization import mobility_analysis_dashboard


def main():
    print("Process start.")
    # data ingestion

    print("Data ingestion started.")
    get_realtime_bicycle_data()
    get_commune_data()
    print("Data ingestion ended.")

    # data consolidation
    print("Consolidation data started.")
    create_consolidate_tables()
    consolidate_city_data()
    consolidate_station_data()
    consolidate_station_statement_data()
    print("Consolidation data ended.")

    # data agregation
    print("Agregate data started.")
    create_agregate_tables()
    agregate_dim_city()
    agregate_dim_station()
    agregate_fact_station_statements()
    print("Agregate data ended.")

    # data visualization
    mobility_analysis_dashboard()
    print("Process ended.")

if __name__ == "__main__":
    main()
