import duckdb
import streamlit as st

def mobility_analysis_dashboard():

    import plotly.express as px

    st.title("🚲 Bicycle Station Data Visualization From Some French Cities")

    # Global ETL loading message
    with st.spinner("⏳ Please wait while the ETL process is running and data is being prepared..."):

        # -------------------------
        # 📍 Station Locations
        # -------------------------
        st.header("📍 Station Locations")

        @st.cache_data
        def get_data_from_duckdb():
            con = duckdb.connect(
                database="data/duckdb/mobility_analysis.duckdb", read_only=True
            )
            sql_statement = """
            SELECT 
                s.NAME AS station_name,
                s.ADDRESS AS address,
                s.STATUS AS status,
                s.LONGITUDE AS longitude,
                s.LATITUDE AS latitude,
                s.CAPACITTY AS capacity
            FROM DIM_STATION s;
            """
            df = con.execute(sql_statement).fetchdf()
            return df

        with st.spinner("Loading station location data..."):
            data = get_data_from_duckdb()

        st.dataframe(data)

        status_filter = st.selectbox("Station status", ["All", "OUI", "NON"])

        if status_filter != "All":
            data = data[data["status"] == status_filter]

        with st.spinner("Rendering map..."):
            fig = px.scatter_mapbox(
                data,
                lat="latitude",
                lon="longitude",
                hover_name="station_name",
                hover_data={
                    "address": True,
                    "capacity": True,
                },
                size="capacity",
                size_max=15,
                zoom=5,
                center={"lat": 46.5, "lon": 2.5},
                height=600
            )

            fig.update_layout(
                mapbox_style="open-street-map",
                margin={"r":0,"t":0,"l":0,"b":0}
            )

            st.plotly_chart(fig)

        st.metric("Number of stations", len(data))

        # -------------------------
        # 🏙️ Available Docks by City
        # -------------------------
        st.header("🏙️ Available Docks by City")

        @st.cache_data
        def get_docks_by_city():
            con = duckdb.connect(
                database="data/duckdb/mobility_analysis.duckdb", read_only=True
            )
            
            query = """
            SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
            FROM DIM_CITY dm INNER JOIN (
                SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
                FROM FACT_STATION_STATEMENT
                WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
                GROUP BY CITY_ID
            ) tmp ON dm.ID = tmp.CITY_ID
            WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse', 'montpellier');
            """
            
            return con.execute(query).fetchdf()

        with st.spinner("Aggregating available docks by city..."):
            df_city = get_docks_by_city()

        st.dataframe(df_city)

        df_city = df_city.sort_values(by="SUM_BICYCLE_DOCKS_AVAILABLE", ascending=False)

        with st.spinner("Building city chart..."):
            fig = px.bar(
                df_city,
                x="NAME",
                y="SUM_BICYCLE_DOCKS_AVAILABLE",
                title="Available Bicycle Docks by City",
                text_auto=True
            )

            st.plotly_chart(fig)

        total_docks = df_city["SUM_BICYCLE_DOCKS_AVAILABLE"].sum()
        st.metric("Total available docks", total_docks)

        # -------------------------
        # 🚲 Station Analysis
        # -------------------------
        st.header("🚲 Station Analysis")

        @st.cache_data
        def get_avg_bikes_per_station():
            con = duckdb.connect(
                database="data/duckdb/mobility_analysis.duckdb", read_only=True
            )
            
            query = """
            SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
            FROM DIM_STATION ds JOIN (
                SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
                FROM FACT_STATION_STATEMENT
                GROUP BY station_id
            ) AS tmp ON ds.id = tmp.station_id;
            """
            
            return con.execute(query).fetchdf()

        with st.spinner("Computing average bikes per station..."):
            df_station = get_avg_bikes_per_station()

        st.dataframe(df_station)

        with st.spinner("Generating top stations chart..."):
            top_stations = df_station.sort_values(
                by="avg_dock_available",
                ascending=False
            ).head(10)

            fig_top = px.bar(
                top_stations,
                x="avg_dock_available",
                y="NAME",
                orientation="h",
                title="Top 10 Stations (Most Bikes Available)"
            )

            st.plotly_chart(fig_top)

        with st.spinner("Generating distribution histogram..."):
            fig_hist = px.histogram(
                df_station,
                x="avg_dock_available",
                nbins=30,
                title="Distribution of Average Bikes Available"
            )

            st.plotly_chart(fig_hist)

        st.metric("Total stations", len(df_station))
        st.metric("Average bikes per station", round(df_station["avg_dock_available"].mean(), 2))


if __name__ == "__main__":
    mobility_analysis_dashboard()