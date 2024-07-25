# NYC-Taxi-Data
data pipeline to download, clean, and visualize New York Cities taxi data for April 2024
This repository contains a data pipeline for analyzing NYC taxi trip data. The pipeline performs the following tasks:

Data Ingestion: Loads NYC taxi trip data and geographic data.
Data Cleaning: Handles missing values and normalizes data formats.
Spatial Joins: Maps trip pickup and dropoff locations to taxi zones and neighborhoods.
Aggregation and Analysis: Aggregates data by neighborhoods and boroughs and generates insights.
Prerequisites
Ensure you have the following installed:

Python 3.7+
PostgreSQL with PostGIS extension
Required Python packages: pandas, geopandas, sqlalchemy, psycopg2, pyarrow, fastparquet, matplotlib, seaborn

Installation
Clone the Repository:

git clone https://github.com/jnordberg1/NYC-Taxi-Data.git
cd NYC-Taxi_data
Set Up a Virtual Environment:

python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
Install Required Packages:

pip install -r requirements.txt
Set Up PostgreSQL with PostGIS:

Install PostgreSQL and create a database.
Install the PostGIS extension.
sql
Copy code
CREATE EXTENSION postgis;
Update Configuration:

Modify the pipeline.py script to update database connection details if needed.

Running the Pipeline
Download Data:
Run download_data.py

Prepare Data:

Place your NYC taxi trip data in Parquet format in the data/ directory.
Place the shapefiles (e.g., taxi_zones.shp, nta_shapefile.shp) in the data/ directory.
Execute the Pipeline:

python pipeline.py

Graphs and visualizations will be displayed as output.
Visualizations
The pipeline generates visualizations for:

Number of trips per neighborhood, split by borough.
Peak hours of operation per neighborhood, split by borough.
Troubleshooting
Timezone-Aware Datetime Errors: Ensure that your datetime columns are converted to UTC before processing.
Column Overlap Errors: Specify suffixes during DataFrame merges to handle overlapping columns.
PostGIS Errors: Ensure the PostGIS extension is correctly installed and configured.
Contributing
Feel free to open issues or submit pull requests. Please follow the coding standards and include tests for new features.
