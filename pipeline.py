# -*- coding: utf-8 -*-
"""
For this bit of code, the pipeline, I chose to use Postgres as I am more familiar with this
database configuration and you can do the same operations as duckdb needed for this project. 
I am happy to look into how you might do this with duckdb if required.
"""

import pandas as pd
import geopandas as gpd
import psycopg2
from sqlalchemy import create_engine
from typing import Tuple
import psycopg2.extras as extras
import matplotlib.pyplot as plt
import seaborn as sns


#functon to insert dataframe values into postgres database
def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    # cursor.close()
    
    
#function to prep data for loading
def prep_data(conn, yellow_taxi_data_path: str, green_taxi_data_path: str, nta_shapefile_path: str, taxi_zone_shp: str) -> Tuple[pd.DataFrame, gpd.GeoDataFrame]:
    try:
        #load yellow taxi data into PostgreSQL
        yellow_taxi_df = pd.read_parquet(yellow_taxi_data_path)
        #add in taxi type field
        yellow_taxi_df['type'] ='yellow'
        #load green taxi data into PostgreSQL
        green_taxi_df = pd.read_parquet(green_taxi_data_path)
        green_taxi_df['type'] ='green'
    
        #combine yellow and green taxi data
        taxi_df = pd.concat([yellow_taxi_df, green_taxi_df], ignore_index=True)
        taxi_df = taxi_df.rename(columns = {'VendorID':'vendor_id', 'RatecodeID':'ratecode_id', 
               'PULocationID' :'pu_location_id', 'DOLocationID' : 'do_location_id', 'Airport_fee' : 'airport_fee'})
        
        #drop un-needed columns
        taxi_df = taxi_df.drop(['ehail_fee', 'trip_type', 'airport_fee', 'lpep_dropoff_datetime',
                                'lpep_pickup_datetime', 'congestion_surcharge', 'improvement_surcharge',
                                'tolls_amount', 'mta_tax','extra', 'payment_type', 'store_and_fwd_flag',
                                'ratecode_id', 'passenger_count'], axis = 1)
       
        taxi_df['tpep_pickup_datetime'] = taxi_df['tpep_pickup_datetime'].astype(str).replace('NaT', None)
        taxi_df['tpep_dropoff_datetime'] = taxi_df['tpep_dropoff_datetime'].astype(str).replace('NaT', None)
        
        #replace 'nan' string values with None
        taxi_df_1 = taxi_df.replace('nan', None)
        
        #filter invalid trips
        taxi_df_1 = taxi_df_1[(taxi_df_1['trip_distance'] > 0) & (taxi_df_1['fare_amount'] > 0)]
    
    
        #load NTA  & taxi zone shapefile
        nta_gdf = gpd.read_file(nta_shapefile_path)
        taxi_zone_gdf = gpd.read_file(taxi_zone_shp)
        #load Taxi Zone shapefile
        return taxi_df_1, nta_gdf, taxi_zone_gdf
    except Exception as e:
        print(e)


"""
function to join geographic data to the taxi_data table
"""

def joins(engine):
    try:
        taxi_data_df = pd.read_sql_query('select * from "taxi_data"',con=engine)
        taxi_zones_df = gpd.read_postgis('select * from "taxi_zones"', con=engine, geom_col = 'geometry')
        # nta_zones_df = gpd.read_postgis('select * from "nta_zones"', con = engine, geom_col = 'geometry')
        #join the taxi_data frame to the taxi_zones_df.
        taxi_data_df = taxi_data_df.merge(taxi_zones_df, left_on='pu_location_id', right_on='LocationID')
        #rename the pick up zone columns
        taxi_data_df = taxi_data_df.rename(columns = {'borough':'pick_up_borough','zone' : 'pick_up_zone'})
        #delete fields
        taxi_data_df = taxi_data_df.drop([ 'OBJECTID', 'Shape_Leng','Shape_Area', 'LocationID','geometry'], axis = 1)
        #join on the drop off location to get drop off zone data
        taxi_data_df = taxi_data_df.merge(taxi_zones_df, left_on='do_location_id', right_on = 'LocationID')
        taxi_data_df = taxi_data_df.rename(columns = {'borough':'drop_off_borough','zone' : 'drop_off_zone'})
        taxi_data_df = taxi_data_df.drop([ 'OBJECTID', 'Shape_Leng','Shape_Area', 'LocationID','geometry'], axis = 1)
        return taxi_data_df
    except Exception as e:
        print(e)
  
"""
Function to visualize the data
"""
def visualize(data_df, taxi_zone_path):
    try:
        
        # Number of trips per pickup borough
        pickup_trip_counts = data_df['pick_up_borough'].value_counts()
        # neighborhood_pickup_counts = data_df['pick_up_zone'].value_counts()
        # Number of trips per dropoff borough
        dropoff_trip_counts = data_df['drop_off_borough'].value_counts()
        # neighborhood_dropoff_counts = data_df['drop_off_zone'].value_counts()
    
        # # Combine both to get total trips per borough
        total_trip_counts_borough = pickup_trip_counts.add(dropoff_trip_counts, fill_value=0)
        # total_trip_counts_neighborhood = neighborhood_pickup_counts.add(neighborhood_dropoff_counts, fill_value = 0)
        
        # Plot the trips per borough
        fig, ax = plt.subplots(figsize=(12,5))
        # plt.figure(figsize=(12, 6))
        sns.barplot(x=total_trip_counts_borough.index, y=total_trip_counts_borough.values, palette='viridis')
        plt.title('Number of Trips per Borough')
        plt.xlabel('Borough')
        plt.ylabel('Number of Trips')
        plt.xticks(rotation=45)
        ax.yaxis.get_major_formatter().set_scientific(False)
        ax.yaxis.get_major_formatter().set_useOffset(False)
        plt.show()
        
        # Create a new DataFrame with combined trip counts per neighborhood by borough
        pickup_counts = data_df.groupby(['pick_up_borough', 'pick_up_zone']).size().reset_index(name='pickup_trip_count')
        dropoff_counts = data_df.groupby(['drop_off_borough', 'drop_off_zone']).size().reset_index(name='dropoff_trip_count')
        
        # Merge the pickup and dropoff counts
        trip_counts = pd.merge(pickup_counts, dropoff_counts, left_on=['pick_up_borough', 'pick_up_zone'], right_on=['drop_off_borough', 'drop_off_zone'], how='outer')
        
        # Fill NaN values with 0
        trip_counts.fillna(0, inplace=True)
        
        # Combine pickup and dropoff trip counts
        trip_counts['total_trip_count'] = trip_counts['pickup_trip_count'] + trip_counts['dropoff_trip_count']
        
        # Drop unnecessary columns
        trip_counts.drop(columns=['pickup_trip_count', 'dropoff_trip_count'], inplace=True)
        
        # Rename columns for clarity
        trip_counts.rename(columns={'pick_up_borough': 'borough', 'pick_up_zone': 'neighborhood'}, inplace=True)
        
        # Sort the DataFrame for better visualization
        trip_counts.sort_values(by='total_trip_count', ascending=False, inplace=True)
        
        # Plot the data
        boroughs = trip_counts['borough'].unique()
        # Plot the data for each borough separately
        for borough in boroughs:
            borough_data = trip_counts[trip_counts['borough'] == borough]
            plt.figure(figsize=(14, 8))
            sns.barplot(data=borough_data, x='neighborhood', y='total_trip_count', palette='viridis')
            plt.title(f'Number of Trips per Neighborhood in {borough}')
            plt.xlabel('Neighborhood')
            plt.ylabel('Number of Trips')
            plt.xticks(rotation=90)
            plt.tight_layout()
            plt.show()
        
        # Average trip distance and fare by pickup borough
        avg_trip_distance_pickup = data_df.groupby('pick_up_borough')['trip_distance'].mean()
        avg_fare_pickup = data_df.groupby('pick_up_borough')['fare_amount'].mean()
        
        # Average trip distance and fare by pickup neighborhood
        avg_trip_distance_pickup_neighborhood = data_df.groupby('pick_up_zone')['trip_distance'].mean()
        avg_fare_pickup_neighborhood = data_df.groupby('pick_up_zone')['fare_amount'].mean()
    
        
        # Combine results into one DataFrame
        avg_trip_metrics_borough = pd.DataFrame({
            'avg_trip_distance': avg_trip_distance_pickup,
            'avg_fare': avg_fare_pickup
        }).fillna(0)
        avg_trip_metrics_borough.to_csv('borough_metrics.csv')
        
        avg_trip_metrics_neighborhood = pd.DataFrame({
            'avg_trip_distance': avg_trip_distance_pickup_neighborhood,
            'avg_fare': avg_fare_pickup_neighborhood})
        avg_trip_metrics_neighborhood.to_csv('neighborhood_metrics.csv')
        
        
        taxi_zones = gpd.read_file(taxi_zone_path)
        taxi_zones['zone'] = taxi_zones['zone'].astype(str)
        # print(taxi_zones)
        # pd.set_option('display.max_columns', None)
        # print(trip_counts.head())
        trip_counts['neighborhood'] = trip_counts['neighborhood'].astype(str)
        merged = taxi_zones.set_index('zone').join(trip_counts.set_index('neighborhood'), lsuffix='_zone', rsuffix='_data')
        fig, ax = plt.subplots(1, 1, figsize=(15, 10))
        merged.plot(column='total_trip_count', ax=ax, legend=True, cmap='OrRd',
                    legend_kwds={'label': "Number of Trips",
                                 'orientation': "horizontal"})
        ax.set_title('Number of Trips per Neighborhood')
        ax.set_axis_off()
        plt.show()
        
        
        #Peak hours analysis
        data_df['tpep_pickup_datetime'] = pd.to_datetime(data_df['tpep_pickup_datetime'], utc=True)
        # Extract the hour from the pickup datetime
        data_df['pickup_hour'] = data_df['tpep_pickup_datetime'].dt.hour
        # print(data_df.columns)
        # Group by neighborhood and hour, and count the number of trips
        hourly_trip_counts = data_df.groupby(['pick_up_zone', 'pickup_hour', 'pick_up_borough']).size().reset_index(name='trip_count')
        # print(hourly_trip_counts.columns)
        # Find the hour with the maximum number of trips for each neighborhood
        peak_hours = hourly_trip_counts.loc[hourly_trip_counts.groupby('pick_up_zone')['trip_count'].idxmax()]
        # print(peak_hours.columns)
        # Rename columns for clarity
        peak_hours.rename(columns={'pick_up_zone': 'neighborhood', 'pickup_hour': 'peak_hour'}, inplace=True)
        boroughs = peak_hours['pick_up_borough'].unique()
        for borough in boroughs:
            borough_data = peak_hours[peak_hours['pick_up_borough'] == borough]
            
            # Plot peak hours
            plt.figure(figsize=(14, 8))
            sns.barplot(data=borough_data, x='neighborhood', y='peak_hour', palette='viridis')
            plt.title(f'Peak Hour of Operation per Neighborhood in {borough}')
            plt.xlabel('Neighborhood')
            plt.ylabel('Peak Hour')
            plt.xticks(rotation=90)
            plt.tight_layout()
            plt.show()
    except Exception as e:
        print(e)
    
    
if __name__ == "__main__":
    #db connection

    conn                    = psycopg2.connect(database = "YOUR DATABASAE", user ='postgres', password = "YOUR PASSWORD", host = '127.0.0.1',
                                           port = '5432')
    
    engine                  = create_engine("postgresql://postgres:{0}@127.0.0.1:5432/blumen_coding_prompt".format(your_password))

    #files
    green_taxi_data_path    = "data/green_tripdata_2024-04.parquet"
    yellow_taxi_data_path   = "data/yellow_tripdata_2024-04.parquet"
    nta_shapefile_path      = "data/nyct2020_24b/nyct2020_24b/nyct2020.shp"
    taxi_zone_path          = "data/taxi_zones/taxi_zones.shp"
    # # Load and process data
    taxi_df, nta_gdf, taxi_zone_gdf = prep_data(conn, yellow_taxi_data_path, green_taxi_data_path, nta_shapefile_path, taxi_zone_path)
    execute_values(conn, taxi_df, "taxi_data")
    nta_gdf.to_postgis("nta_zones", engine, if_exists='replace', index=False, index_label='Index')
    taxi_zone_gdf.to_postgis("taxi_zones", engine, if_exists='replace', index=False, index_label='Index')
    
    data_df = joins(engine)
    visualize(data_df, taxi_zone_path)
