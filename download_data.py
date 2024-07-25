# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 10:48:01 2024

@author: JacobNordberg
"""

# download_data.py
import os
import requests

#Function to use requests and os library to create a folder in the active dir and save downloaded
#files to
def download_file(url: str, dest_folder: str):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    filename = url.split('/')[-1]
    file_path = os.path.join(dest_folder, filename)
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return file_path

if __name__ == "__main__":
    yellow_taxi_data_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-04.parquet"
    green_taxi_data_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-04.parquet"
    taxi_zone_shp_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
    nta_shapefile_url = "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/nyct2020_24b.zip"
    # download_file(yellow_taxi_data_url, "data")
    # download_file(green_taxi_data_url, "data")
    # download_file(nta_shapefile_url, "data")
    download_file(taxi_zone_shp_url, "data")
