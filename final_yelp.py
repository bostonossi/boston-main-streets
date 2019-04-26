import sys
import csv
import requests, json 
import pandas as pd
from pandas import ExcelWriter
import time


#input example:
#python <name_of_the_script> <file_name_to_read_from>
#python final_yelp.py spark_output_intersections.csv

#output:
#an excel file with intersections with longitude and latitude
#spark_output_yelp.xlsx



name_of_script = sys.argv[0]
filename = sys.argv[1]
df = pd.read_csv(filename)

corners = df['intersection_name']
corner_dict = {}    #we will store all of the dataframes in this dictionary
for name in corners:
    corner_dict[name] = pd.DataFrame()


url = 'https://api.yelp.com/v3/businesses/search'
client_id = 'Ll7WkVZDcZCR9sWpL1btUg'
api_key_yelp = 'dbDtwne7dQZloCURvNmt9y_YjNI-oZt35uOZy2cbb4l8Jb0OLiznTqi8l8K-vbTompNSJBnoKQMuZODgSZ2x8nfGJKwg7To5KerutYxn8I-AjXp2tHtkKh_ko_a5XHYx'
headers = {
        'Authorization': 'Bearer {}'.format(api_key_yelp),
    }
for i in range(len(df)):
    longitude = df['intersection_long'].loc[i]
    latitude = df['intersection_lat'].loc[i]
    url_params = {'longitude':float(longitude),
         'latitude':float(latitude)}

    response = requests.get(url, headers=headers, params=url_params).json()
    data_frame = pd.DataFrame.from_dict(response['businesses'])
    corner_dict[corners[i]] = data_frame

df = pd.concat([df.assign(idx=key) for key, df in corner_dict.items()], ignore_index=True)
writer = ExcelWriter('spark_output_yelp.xlsx')
df.to_excel(writer)
writer.save()