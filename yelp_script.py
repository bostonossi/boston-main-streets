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
df = df.dropna(subset = ['intersection_name'])   #if there is an Nan value in a column of intersection_name, then drop the corresponding row
corners = df['intersection_name']
corner_dict = {}    #we will store all of the dataframes in this dictionary
for name in corners:
    corner_dict[name] = pd.DataFrame()
   
    
    
url = 'https://api.yelp.com/v3/businesses/search'
client_id = ''  #your client ID for yelp here
api_key_yelp = ''  #your yelp API key here
headers = {
        'Authorization': 'Bearer {}'.format(api_key_yelp),
    }
limit = 50
offset = 0
number_of_results = 4 #total number of businesses to be returned per intersection = number_of_results*50
radius = 1000 #radius around the intersection that we want to get covered
for i in range(len(corner_dict)):
    longitude = df['intersection_long'].loc[i]
    latitude = df['intersection_lat'].loc[i]
    offset = 0
    df_added = pd.DataFrame()
    for j in range(number_of_results):
        
        url_params = {'longitude':float(longitude),
             'latitude':float(latitude),
             'limit': limit,
             'offset':offset,
             'radius':radius
                     }
        response = requests.get(url, headers=headers, params=url_params).json()
        if 'businesses' in response.keys():
            new_df = pd.DataFrame.from_dict(response['businesses'])
            df_added = df_added.append(new_df, ignore_index='True')
            offset += 50
        else:
            break
    corner_dict[corners[i]] = df_added

df = pd.concat([df.assign(idx=key) for key, df in corner_dict.items()], ignore_index=True, sort=False)
writer = ExcelWriter('spark_output_yelp_Mission_Hill_final.xlsx')
df.to_excel(writer)
writer.save()



#for finding the main catefory of a given alias from the results:
# string = df['categories'].loc[5][0]['alias']
# for i in range(len(data)):
#     if string in data[i]['alias']:
#         parent_string = data[i]['parents'][0]


