import sys
import csv
import requests, json 
import pandas as pd
from pandas import ExcelWriter
import time
import argparse


#input example:
#python <name_of_the_script> <file_name_to_read_from>
#python final_yelp.py -- spreadsheet spark_output_intersections.csv --radius 1000 --results 20
#radius and results are optional parameters 

#output:
#an excel file with intersections with longitude and latitude
#spark_output_yelp.xlsx


input = argparse.ArgumentParser()
input.add_argument("-s", "--spreadsheet", required=True, help="Spreadsheet consisting of addresses")
input.add_argument("-r", "--radius", type =int, default=1000, help="radius to search within")
input.add_argument("-n", "--results", type =int, default=20, help="number of results to return per intersection, i.e. results*50")
arguments = vars(input.parse_args())
#reading csv file
df = pd.read_csv(arguments['spreadsheet'])

number_of_results = arguments["results"] #total number of businesses to be returned per intersection = number_of_results*50
radius = arguments["radius"] #radius around the intersection that we want to get covered

df = df.dropna(subset = ['intersection_name'])   #if there is an Nan value in a column of intersection_name, then drop the corresponding row


spreadsheet_name = str(arguments['spreadsheet'])[13:-3]
spreadsheet_name = "yelp_" + spreadsheet_name + "xlsx"
corner_dict = {} #to store the data frames
corners = df['intersection_name']
#we will store all of the dataframes in this dictionary
def all_dictionary():
  
    for name in corners:
        corner_dict[name] = pd.DataFrame()
   
    

def get_businesses():   
    url = 'https://api.yelp.com/v3/businesses/search'
    client_id = ''  #your client ID for yelp here
    api_key_yelp = ''  #your yelp API key here
    headers = {
            'Authorization': 'Bearer {}'.format(api_key_yelp),
        }
    limit = 50
    offset = 0
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


def main_function():
    all_dictionary()
    get_businesses()
    df = pd.concat([df.assign(idx=key) for key, df in corner_dict.items()], ignore_index=True, sort=False)
    df.drop_duplicates(subset='name',inplace=True)
    df = df[df.distance <= 1000]
    writer = ExcelWriter(spreadsheet_name)
    df.to_excel(writer)
    writer.save()

main_function()




#for finding the main catefory of a given alias from the results:
# string = df['categories'].loc[5][0]['alias']
# for i in range(len(data)):
#     if string in data[i]['alias']:
#         parent_string = data[i]['parents'][0]


