import sys
import csv
import requests, json 
import pandas as pd
from pandas import ExcelWriter
import time
import argparse


#input example:
#python <name_of_the_script> <file_name_to_read_from>
#python get_intersections.py Allston-Village-Business-Inventory.csv 

#output:
#an excel file with intersections with longitude and latitude
#spark_output_intersections.csv


input = argparse.ArgumentParser()
input.add_argument("-s", "--spreadsheet", required=True, help="Spreadsheet consisting of addresses")
input.add_argument("-c", "--city", type =str, default="Boston", help="city in which we are interested")
input.add_argument("-st", "--state", type =str, default="MA", help="state where the city of interest is located")
input.add_argument("-r", "--radius", type =int, default=1000, help="radius to search within")
input.add_argument("-n", "--results", type =int, default=20, help="number of results to return per intersection, i.e. results*50")
arguments = vars(input.parse_args())
#reading csv file
df = pd.read_csv(arguments['spreadsheet'])



city = arguments["city"]
state = arguments["state"]
number_of_results = arguments["results"] #total number of businesses to be returned per intersection = number_of_results*50
radius = arguments["radius"] #radius around the intersection that we want to get covered
spreadsheet_name = str(arguments['spreadsheet'])[:-3]
spreadsheet_name = "yelp_" + spreadsheet_name + "xlsx"






#reading csv for Chinatown; this problem is caused by excel files not following same format for all mainstreets
#do it in the case if the columns aren't already named in the csv file or you want to change the names
# colnames=['Names', 'Address', 'Cellphone'] 
# df = pd.read_csv(filename, names=colnames, header=None)


# Getting addresses from the excel file, and then find the nearest intersection
#there are different codes for different neighborhoods because excel files don't follow uniform format
#For Allston, East Boston, Ashmont, Grove Hall, Hyde Jackson, Hyde Park, JP Centre South:
# true_address = []
# for i in range(len(df['Street'])):                                #combining addresses so that it is in required format
#     true_address.append(str(df["#"][i])+ " " + str(df["Street"][i])) #for the API request
# df['true_address'] = true_address
# df['true_address'].replace(" ","+",regex=True)   #right format for the API request


#for Bowdoin, Mission Hill
# list_columns = list(df.columns)  #for debugging purposes in case if there is a problem with reading a column
# print(list_columns)
def get_address(df):
    df['true_address'] = df['Billing Street']
    df['true_address'].replace(" ","+",regex=True)   #right format for the API request
    return df


#for Brighton, Chinatown, Fields Corner, Four Corners
# df['true_address'] = df['Unnamed: 5']
# df['true_address'].replace(" ","+",regex=True)   #right format for the API request
# df['true_address'].replace(",","+",regex=True)


#use this to get the longitude and latitude of the addresses provided in the spreadsheet
def address_longitude(df):
    longitude = []         #store longitude of the addresses
    latitude = []          #store latitude of the addresses
    my_api_key = ''     				#your Google API key for geocode here  
    									#Google API key for getting longitude and latitude for an address
                                                                 #5$ per 1,000 request

    url_main = "https://maps.googleapis.com/maps/api/geocode/json?address="
    url_last = ",+" + city + "," + "+" + state + "&key=" + my_api_key
    for i in range(len(df)):
        url_request = url_main + str(df['true_address'][i]) + url_last
        r_test = requests.get(url_request)
        x = r_test.json()
        if x['status'] == 'OK':
            longitude.append(x['results'][0]['geometry']['location']['lng'])
            latitude.append(x['results'][0]['geometry']['location']['lat']) 
    df['longitude'] = pd.Series(longitude)    #longitude for the addresses given in the excel file
    df['latitude'] = pd.Series(latitude)     #latitude for the addresses given in the excel file
    return df



#find the nearest intersection for a given longitude and latitude pair
def find_intersection(df):
    intersection_name = []
    intersection_long = []
    intersection_lat = []
    result_first = 'http://api.geonames.org/findNearestIntersectionJSON?lat='
    result_last = '&username='                                          #your geoname username here
    for i in range(len(df['longitude'])):
        url_final = result_first + str(df['latitude'].loc[i]) + '&lng=' + str(df['longitude'].loc[i]) + result_last
        r_test = requests.get(url_final).json()
        if "intersection" in r_test.keys():     #making sure that there is no error in the output
            string_add = r_test['intersection']['street1'] + " and " + r_test['intersection']['street2'] #adding the intersection name
            intersection_long.append(r_test['intersection']['lng'])
            intersection_lat.append(r_test['intersection']['lat'])
            intersection_name.append(string_add)
    df['intersection_long'] = pd.Series(intersection_long) #longitude for the nearest intersection for a given address
    df['intersection_lat'] = pd.Series(intersection_lat)   #latitude for the nearest intersection for a given address
    df['intersection_name'] = pd.Series(intersection_name)
    df.drop_duplicates(subset='intersection_name', inplace=True)
    df = df.dropna(subset = ['intersection_name'])
    return df




#we will store all of the dataframes in this dictionary
def dictionary_df(df):
    corner_dict = {} #to store the data frames
    corners = df['intersection_name']
    for name in corners:
        corner_dict[name] = pd.DataFrame()
    return corner_dict
   
    

def get_businesses(df, corner_dict): 
    df = df.reset_index(drop=True)   
    corners = df['intersection_name']
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
    return corner_dict

def main_function(df):
    final_df = get_address(df)
    final_df = address_longitude(final_df)
    final_df = find_intersection(final_df)
    final_corner_dict = dictionary_df(final_df)
    final_corner_dict = get_businesses(final_df, final_corner_dict)
    final_df = pd.concat([df.assign(idx=key) for key, df in final_corner_dict.items()], ignore_index=True, sort=False)
    final_df.drop_duplicates(subset='name',inplace=True)
    final_df = final_df[final_df.distance <= 1000]
    writer = ExcelWriter(spreadsheet_name)
    final_df.to_excel(writer)
    writer.save()

main_function(df)    


