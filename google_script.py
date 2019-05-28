import sys
import csv
import requests, json 
import pandas as pd
from pandas import ExcelWriter
import time

#for getting the data from google
#there can be some bugs left in the code as this implementation was abondened midway due to cost

name_of_script = sys.argv[0]
filename = sys.argv[1]

corner_list=[[],[]]
corner_list_new=[['42.34880276843278', '42.355566965738625'],['-71.06284761175631', '-71.0971637297189']]     #store longitudes and latitudes of the intersections
corners = ['Kenmore', 'Park Street']

with open(filename) as csvfile:
    readCSV = csv.reader(csvfile,delimiter=',')
    data = list(readCSV)
    for i in range(len(data)):
    	corner_list[0].append(data[i][0])
    	corner_list[1].append(data[i][1])


corner_dict = {}    #we will store all of the dataframes in this dictionary
for name in corners:
    corner_dict[name] = pd.DataFrame()

url_first_part='https://maps.googleapis.com/maps/api/place/nearbysearch/json?location='
radius_url = '&radius=1000&type='
place_types = ['bar', 'cafe']   #types of the places that we are interested in; increase as we go 	 
api_key_test = '&key='  #store your API here


def function_api():
    the_number_of_name = 0    #to correctly access longitude, latitude pair for a corner from the list
    for name, df in corner_dict.items():
        longitude = corner_list[0][the_number_of_name]   #getting longitude for the street corner of interest
        latitude = corner_list[1][the_number_of_name]       #getting latitude for the street corner of interest
        the_number_of_name += 1
        url_addition = str(longitude) + "," + str(latitude)
        for member in place_types:
            number_of_requests = 0
            next_page_token = ''
            while(number_of_requests < 2):
                number_of_requests += 1
                if next_page_token == '':
                    url_test_result = url_first_part  + url_addition + radius_url + str(member) + api_key_test
                    r_test = requests.get(url_test_result)
                    x = r_test.json()
                    data_frame=pd.DataFrame(x['results'])
                    df = df.append(data_frame, ignore_index='True')
                    corner_dict[name] = df
                    if 'next_page_token' in x:
                        next_page_token = x['next_page_token']
                    else:
                        break

                else:
                    url_test_result_second = url_first_part  + url_addition + radius_url + str(member)  + api_key_test + "&pagetoken="+next_page_token
                    time.sleep(3)                                       #sleep is mandatory for the requests; otherwise we won't get the correct response
                    r_test = requests.get(url_test_result_second)
                    x = r_test.json()
                    data_frame=pd.DataFrame(x['results'])
                    df = df.append(data_frame, ignore_index='True')
                    corner_dict[name] = df
                    if 'next_page_token' in x:
                        next_page_token = x['next_page_token']
                    else:
                        break
                
function_api() 

df = pd.concat([df.assign(idx=key) for key, df in corner_dict.items()], ignore_index=True)
writer = ExcelWriter('spark_output_new.xlsx')
df.to_excel(writer)
writer.save()