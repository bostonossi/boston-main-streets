import sys
import csv
import requests, json 
import pandas as pd
from pandas import ExcelWriter
import time


#input example:
#python <name_of_the_script> <file_name_to_read_from>
#python get_intersections.py Allston-Village-Business-Inventory.csv 

#output:
#an excel file with intersections with longitude and latitude
#spark_output_intersections.csv



name_of_script = sys.argv[0]
filename = sys.argv[1]
df = pd.read_csv(filename)


# Getting addresses from the excel file, and then find the nearest intersection

true_address = []
for i in range(len(df['Street'])):                                #combining addresses so that it is in required format
    true_address.append(str(df["#"][i])+ " " +df["Street"][i]) #for the API request
df['true_address'] = true_address


my_api_key = #your api key here       #API key for getting longitude and latitude for an address
                                                             #5$ per 1,000 request

url_main = "https://maps.googleapis.com/maps/api/geocode/json?address="
url_last = ",+Boston,+MA&key=" + my_api_key
longitude = []         #store longitude of the addresses
latitude = []          #store latitude of the addresses
for i in range(len(df)):
    url_request = url_main + df['true_address'][i] + url_last
    r_test = requests.get(url_request)
    x = r_test.json()
    longitude.append(x['results'][0]['geometry']['location']['lng'])
    latitude.append(x['results'][0]['geometry']['location']['lat']) 


result_first = 'http://api.geonames.org/findNearestIntersectionJSON?lat=' #find the nearest intersection for a given
result_last = '&username=user_name_here'                                          #longitude and latitude pair
intersection_name = []
intersection_long = []
intersection_lat = []
for i in range(len(longitude)):
    url_final = result_first + str(latitude[i]) + '&lng=' + str(longitude[i]) + result_last
    r_test = requests.get(url_final).json()
    string_add = r_test['intersection']['street1'] + " and " + r_test['intersection']['street2'] #adding the intersection name
    intersection_long.append(r_test['intersection']['lng'])
    intersection_lat.append(r_test['intersection']['lat'])
    intersection_name.append(string_add)




df['longitude'] = longitude    #longitude for the addresses given in the excel file
df['latitude'] = longitude     #latitude for the addresses given in the excel file
df['intersection_long'] = intersection_long #longitude for the nearest intersection for a given address
df['intersection_lat'] = intersection_lat   #latitude for the nearest intersection for a given address
df['intersection_name'] = intersection_name




df.to_csv('spark_output_intersections.csv',index=False)

# writer = ExcelWriter('spark_output_intersections.xlsx')   #storing the output here
# df.to_excel(writer)
# writer.save()