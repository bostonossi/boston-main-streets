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
#reading csv file
df = pd.read_csv(filename)

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
df['true_address'] = df['Address']
df['true_address'].replace(" ","+",regex=True)   #right format for the API request

#for Brighton, Chinatown, Fields Corner, Four Corners
# df['true_address'] = df['Unnamed: 5']
# df['true_address'].replace(" ","+",regex=True)   #right format for the API request
# df['true_address'].replace(",","+",regex=True)




my_api_key = ''     				#your Google API key for geocode here  
									#Google API key for getting longitude and latitude for an address
                                                             #5$ per 1,000 request

url_main = "https://maps.googleapis.com/maps/api/geocode/json?address="
url_last = ",+Boston,+MA&key=" + my_api_key
longitude = []         #store longitude of the addresses
latitude = []          #store latitude of the addresses
for i in range(len(df)):
    url_request = url_main + str(df['true_address'][i]) + url_last
    r_test = requests.get(url_request)
    x = r_test.json()
    if x['status'] == 'OK':
        longitude.append(x['results'][0]['geometry']['location']['lng'])
        latitude.append(x['results'][0]['geometry']['location']['lat']) 


result_first = 'http://api.geonames.org/findNearestIntersectionJSON?lat=' #find the nearest intersection for a given longitude and latitude pair
result_last = '&username='                                          #your geoname username here
intersection_name = []
intersection_long = []
intersection_lat = []
for i in range(len(longitude)):
    url_final = result_first + str(latitude[i]) + '&lng=' + str(longitude[i]) + result_last
    r_test = requests.get(url_final).json()
    if "intersection" in r_test.keys():     #making sure that there is no error in the output
        string_add = r_test['intersection']['street1'] + " and " + r_test['intersection']['street2'] #adding the intersection name
        intersection_long.append(r_test['intersection']['lng'])
        intersection_lat.append(r_test['intersection']['lat'])
        intersection_name.append(string_add)




df['longitude'] = pd.Series(longitude)    #longitude for the addresses given in the excel file
df['latitude'] = pd.Series(latitude)     #latitude for the addresses given in the excel file
df['intersection_long'] = pd.Series(intersection_long) #longitude for the nearest intersection for a given address
df['intersection_lat'] = pd.Series(intersection_lat)   #latitude for the nearest intersection for a given address
df['intersection_name'] = pd.Series(intersection_name)


#didn't find an efficient way to deal with the duplicates; if you know of a function to do all the work, please just use it
dropped_column_list = []
for i in range(len(df['intersection_long'])):
    for j in range(i+1,len(df)):
        if i not in dropped_column_list and j not in dropped_column_list:
            if df['intersection_long'].loc[i] == df['intersection_long'].loc[j] and df['intersection_lat'].loc[i] == df['intersection_lat'].loc[j]:
                #print("entered this condition")
                df.drop(j,inplace=True)
                dropped_column_list.append(j)
df.index = range(len(df))


df.to_csv('spark_output_intersections_Mission_Hill_final.csv',index=False)
#if you want an Excel format comment the above line and remove comments from lines below
# writer = ExcelWriter('spark_output_intersections.xlsx')   #storing the output here
# df.to_excel(writer)
# writer.save()