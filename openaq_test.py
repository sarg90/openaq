import pandas as pd
import numpy as np
import requests
import json

##Front end will send query1 and query2 as follows
##it works with specific coordinates or just with country code
q1 = {'country':'MX','parameter':'pm25','coordinates':'32.594177,-115.423300','radius':'100000'}
q2= {'country':'MX','coordinates':'32.594177,-115.423300','radius':'100000'}

##try sending this without the coordinates
##q1 = {'country':'US','parameter':'pm25','radius':'1'}
##q2= {'country':'US','radius':'1'}

##On a flask application this function can be import like
##from openaq_test.py import openaq_call
##once tested this can be install as a whl library

def openaq_call(query,query2):
    try:
        df_final = pd.DataFrame({'A': []})
        ##Setting response from measurements using query
        measurements_response = requests.get("https://docs.openaq.org/v2/measurements",params=query)
        ##Creating dataframe from measurements response
        df_measurements = pd.json_normalize(measurements_response.json()['results'])
        ##Renaming date.utc column from measurement dataframe to lastUpdated, as this will be used as join key to locations dataframe
        df_measurements.rename(columns= {'date.utc':'lastUpdated'},inplace=True)
        ##Obtaining only most recent data from df_measurements by dropping duplicates by locationId
        ##the api already respond with them in order so we keep the first of each location
        df_measurements=df_measurements.drop_duplicates(subset='locationId',keep='first').reset_index(drop=True)

        ##Setting response from locations using query2
        locations_response = requests.get("https://docs.openaq.org/v2/locations", params=query2)
        df_locations = pd.json_normalize(locations_response.json()['results'])
        ##Expanding parameters from location response to enhance measurement dataframe
        df_locations = df_locations[['name', 'id']].rename(columns= {'id':'locationId'}).join(df_locations['parameters'].apply(lambda x: pd.Series(json.loads(json.dumps([item for item in x if item["parameter"] == "pm25"][0])))))

        ##Inner join measurement dataframe with location dataframe to add avg column as enhancement.
        ##In addition, this will return just the most recent read for response
        df_final = pd.merge(df_measurements, df_locations,on=["locationId", "parameter"],how='left')

        ##Extra tranformation that will calculate the average of the parameter applied to whole final dataframe
        df_final['total_set_avg'] = df_final.groupby('parameter')['value'].transform(np.average)
        df_final['total_set_max'] = df_final.groupby('parameter')['value'].transform(np.max)
        df_final['total_set_min'] = df_final.groupby('parameter')['value'].transform(np.min)
        #Dropping and renaming columns
        df_final=df_final.drop(columns=['unit_y','lastUpdated_y']).rename(columns= {'unit_x':'unit','lastUpdated_x':'lastUpdated'},inplace=False)

        ##Status Calc
        if len(df_final.index) > 0:
            status = 'Success'
        else:
            status = 'Empty, check input parameters'
    except:
        status = 'API call issue'
    return df_final,status

df,response_status = openaq_call(q1,q2)

##To a local csv to inspect manually results
df.to_csv('out.csv',index=False)
print(df)
print(response_status)

##A conversation needs to be held with business side to understand what
##to do with blank values on average column, this means that theres no location
##data on that location




