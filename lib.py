

import ijson
import numpy
import math
import pandas as pd
import json
from glom import glom
from math import radians, sin, cos, sqrt, asin
 

def parse_float(x):
    try:
        x = float(x)
    except Exception:
        x = 0
    return x

def div_prevent_zero(a,b):
    if b > 0:
        return a / b
    else:
        return 0
 
 # from rosetta code
def haversine(lat1, lon1, lat2, lon2):
    R = 6372.8  # Earth radius in kilometers
 
    dLat = radians(lat2 - lat1)
    dLon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
 
    a = sin(dLat / 2)**2 + cos(lat1) * cos(lat2) * sin(dLon / 2)**2
    c = 2 * asin(sqrt(a))
 
    return R * c

def subgroup_distance(subgroup, long_col, lat_col, distance_col_name):
    distance = []
    current = []
    previous = []
    lat1 = 0
    ling1 = 0
    i = 0
    for index, row in subgroup.iterrows():
        lat1 = row[lat_col]
        long1 = row[long_col]
        current = [lat1, long1]
        if i < 1:
            distance.append(0)
        else:
            diff_km = haversine(current[0], current[1], previous[0], previous[1])
            distance.append(diff_km * 1000)
        
        previous = current
        i+=1
    
    subgroup[distance_col_name] = distance
    return subgroup


def subgroup_timedelta(subgroup, date_col, timediff_col):
    delta = []
    current = 0
    previous = 0
    i = 0
    for index, row in subgroup.iterrows():
        current = row[date_col]
        if i < 1:
            delta.append(0)
        else:
            diff = pd.Timedelta(current - previous).seconds
            delta.append(diff)
        
        previous = current
        i+=1
    
    subgroup[timediff_col] = delta
    return subgroup

def compute_speed_kn(subgroup, dist_col, time_col, speed_col):
    speed = []
    speed_kn = 0
    i=0
    for index, row in subgroup.iterrows():
        if i < 1:
            speed.append(0.0)
        else:
            speed_kn = div_prevent_zero(row[dist_col], row[time_col])
            speed.append(speed_kn / 1.94)
        i+=1
    
    subgroup[speed_col] = speed
    return subgroup

def parseInputFile(filename, messageTypes=[1, 2, 3, 18, 19, 27]):
    data = []
    good_columns = [
    "MessageID",   
    "UserID",
    "Longitude",
    "Latitude",
    "UTCTimeStamp"
    ]
    with open(filename, 'r') as input_file:
        jsonobj = ijson.items(input_file, '', multiple_values=True)
        #jsons = (o for o in jsonobj)
        jsons = (o for o in jsonobj if o['Message']['MessageID'] in messageTypes)
        for row in jsons:
            # if j['Message']['MessageID'] in messageTypes:
            # print(j)
            selected_row = {}
            selected_row["MessageID"] = row['Message']['MessageID']
            selected_row["UserID"] = row['Message']['UserID']
            selected_row["Longitude"] = row['Message']['Longitude']
            selected_row["Latitude"] = row['Message']['Latitude']
            selected_row["UTCTimeStamp"] = row['UTCTimeStamp']

            data.append(selected_row)
            # print(selected_row)
    return data


def generate_geojson_feature(lat, lon, vessel_ID, time):
    prop = {"name": vessel_ID, "time": time}
    geometry = { "type": "Point", "coordinates": [lon, lat]}
    feature = { "type": "Feature", "geometry" : geometry, "properties" : prop}
    return feature

def geojson_list(feat_list):
    features = { "type": "FeatureCollection", "features": feat_list }
    return features

def dataframe_to_geojson_feature_list(subgroup, all_zero_speed_geojson_list):
    for index, row in subgroup.iterrows():
        feat = generate_geojson_feature(row['Latitude'], row['Longitude'], row['UserID'], str(row['time']))
        all_zero_speed_geojson_list.append(feat)


def write_geojson_file(features, filename, chunksize = 5000):
    with open(filename, 'w') as outfile:
        json.dump(features, outfile)





# parse positional reports
def extract_json(filename, messageTypes=[1, 2, 3, 18, 19, 27], chunksize = 10):

    good_columns = [
    "MessageID",   
    "UserID",
    "Longitude",
    "Latitude",
    "UTCTimeStamp"
    ]
    # with open(filename, 'r') as input_file:
    #     jsonobj = ijson.items(input_file, 'Message', multiple_values=True)
    #     jsons = (o for o in jsonobj if o['MessageID'] in messageTypes)
    #     for j in jsons:
    #         print(j)

    df = pd.read_json(filename, lines=True)

    
    df2 = pd.DataFrame( df)
    # df2 = pd.json_normalize(df)
    # df2['UserID'] = df.Message.UserID
    # df2['Longitude'] = df.Message.Longitude
    # df2['UTCTimeStamp'] = df.UTCTimeStamp
    
    print(df2.to_json(orient="records", lines=True))
    # pd.json_normalize(df, 'Message', ['UTCTimeStamp'])
    print(df2.head())


    #filtering the data set to the minimun excluding message types which are unwanted and 
    # columns.
    result = None
    with pd.read_json(filename, lines=True, chunksize=chunksize) as reader:
        for chunk in reader:
            utc = chunk["UTCTimeStamp"]
            msgs = chunk["Message"]
            print(msgs)
            for u in utc:
                print(u)
                result = result.add(u)
            for u in msgs:
                print(u)
            
