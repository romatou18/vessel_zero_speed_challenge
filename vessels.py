#!/usr/bin/python3

import lib
import pandas as pd
import ijson
import sys, getopt
import time

def parseArgs(argv):
    inputfile = ''
    outputfile = ''
    try:
        opts, args = getopt.getopt(argv, "hi:o:", ["ifile=", "ofile="])
    except getopt.GetoptError:
        print('test.py -i <inputfile> -o <outputfile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('test.py -i <inputfile> -o <outputfile>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg
        print('Input file is "', inputfile)
        print('Output file is "', outputfile)
    
    return inputfile, outputfile

# Driver code 
if __name__ == "__main__": 
    inputfile, outputFile = parseArgs(sys.argv[1:])

    # lib.extract_json(inputfile)

    print("Parsing json input file...")
    data = lib.parseInputFile(inputfile)
    print("Done ")

    print("Parsing Data set ")

    stops = pd.DataFrame(data)
    print("Done ")

    print(stops.head())

    # parse floats for coords
    stops["Longitude"] = stops["Longitude"].apply(lib.parse_float)
    stops["Latitude"] = stops["Latitude"].apply(lib.parse_float)

    # get the unique vessel ids
    print("Counting unique vessel IDs")

    vesselIDs = stops["UserID"].unique()
    # for each vessel create a separate dataset, compute displacement and speed.
    all_zero_speed_geojson_list = []
    vessel_counter = 0
    total_vessel = len(vesselIDs)
    print("Found {} unique vessels IDs, starting processing...".format(total_vessel))
    time.sleep(5)

    print("Starting processing stops locations and time ...")
    for vid in vesselIDs:

        #processing vessels one at a time
        df_vessel = stops[stops['UserID'].isin([vid])]

        #sorting them by time stamp, and therefore by now sorted by vessel and time
        df_sorted = df_vessel.sort_values(by=['UTCTimeStamp'], ascending=True)

        # noticed some duplicates at 2 levels: all data duplicated and lat/long for a given vesselID
        # also notice duplicate messageIDs for a given vessel but with different lat/long, ignoring the 
        # message ID for now as really de facto the unique key in the dataset is lat/long/time/vesselID
        df_sorted = df_sorted.drop_duplicates()
        df_sorted = df_sorted.drop_duplicates(subset=['Latitude', 'Longitude'])

        # calculate time difference between 2 points starting at position 1 [0..N]
        #adding a time column containing a readable timestamp for debug purpose, could be skipped for optimisation
        # ended up using the actual timestamp in the geojson feature i.e. time property. Which makes it a lot more useful
        #than simply knowing the vessel ID and location ;-)
        df_sorted['time'] = pd.to_datetime(df_sorted['UTCTimeStamp'], unit='s').apply(lambda x: x.to_datetime64())

        # computing time diff in seconds between Time T1 and TN-1 starting at 1 [0..N]
        df_sorted = lib.subgroup_timedelta(df_sorted, 'time', 'time_diff_s')

        # computing displacement between Time T1 and TN-1 starting at 1 [0..N]
        df_sorted = lib.subgroup_distance(df_sorted, 'Latitude', 'Longitude', 'distance_m')

        #selecting the data required for the final geojson list: coord, time of the stop, and time diff and distance needed here to compute the speed
        df_sorted = pd.DataFrame(df_sorted, columns=['UserID', 'Latitude', 'Longitude', 'time', 'time_diff_s', 'distance_m'])
        
        #computing the speed in knots
        df_sorted = lib.compute_speed_kn(df_sorted, 'distance_m', 'time_diff_s','speed_kn')

        #removing all rows for which the speed is less than 1 kn
        df_sorted = df_sorted[df_sorted.speed_kn < 1.0]

        #removing the first chronological row since the speed is always 0 when starting the reporting
        # df_sorted = df_sorted[1:]
        print("Vessel {} has been processed... {} REMAINING".format(vessel_counter, total_vessel-vessel_counter))

        lib.dataframe_to_geojson_feature_list(df_sorted, all_zero_speed_geojson_list)
        vessel_counter += 1

        print("Vessel {} has been processed...".format(vessel_counter))
        # print(df_sorted.head())
    
    # wrapping up the final geojson feature list object
    print("generating geojson feature list...")
    wrapped_list = lib.geojson_list(all_zero_speed_geojson_list)

    print("writting output geojson file to {}".format(outputFile))
    lib.write_geojson_file(wrapped_list, outputFile)



