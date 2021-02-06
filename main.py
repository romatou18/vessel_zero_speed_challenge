#!/usr/bin/python3

import lib
import pandas as pd
import ijson
import sys, getopt


def main(argv):
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
    inputfile, outputFile = main(sys.argv[1:])

    # lib.extract_json(inputfile)
    data = lib.parseInputFile(inputfile)
    print(data[0])
  
    stops = pd.DataFrame(data)
    print(stops.head())

    # parse floats for coords
    stops["Longitude"] = stops["Longitude"].apply(lib.parse_float)
    stops["Latitude"] = stops["Latitude"].apply(lib.parse_float)

    # get the unique vessel ids
    vesselIDs = stops["UserID"].unique()

    print("vessel IDs =", vesselIDs)

    # for each vessel create a separate dataset, compute displacement and speed.


    all_zero_speed_geojson_list = []

    for vid in vesselIDs[0:20]:

        df_vessel = stops[stops['UserID'].isin([vid])]
        df_sorted = df_vessel.sort_values(by=['UTCTimeStamp'], ascending=True)
        df_sorted = df_sorted.drop_duplicates()
        df_sorted = df_sorted.drop_duplicates(subset=['Latitude', 'Longitude'])

        # calculate time difference

        df_sorted['time'] = pd.to_datetime(df_sorted['UTCTimeStamp'], unit='s').apply(lambda x: x.to_datetime64())
        df_sorted = lib.subgroup_timedelta(df_sorted, 'time', 'time_diff_s')

        # calculate displacement to N-1
        df_sorted = lib.subgroup_distance(df_sorted, 'Latitude', 'Longitude', 'distance_m')

        df_sorted = pd.DataFrame(df_sorted, columns=['UserID', 'Latitude', 'Longitude', 'time', 'time_diff_s', 'distance_m'])
        df_sorted = lib.compute_speed_kn(df_sorted, 'distance_m', 'time_diff_s','speed_kn')

        #removing all rows for which the speed is less than 1 kn
        df_sorted = df_sorted[df_sorted.speed_kn < 1.0]

        #removing the first chronological row since the speed is always 0 when starting the reporting
        # df_sorted = df_sorted[1:]

        # print(df_sorted.head())

        lib.dataframe_to_geojson_feature_list(df_sorted, all_zero_speed_geojson_list)
    
    # wrapping up the final geojson feature list object
    print(" all_zero_speed_geojson_list:")
    print(all_zero_speed_geojson_list)
    wrapped_list = lib.geojson_list(all_zero_speed_geojson_list)


    print("writting output geojson file to {}".format(outputFile))
    lib.write_geojson_file(wrapped_list, outputFile)



