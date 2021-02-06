import lib
import pandas as pd
import ijson

# Driver code 
if __name__ == "__main__": 
    lat1 = -16.566380
    lon1 = 79.160320
    lat2 = -16.566080
    lon2 = 79.161023
      
    print(lib.haversine(lat1, lon1,lat2, lon2), "K.M.") 