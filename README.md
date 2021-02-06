# vessel_zero_speed_challenge
Coding challenge
Python program running on v3.8.2

## Pre requisite
- Install python 3.8 
- Install python-pip 
`sudo apt-get update && sudo apt-get install python-pip3`
- install the requirements.txt file using the following command
`pip3 install -r requirements.txt`
- alternatively use the following pip3 command
`pip3 install pandas==1.2.1 numpy==1.20.0 ijson==3.1.3`

## Code files

- vessels.py contains the main program to run as follows
`python3 vessels.py -i inputfilePath -o outpuFilePath`
- lib.py : contains the various helpers that implement the sub tasks and steps of the algorithm
- test.py : test runner used to fiddle around along the process.


## Notes
- Another solution thought of was to use a SQL DB instead of python and Pandas. The clear advantage would have been the processing line by line and the easier splitting of the processing in batches.
This python solution is suboptimal due to the limitations of pandas lib. The actually ijson reader lib does a good job at parsing the json files in chunks and the highter limit of memory usage then is the size defined for the chunks.

I did not choose that thinking that I could get away with this solution. I wanted to try using the multiprocessing lib that is quite powerful and allows leveraging multiple cores but did not have the time in the end. But would have expected a factor 2 to 4 improvement on an average end consumer laptop.

If it was to be done again, i'd definitely us a bigger database system and a compiled program probably in GO.

## demo run data and screenshot
The following command was run with the example file cut down to 1.1GB
`./vessels.py -i test2.json -o zero_speed2.json`

The result file is attached in the project directory as well as a screenshot from geojson.io. PArtial preview as the website crashed before loading the whole file unsurprisingly.
Screenshot_zero_speed2.png




