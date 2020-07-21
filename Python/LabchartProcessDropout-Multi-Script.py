#LabchartProcessDropout-Multi-Script.py
#By: Kyle Fricke, PhD
#Date: July 2020
#Rev: 0
#Change Notes:
# -Initial Version
#----------------------------------------------------------------------------------------------------------------------#


#----------------------------------------------------------------------------------------------------------------------#
# This script processes a Labchart text file with columns of data that have the custom data dropout function applied
# The output of this script will be to a series of text files with total record time, dropout time, and % dropout.
# That file can be combined with others to generate a dropout rate for a complete dataset
# This script  process' multiple (labchart) files - use LabchartProcessDropout-single for single file access
# Outputs are placed in the outputs folder
# Function in Labchart: Threshold(Differentiate(Window(Differentiate(Ch2),-100,100),10,0,1),1) where Ch2 is the pressure channel used
#All critical functions are located in ProcessLabchartDropoutxt
#----------------------------------------------------------------------------------------------------------------------#

#----------------------------------------------------------------------------------------------------------------------#
# Previous development versions that are no longer used:
    #labchartpanda.py
    #SingleFileLabchartProcess.py
    #MultiFileLabchartProcess.py
#----------------------------------------------------------------------------------------------------------------------#

#----------------------------------------------------------------------------------------------------------------------#
import ProcessLabchartDropoutTxt
import argparse
import pandas as pd
import os
import datetime  # import datetime module so that we can print the timestamp for beginning and end
import ReadDirectory
#----------------------------------------------------------------------------------------------------------------------#

#----------------------------------------------------------------------------------------------------------------------#
#Arguement Parser

parser = argparse.ArgumentParser(description='Process Multiple Text Files from Labchart') # Create Parser object for CMD parsing
parser.add_argument('-o', dest='OutputFilename', required=False, help='Output filename to be used', metavar="FILE",
                    type=str)
parser.add_argument('-c', dest='ProcessChannels', required=False, help='# of Channels to Process', metavar="Channels",
                    type=int, default=3)
parser.add_argument('-ch', dest='Chunksize', required=False, help='Chunksize for processing', metavar="Chunksize",
                    type=int, default=100000)
parser.add_argument('-p', dest='Path', required=False, help='Path where files are located', metavar="Path", type=str, default="./")
parser.add_argument('-t', dest='Type', required = False, help = 'Type of Files to Parse', metavar="type", type=str, default=".txt")
parser.add_argument('-v', dest='Verbose', required=False, help='Verbose Mode', metavar="Verbose", type=int)

args = parser.parse_args()
Path = args.Path
P_channels = args.ProcessChannels
Debug = args.Verbose
Chunksize = args.Chunksize
Type = args.Type

if args.Verbose == 1:
    print('Path to Search: ' + str(args.Path))
    print('Type of Files to search: ' + str(args.Type))

# Pandas display options

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
#----------------------------------------------------------------------------------------------------------------------#



#----------------------------------------------------------------------------------------------------------------------#
#File Setup

#Get File Names in folder
File_Names = ReadDirectory.OutputFileNames_Dironly(Path,Type)#Returns a series of file paths to use in specific folder outlined by the path variable


#Make Output Directory
dir = os.path.join(Path, "Outputs")
if not os.path.exists(dir):
    os.mkdir(dir)

# For each file (f) in the series of file names process each
for f in File_Names:
    if Debug == 1:
        print(f)

    #File Name and Processing Info

    #These are selected via the path and not by the user as in the single mode
    InputFileName = f

    #Notes:
    #join will join the two paths together, where dir is the output path and
    #os.path.splitext(os.path.basename(f))[0] will return just the filename of the inputted file
    #We do this so that we can extract the file name add a suffex to it and save it in the output folder denoted by output
    OutputFileName = os.path.join(dir,os.path.splitext(os.path.basename(f))[0] + 'Output-Verbose.txt')
    OutputFileNamecsv = os.path.join(dir,os.path.splitext(os.path.basename(f))[0] + 'Output-Dataframe.csv')

    #Start Times

    #open outputfile:
    outputfile = open(OutputFileName,'a+') #open and append (or create if it doesn't exist) the output file for writing
    start_time = datetime.datetime.now().time().strftime('%H:%M:%S') # Start time

    print("Process started at " + start_time)
    print("Start to Process:" + str(InputFileName))
    outputfile.write("Process started at: " + str(start_time) + "\n") #print timestamp
    outputfile.write("Start to Process:" + str(InputFileName) + "\n")
    outputfile.close()

    #Function Calls
    #Process header, read in data, process dataframe. More info Found in ProcessLabchartDropouttxt.py
    Interval_float, Col_Names = ProcessLabchartDropoutTxt.read_header(InputFileName, OutputFileName, P_channels, Debug)
    Data = ProcessLabchartDropoutTxt.read_data(InputFileName, Col_Names, Chunksize, Debug)
    OutputDataFrame = ProcessLabchartDropoutTxt.calc_data_drop(Data,OutputFileName,OutputFileNamecsv,Col_Names,Interval_float,Debug)

    #End Time:
    #Close Txt File
    end_time = datetime.datetime.now().time().strftime('%H:%M:%S')
    print("Process ended at " + end_time)
    total_time = (datetime.datetime.strptime(end_time,'%H:%M:%S') - datetime.datetime.strptime(start_time,'%H:%M:%S'))
    outputfile = open(OutputFileName,'a+') #open and append (or create if it doesn't exist) the output file for writing
    outputfile.write("Process ended at: " + str(end_time) + "\n") #print timestamp
    outputfile.write("Total process time: "+ str(total_time) + "\n") # print total processing time
    print("Total process time: ", str(total_time) , "\n")
    outputfile.close() #close output file

#----------------------------------------------------------------------------------------------------------------------#


#----------------------------------------------------------------------------------------------------------------------#

#Calculate total processing time of complete set
#Complete Process Time
EndProcessTime = datetime.datetime.now().time().strftime('%H:%M:%S')
Total_ProcessTime = (datetime.datetime.strptime(EndProcessTime,'%H:%M:%S') - datetime.datetime.strptime(start_time,'%H:%M:%S'))
print("Multi File Processing Time: " + str (Total_ProcessTime))


