#LabchartProcessDropout-Single-Script.py
#By: Kyle Fricke, PhD
#Date: July 2020
#Rev: 0
#Change Notes:
# -Initial Version

#----------------------------------------------------------------------------------------------------------------------#
# This script processes a Labchart text file with columns of data that have the custom data dropout function applied
# The output of this script will be to a series of text files with total record time, dropout time, and % dropout.
# That file can be combined with others to generate a dropout rate for a complete dataset
# This script  process just single txt (labchart) files - use LabchartProcessDropout-Multi for multi file access
# Outputs are placed in the outputs folder
# Function in Labchart: Threshold(Differentiate(Window(Differentiate(Ch2),-100,100),10,0,1),1) where Ch2 is
# the pressure channel used
#All critical functions are located in ProcessLabchartDropoutxt
#----------------------------------------------------------------------------------------------------------------------#


# Previous development versions that are no longer used:
    #labchartpanda.py
    #SingleFileLabchartProcess.py
    #MultiFileLabchartProcess.py
#

#----------------------------------------------------------------------------------------------------------------------#
import ProcessLabchartDropoutTxt
import argparse
import pandas as pd
import os
import datetime  # import datetime module so that we can print the timestamp for beginning and end
#----------------------------------------------------------------------------------------------------------------------#




#----------------------------------------------------------------------------------------------------------------------#
#Arguement Parser
def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return open(arg, 'r')  # return an open file handle


parser = argparse.ArgumentParser(description='Process Labchart Data Dropout Txt Files')  # Create Parser object for CMD parsing
parser.add_argument('-i', dest='InputFilename', required=True, help='Input txt file with multi-col data to process',
                    metavar="FILE", type=lambda x: is_valid_file(parser, x))
# parser.add_argument('-i', dest='InputFilename', required = True, help = 'Input txt file with multi-col data to process', metavar="FILE", type=str)
parser.add_argument('-o', dest='OutputFilename', required=False, help='Output filename to be used', metavar="FILE",
                    type=str)
parser.add_argument('-c', dest='ProcessChannels', required=False, help='# of Channels to Process', metavar="Channels",
                    type=int, default=3)
parser.add_argument('-ch', dest='Chunksize', required=False, help='Chunksize for processing', metavar="Chunksize",
                    type=int, default=100000)
parser.add_argument('-p', dest='Path', required=False, help='Path Used', metavar="Path", type=str, default="./")
parser.add_argument('-v', dest='Verbose', required=False, help='Verbose Mode', metavar="Verbose", type=int)

args = parser.parse_args()

# Use input name as default output name
args.OutputFilename = args.OutputFilename if args.OutputFilename else os.path.splitext(args.InputFilename.name)[0]
Path = args.Path
#----------------------------------------------------------------------------------------------------------------------#


#----------------------------------------------------------------------------------------------------------------------#
#File Making and setup

#Make Output Directory
dir = os.path.join(Path, "Outputs")
if not os.path.exists(dir):
    os.mkdir(dir)


# Notes:
# join will join the two paths together, where dir is the output path and
# os.path.splitext(os.path.basename(f))[0] will return just the filename of the inputted file
# We do this so that we can extract the file name add a suffex to it and save it in the output folder denoted by output


InputFileName = Path + args.InputFilename.name
#We are combining the input name with the path and then removing it. This code just keeps it consistant with multi code
OutputFileName = os.path.join(dir, os.path.splitext(os.path.basename(InputFileName))[0] + 'Output-Verbose.txt')
OutputFileNamecsv = os.path.join(dir, os.path.splitext(os.path.basename(InputFileName))[0] + 'Output-Dataframe.csv')
P_channels = args.ProcessChannels
Debug = args.Verbose
Chunksize = args.Chunksize



if args.Verbose == 1:
    # print(args)
    # print(str(args.InputFilename))
    # print(str(args.OutputFilename))
    # print(str(args.Path))
    print('Input FileName: ' + InputFileName)
    print('Output Filename: ' + OutputFileName)
    print('Number of Channels to be Processed: ' + str(args.ProcessChannels))
#----------------------------------------------------------------------------------------------------------------------#



#----------------------------------------------------------------------------------------------------------------------#
#Start Times and write to file

#open outputfile:
outputfile = open(OutputFileName,'a+') #open and append (or create if it doesn't exist) the output file for writing
start_time = datetime.datetime.now().time().strftime('%H:%M:%S') # Start time
print("Process started at " + start_time)
print("Start to Process:" + str(InputFileName))
outputfile.write("Process started at: " + str(start_time) + "\n") #print timestamp
outputfile.write("Start to Process: " + str(InputFileName) + "\n")
outputfile.close()
#----------------------------------------------------------------------------------------------------------------------#



#----------------------------------------------------------------------------------------------------------------------#
#Function Calls
#Process Header, Read in data, Process dataframe - See ProcessLabchartDropoutTxt.py for more details
Interval_float, Col_Names = ProcessLabchartDropoutTxt.read_header(InputFileName, OutputFileName, P_channels, Debug)
Data = ProcessLabchartDropoutTxt.read_data(InputFileName, Col_Names, Chunksize, Debug)
OutputDataFrame = ProcessLabchartDropoutTxt.calc_data_drop(Data,OutputFileName,OutputFileNamecsv,Col_Names,Interval_float,Debug)
#----------------------------------------------------------------------------------------------------------------------#



#----------------------------------------------------------------------------------------------------------------------#
#End Time and wrap up

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
