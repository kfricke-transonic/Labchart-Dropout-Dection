#ProcessDataFrame-Script.py
#By: Kyle Fricke, PhD
#Date: July 2020
#Rev: 0
#Change Notes:
# - Inital Version
#----------------------------------------------------------------------------------------------------------------------#
#This script takes in a path or defaults to the current folder. It will read in all csv files or
#another type and process them into a data frame
#
#This script should be used to process all pre-processed DataDrop dataframes
#Should be used to combine multiple dataframes to generate a comlpete data dropout set
#Dataframes are combined then all unique channels are found and summed
#stats are provided at the end
#----------------------------------------------------------------------------------------------------------------------#

import argparse
import ReadDirectory
import pandas as pd
import os
import datetime  #import datetime module so that we can print the timestamp for beginning and endtamp for beginning and end

#----------------------------------------------------------------------------------------------------------------------#
#Arguement Parser for the Command Line
parser = argparse.ArgumentParser(description='Process DataDropout Frames') # Create Parser object for CMD parsing
parser.add_argument('-o', dest='OutputFilename', required = False, help = 'Output filename to be used', metavar="FILE", type=str, default = 'Multi-Process-Completed-')# use better default name
parser.add_argument('-c', dest='ProcessChannels', required = False, help = '# of Files to Process', metavar="FilesProcess", type=int, default=1)
parser.add_argument('-p', dest='Path', required = False, help = 'Path where files are located', metavar="Path", type=str, default="./")
parser.add_argument('-t', dest='Type', required = False, help = 'Type of Files to Parse', metavar="type", type=str, default=".csv")
parser.add_argument('-v', dest='Verbose', required = False, help = 'Verbose Mode', metavar="Verbose", type=int)

args = parser.parse_args()


Path = args.Path

# Make Output Directory
dir = os.path.join(Path, "Outputs")
if not os.path.exists(dir):
    os.mkdir(dir)


# Notes:
# join will join the two paths together, where dir is the output path and
# os.path.splitext(os.path.basename(f))[0] will return just the filename of the inputted file
# We do this so that we can extract the file name add a suffex to it and save it in the output folder denoted by output
OutputFileName = os.path.join(dir, (args.OutputFilename + 'Output-Verbose.txt'))
OutputFileNamecsv = os.path.join(dir, (args.OutputFilename + 'Output-Dataframe.csv'))
OutputFileNameexcel = os.path.join(dir, (args.OutputFilename + 'Output-Dataframe.xlsx'))

#open outputfile:
outputfile = open(OutputFileName,'a+') #open and append (or create if it doesn't exist) the output file for writing
start_time = datetime.datetime.now().time().strftime('%H:%M:%S') # Start time
print("Process started at " + start_time)
outputfile.write("Process started at: " + str(start_time) + "\n") #print timestamp

P_channels = args.ProcessChannels
Type = args.Type
if args.Verbose == 1:
    print(str(Path))
    print('Output Filename: ' + OutputFileNamecsv)
    #print('Number of Channels to be Processed: '  + str(args.ProcessChannels))

#----------------------------------------------------------------------------------------------------------------------#


#----------------------------------------------------------------------------------------------------------------------#
File_Names = ReadDirectory.OutputFileNames_Dironly(Path,Type) #Returns a series of file paths to use

if args.Verbose == 1:
    print("Files to be Processed:")
    for f in File_Names:
     print(f)

#Read Data - Reads in processed csvs - outputted from LabchartProcessDropout-Multi/sing
Data = pd.concat((pd.read_csv(f) for f in File_Names))#Reads all csvs and adds them to a single data frame
Data = Data.reset_index(drop=True)  # Reset index values of dataframe resets the dataframe index
if args.Verbose == 1:
    print(Data) #Print DataFrame


#Create Output DataFrame Info
OutputCols = ["Channel Name", "Total Elements", "Total Dropout Elements", "Record Time [Sec]", "Dropout Time [Sec]", "% Dropout", "Sample Rate [Sec]","Start Date","End Date"]
OutputDataFrame = pd.DataFrame(columns=OutputCols)
#----------------------------------------------------------------------------------------------------------------------#




#----------------------------------------------------------------------------------------------------------------------#
#Clean up dataframe and add up the multirows to add them all together
Unique_Channels = Data['Channel Name'].unique() # obtain all unique channel names

for name in Unique_Channels:
        f_data = Data.loc[Data['Channel Name']==name] # Returns all rows of the unique name
        if args.Verbose == 1:
            print(f_data)
        #add up columns that make sense to be added up
        aggregation_functions = {'Total Elements': 'sum', 'Total Dropout Elements': 'sum', 'Record Time [Sec]': 'sum','Dropout Time [Sec]':'sum','% Dropout':'sum','Sample Rate [Sec]':'first','Start Date':'first','End Date':'last' }
        f_data_new = f_data.groupby(f_data['Channel Name']).aggregate(aggregation_functions) # add up data and place in new data frame
        f_data_new = f_data_new.reset_index() #Reset index values of dataframe
        # Recalculate % Dropout of overall items as times are now added up
        New_Drop = (f_data_new['Total Dropout Elements']/f_data_new['Total Elements']) * 100
        f_data_new['% Dropout'] = New_Drop
        OutputDataFrame = OutputDataFrame.append(f_data_new) # append data to Outputdata frame
        if args.Verbose ==1:
            print(f_data_new)



print("Combined Processed Data")

#Add additional calculated columns
#Convert Time from seconds to hrs and days and add columns
OutputDataFrame['Record Time [hr]'] = OutputDataFrame['Record Time [Sec]'] / 3600
OutputDataFrame['Record Time [Day]'] = OutputDataFrame['Record Time [Sec]'] / 86400
OutputDataFrame['Dropout Time [hr]'] = OutputDataFrame['Dropout Time [Sec]'] / 3600
OutputDataFrame['Dropout Time [Day]'] = OutputDataFrame['Dropout Time [Sec]'] / 86400

## reset index for data
OutputDataFrame = OutputDataFrame.reset_index(drop=True)  # Reset index values of dataframe
print(OutputDataFrame)
#----------------------------------------------------------------------------------------------------------------------#



#----------------------------------------------------------------------------------------------------------------------#
#Stats and writing to output files

#Calculate Max values
#%Drop Max
Max_Drop = OutputDataFrame.loc[OutputDataFrame["% Dropout"].idxmax(), "% Dropout"]
Max_Drop = f"{Max_Drop:.2f}" # formatting for Print
Channel_Name_Max= OutputDataFrame.loc[OutputDataFrame["% Dropout"].idxmax(), "Channel Name"]
#Dropout time max
Channel_Name_Drop= OutputDataFrame.loc[OutputDataFrame["Dropout Time [Sec]"].idxmax(), "Channel Name"]
MaxDrop_Time_Sec = OutputDataFrame.loc[OutputDataFrame["Dropout Time [Sec]"].idxmax(), "Dropout Time [Sec]"]
MaxDrop_Time_Hr = float(MaxDrop_Time_Sec)/3600
MaxDrop_Time_Day = float(MaxDrop_Time_Sec)/86400

#Formating for Print
MaxDrop_Time_Sec = f"{MaxDrop_Time_Sec:.2f}" # formatting for Print
MaxDrop_Time_Hr = f"{MaxDrop_Time_Hr:.2f}" # formatting for Print
MaxDrop_Time_Day = f"{MaxDrop_Time_Day:.2f}" # formatting for Print

#Calculate Min Values
#%Min Drop out
Channel_Name_Min =OutputDataFrame.loc[OutputDataFrame["% Dropout"].idxmin(), "Channel Name"]
Min_Drop = OutputDataFrame.loc[OutputDataFrame["% Dropout"].idxmin(), "% Dropout"]
Min_Drop = f"{Min_Drop:.2f}" # formatting for Print

#Min Dropout time
Channel_Name_Drop_Min =OutputDataFrame.loc[OutputDataFrame["% Dropout"].idxmin(), "Channel Name"]
MinDrop_Time_Sec = OutputDataFrame.loc[OutputDataFrame["Dropout Time [Sec]"].idxmin(), "Dropout Time [Sec]"]
MinDrop_Time_Hr = float(MinDrop_Time_Sec)/3600
MinDrop_Time_Day = float(MinDrop_Time_Sec)/86400
#Formating for print
MinDrop_Time_Sec = f"{MinDrop_Time_Sec:.2f}" # formatting for Print
MinDrop_Time_Hr =  f"{MinDrop_Time_Hr:.2f}" # formatting for Print
MinDrop_Time_Day= f"{MinDrop_Time_Day:.2f}" # formatting for Print


print("Number of Channels Calculated: " + str(len(OutputDataFrame.index)))
print("Channel with highest dropout %: " + str(Channel_Name_Max) + " at " + str(Max_Drop) + "%")
print("Channel with longest dropout time: " + str(Channel_Name_Drop) + " at " + str(MaxDrop_Time_Sec) + " seconds or " + str(MaxDrop_Time_Hr) +"hr or " + str(MaxDrop_Time_Day) + " Days")
print("Channel with lowest dropout %: " + str(Channel_Name_Min) + " at " + str(Min_Drop) + "%")
print("Channel with shortest dropout time: " + str(Channel_Name_Drop_Min) + " at "+ str(MinDrop_Time_Sec) + " seconds or " + str(MinDrop_Time_Hr) +"hr or " + str(MinDrop_Time_Day) + " Days")
print("Final Dataframe is located in : " + str(OutputFileNamecsv))

#Write to file
outputfile.write("Number of Channels Calculated: " + str(len(OutputDataFrame.index))+'\n')
outputfile.write("Channel with highest dropout %: " + str(Channel_Name_Max) + " at " + str(Max_Drop) + "%"+'\n')
outputfile.write("Channel with longest dropout time: " + str(Channel_Name_Drop) + " at "+ str(MaxDrop_Time_Sec) + " seconds or " + str(MaxDrop_Time_Hr) +"hr or " + str(MaxDrop_Time_Day) + " Days" + '\n')
outputfile.write("Channel with lowest dropout %: " + str(Channel_Name_Min) + " at " + str(Min_Drop) + "%"+'\n')
outputfile.write("Channel with shortest dropout time: " + str(Channel_Name_Drop_Min) + " at "+ str(MinDrop_Time_Sec) + " seconds or " + str(MinDrop_Time_Hr) +"hr or " + str(MinDrop_Time_Day) + " Days" + '\n')
outputfile.write("Final Dataframe is located in : " + str(OutputFileNamecsv)+"\n")
#----------------------------------------------------------------------------------------------------------------------#


#----------------------------------------------------------------------------------------------------------------------#
#Write finished Dataframe to csv and excel
#OutputdataFrame
OutputDataFrame.to_csv(OutputFileNamecsv,index=False) # Outputs datframe.

#Save to Excel
#Data.to_excel(OutputFileNameexcel,sheet_name='Worksheet1', index = False)
#----------------------------------------------------------------------------------------------------------------------#



#----------------------------------------------------------------------------------------------------------------------#
#Close Txt File and wrap up
end_time = datetime.datetime.now().time().strftime('%H:%M:%S')
print("Process ended at " + end_time)
total_time = (datetime.datetime.strptime(end_time,'%H:%M:%S') - datetime.datetime.strptime(start_time,'%H:%M:%S'))
outputfile.write("Process ended at: " + str(end_time) + "\n") #print timestamp
outputfile.write("Total process time: "+ str(total_time) + "\n\n\n") # print total processing time
print("Total process time: " + str(total_time))
outputfile.close() #close output file
#----------------------------------------------------------------------------------------------------------------------#