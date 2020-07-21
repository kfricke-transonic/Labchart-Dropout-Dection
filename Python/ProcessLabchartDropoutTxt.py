#This file will contain all main functions used to process labchart dropout text files.
#Currently, these are implemented in SingleFileLabchartProcess.py and MultiFileLabchartProcess.py
#Will use those files as a basis for these functions
#those file are to remain intact until a functions based script is completed and tested


#Imports
import pandas as pd






#---------------------------------------------------------------------------------------------------------------------#
#Function Read_Header - Confirmed to work
#This function will read in the header info from a labchart txt file, save the header to the verbose text file output
#Will also extract the Column names and pass that back to be used outside the function

#Inputs:
    #Input_FileName = Filename of header to read
    #Output_FileName = Filename for verbose output
    #P_channels = Number of columns of data to process - used for extracting column info
    #Debug = If 1 then Debug info is shown

#Outputs:
    #Interval_float = Interval for data
    #Column names = used for main data prcessing


#Function Call from outside script
def read_header(Input_FileName,Output_FileName,P_channels,Debug):
    outputfile = open(Output_FileName, 'a+')  # open and append (or create if it doesn't exist) the output file for writing

    # -------------------------------------------------------------------------------------------#
    # Labchart HEADER INFO
    #
    """
    #Header Info Format Example
    #Line 1 -- Interval=  0.0125s
    #Line 2 -- ExcelDateTime=    4.3844413444298669e+004  1/14/2020 9:55:21.587405
    #Line 3 -- TimeFormat=                     TimeOfDay                      None
    #Line 4 -- DateFormat=                      M/d/yyyy                      None
    #Line 5 -- ChannelTitle=   4M - S1- B969 Base 1 Com7                Channel 18
    #Line 6 -- Range=                         53.49 mmHg                    1.10 V
    
    #Additional Notes:
    #Channel Title will only provide exported channel names and not date column provided on export
    #Range will just provide total range of signal max - min
    #Only Channel Title will be useful in this script but others are saved for future use
    """



    # Read in Header info from first 6 rows
    #  Channels: In example below it is set to 4 or n+1 channels = 3 channels = 3
    Header_Info = pd.read_csv(Input_FileName, nrows=6, header=None, sep='\t', engine='python', names=range(P_channels + 1), index_col=0)
    if Debug == 1:
        print(Header_Info)


    # -----------------------------------------------------------------------------------------#
    # Extract Sample Rate Interval and write to file
    # print(Header_Info[Header_Info.index.str.startswith('Interval')])# prints out row with info
    Interval = Header_Info.loc['Interval=', 1]  # Interval string format will look like 0.0125 s
    Interval = Interval.split(' ')  # split string to seperate out number
    Interval_float = float(Interval[0])  # cast number string to float
    print('Sample Rate Interval Value =', Interval_float, "sec or ", 1 / Interval_float, "Hz")
    outputfile.write('Sample Rate Interval Value = ' + str(Interval_float) + " sec or " + str(1 / Interval_float) + "Hz" + "\n")
    outputfile.close()  # Close outputfile
    # -----------------------------------------------------------------------------------------#





    # -------------------------------------------------------------------------------------------#
    # Create Column Names for Main Data and output dataframe
    Col_Names = ["Time-Sec", "Date"]  # Start the first two columns with Time and Date
    # Note: if data from labchart is extracted with Comments an additional column will have to be appended with Comments

    for x in range(len(Header_Info.columns)):  # Extract Column names from header info
        Col_Names.append(Header_Info.loc['ChannelTitle=', (x + 1)])  # add column names to the series
    # print(x) # This will print the channel name Debug only
    if Debug == 1:
        print("Column Names:")
        print(Col_Names) # Debug only
    # -------------------------------------------------------------------------------------------#


    return Interval_float, Col_Names


#----------------------------------------------------------------------------------------------------------#
#Read in Main Data Frame - Works
#This function will take an file to read in multi-col data, will skip over  the header and read the data into a
#pandas Data frame
#Will also remove any extra Header info inside of the data put in from Labchart
#Inputs:
    #InputFileName
    #Col_Names
    #ChunkSize =  100k or 1mill to reduce ram usage for big files
#Returns:
    #Data = Panadas Dataframe with all data to be processed

#Function Call from outside script
def read_data(Input_FileName,Col_Names,ChunkSize, Debug):

    #Read Data from Txt Files
    #when having dtype in reading.. Multi-Headers must be removed.
    Data = pd.DataFrame()

    #original line call when no chunk size is used
    #Data = pd.read_csv(FileName,header = None,  skiprows = 6, sep = '\t', engine='python', names =Col_Names, dtype = {Col_Names[2] : np.int8, Col_Names[3] : np.int8, Col_Names[4] : np.int8, Col_Names[0] : np.float16},usecols = [2,3,4])

    #Need to read in data in chunks to keep RAM spikes down
    for chunk in pd.read_csv(Input_FileName, header=None, skiprows=6, sep='\t', engine='python', names=Col_Names, chunksize=ChunkSize):
        Data = pd.concat([Data, chunk], ignore_index=True)
    #print(Data) #print data table if needed for Debug


    #debug data types and sizes
    if Debug == 1:
        print('Initial types:')
        print(Data.dtypes)
        print("")
        print('Data Size')
        print(Data.memory_usage())


    ##Search and Remove MultiHeader if necessary
    #Find all values in table that are not part of the inserted header and remove
    Data = Data.loc[(Data['Time-Sec'] != 'Interval=') & (Data['Time-Sec'] != 'ExcelDateTime=') & (Data['Time-Sec'] != 'TimeFormat=') & (Data['Time-Sec'] != 'DateFormat=') & (Data['Time-Sec'] != 'ChannelTitle=') & (Data['Time-Sec'] != 'Range=')]
    Data = Data.reset_index(drop=True)  # Reset index values of dataframe


    #Adjust data types as there may have been headers in the original data thus was imported as objects
    for Ch_Name in Col_Names[2:]:
       #Data[Ch_Name] = pd.to_numeric(Data[Ch_Name],downcast='float')  # Make all data columns numeric. Some may be objects after header removal
       Data[[Ch_Name]] = Data[[Ch_Name]].astype('float16')  # Make all data columns numeric. Some may be objects after header removal
       #Data[[Ch_Name]] = Data[[Ch_Name]].astype('int8')  # Make int8 if ram space is issue
    if Debug == 1:
        print(Data.dtypes)# Print data Type - Debug Only

    return Data






#-----------------------------------------------------------------------------------------#
# Calculate Dropout Data Rate and Write Data to Files
# calc_data_drop
# Inputs:
    #Data - Pandas Dataframe
    #OutputFileName - Verbose txt file output
    #OutputFileNamecsv - Dataframe csv output
    #Col_Names - Column names for iteration
    #Interval_float
    #Debug

#Outputs:
    #OutputDataFrame - Panadas Dataframe that could be used for future procesing
#-----------------------------------------------------------------------------------------#


def calc_data_drop(Data,Output_FileName,OutputDataFrameFileName, Col_Names, Interval_float,Debug):

    #-------------------------------------------------------------------------------------------#
    #Create Output DataFrame Info
    OutputCols = ["Channel Name", "Total Elements", "Total Dropout Elements", "Record Time [Sec]", "Dropout Time [Sec]", "% Dropout", "Sample Rate [Sec]","Start Date","End Date"]
    OutputDataFrame = pd.DataFrame(columns=OutputCols)
    #-------------------------------------------------------------------------------------------#

    #Setup Output file and Open for write
    outputfile = open(Output_FileName, 'a+')  # open and append (or create if it doesn't exist) the output file for writing

    #Find start date and End date. We will write these to the file/finished data frame
    start_date = Data.loc[(Data.index[0]),Col_Names[1]] # Extract Start date - Date of first value
    end_date = Data.loc[(Data.index[-1]), Col_Names[1]]  # Extract End date - Date of last value


    for Ch_Name in Col_Names[2:]: #Iterate through column list starting at Data Columns - Fist two columns are Time and Date
        #Ch_Name will be the Column name



        #Extract Data when values are 1
        f_data = Data.loc[Data[Ch_Name] == 1] # check specific col for high value
        f_data = f_data.reset_index(drop=True) #Reset index values of dataframe
        #print(len(f_data.index)) # print length of new table - Debug only
        #print(f_data)# extract all rows that are of value 1 - Debug only

        #calculate dropout rates
        Total_Elements = Data.index[-1] + 1
        Total_Filtered = f_data.index[-1] + 1
        Percent_Dropout = (Total_Filtered/Total_Elements)*100


        print("")
        print("Channel Name: = ", Ch_Name)
        print("Total number of items in initial Frame = ", Total_Elements)
        print("Total Record Time", Total_Elements*Interval_float, "sec or ", (Total_Elements*Interval_float)/3600, "hrs or ",(Total_Elements*Interval_float)/86400, "Days")
        print("Number of Filtered elements = ", Total_Filtered)
        print("Total Dropout Time", Total_Filtered*Interval_float, "sec or ", (Total_Filtered*Interval_float)/3600, "hrs or ",(Total_Filtered*Interval_float)/86400, "Days")
        print("% Dropouts = ", Percent_Dropout,"%")
        print("Start Date = ", start_date)
        print("End Date = ", end_date)

        #Write to dataframe
        OutputDataFrame = OutputDataFrame.append({'Channel Name': Ch_Name, 'Total Elements': Total_Elements, 'Total Dropout Elements': Total_Filtered, 'Record Time [Sec]' : Total_Elements*Interval_float, 'Dropout Time [Sec]':Total_Filtered*Interval_float, '% Dropout': Percent_Dropout, 'Sample Rate [Sec]':Interval_float, 'Start Date':start_date,'End Date':end_date},ignore_index=True)



        #write to file # for text related outputs
        outputfile.write("\n")
        outputfile.write("Channel Name: = " + str(Ch_Name) + "\n")
        outputfile.write("Total number of items in initial Frame = " + str(Total_Elements) + "\n")
        outputfile.write("Total Record Time " + str(Total_Elements*Interval_float) + "sec or "+  str((Total_Elements*Interval_float)/3600) + "hrs or "+ str((Total_Elements*Interval_float)/86400) + "Days \n")
        outputfile.write("Number of Filtered elements = " + str(Total_Filtered) + "\n")
        outputfile.write("Total Dropout Time " + str(Total_Filtered * Interval_float) + "sec or " + str((Total_Filtered * Interval_float) / 3600) + "hrs or " + str((Total_Filtered * Interval_float) / 86400) + "Days \n")
        outputfile.write("% Dropouts = " + str(Percent_Dropout) + "% \n")
        outputfile.write("Start Date = " + str(start_date) + "% \n")
        outputfile.write("End Date = " + str(end_date) + "% \n")

    #----------------------------------------------------------------------------------------------------------------------#

    #
    #Output Processed DataFrame to file for future processing
    #print("OUTPUT DATA FRAME") #Debug
    #print(OutputDataFrame) #Debug

    #OutputDataFrame.to_csv(OutputDataFrameFileName, sep='\t') # Outputs dataframe. May remove index value if not needed
    OutputDataFrame.to_csv(OutputDataFrameFileName,index=False) # Outputs dataframe. May remove index value if not needed
    if Debug == 1:
        print(OutputDataFrame)

    #Close Txt File
    outputfile.close() #close output file
    return OutputDataFrame

