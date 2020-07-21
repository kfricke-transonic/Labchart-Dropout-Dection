## Search directory for File Type

## This script (Function) will take a path and list all files of specific type in the folder
## Output will return  series of file paths

import os
#Returns all Files of set type in all directorys - root down
def OutputFileNames(File_Path,Type):
    File_List=[]
    for r, d, f in os.walk(File_Path):
        for file in f:
            if  Type in file:
                File_List.append(os.path.join(r, file))
    return File_List


#Returns all Files of set type in File Path only
def OutputFileNames_Dironly(File_Path,Type):
    File_List=[]
    for file in os.listdir(File_Path):
        if file.endswith(Type):
            File_List.append(os.path.join(File_Path, file))
    return File_List


