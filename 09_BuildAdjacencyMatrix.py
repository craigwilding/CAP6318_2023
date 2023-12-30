#####################################################################
# BUILD NODES AND EDGES - CoAuthors
# For each author, create a node and add its attributes
# For each author-coauthor in PubAuthors, add an edge and set the weight.
#####################################################################
import os
import os.path
import shutil
import datetime
import MyEnvVars as MyEnv
import AuthTools as AUTHOR
import pandas as pd
import numpy as np
from csv import DictReader
from csv import DictWriter

#################################
# PATHS
#################################
# Local directory for where files are stored
wrksp = MyEnv.workspace # where to run from
dirData = os.path.join(wrksp, "Data")
dirOut = os.path.join(wrksp, "Gephi", "GraphTourMgmt5Years")
dirGraph = dirOut
Downloads = MyEnv.downloads 

os.chdir(wrksp)

print(os.getcwd())
if (not os.path.exists(dirOut)) :
    os.mkdir(dirOut)

TAB = "\t"
EOL = '\n'

#################################
# DATA STRUCTURES
#################################
topic = MyEnv.TOPIC
today = datetime.date.today()
CURRENT_YEAR = today.year  # get current year
MIN_YEAR = CURRENT_YEAR - MyEnv.YEARS_PRIOR

##########################################
# Read edge list
##########################################
nodeCount = 1996
adjMatrix = np.zeros((nodeCount, nodeCount))
fieldsEdges = ['Source','Target', 'weight']
fileNameEdgeList = os.path.join(dirGraph, "GS_TourMgt5_Edges" + ".csv")
with open(fileNameEdgeList, 'r', encoding='utf-8') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:

        source = int(row["Source"]) -1
        target = int(row["Target"]) -1
        adjMatrix[source, target] = 1
        adjMatrix[target, source] = 1
    # end for
# end read edge list

# convert array into dataframe
df = pd.DataFrame(adjMatrix)
 
# save the dataframe as a csv file
fileNameAdjMatrix = os.path.join(dirGraph, "adjMatrix" + ".csv")
df.to_csv(fileNameAdjMatrix)