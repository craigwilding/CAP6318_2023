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
import PandasTransforms as PTX
from csv import DictReader
from csv import DictWriter

#################################
# PATHS
#################################
# Local directory for where files are stored
wrksp = MyEnv.workspace # where to run from
dirData = os.path.join(wrksp, "Data")
dirOut = os.path.join(wrksp, "Gephi", MyEnv.GRAPH_FOLDER)
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
authors = AUTHOR.AuthorTools()
dictAuthNodeIds = {}

setAuthIds = set()


def selectEdgeWeight(dictEdgePairs, weightCol) :
    dictEdgeWeights = []
    for pair in dictEdgePairs :
        # print(pair)
        rowEdgePair = dictEdgePairs[pair]
                
        rowEdgeWeight = {}
        rowEdgeWeight["Source"] =  rowEdgePair["Source"]
        rowEdgeWeight["Target"] =    rowEdgePair["Target"]
        if (weightCol == "none") :
            rowEdgeWeight["weight"] = 1
        else :
            rowEdgeWeight["weight"] =   rowEdgePair[weightCol]
        dictEdgeWeights.append(rowEdgeWeight)
        
    return dictEdgeWeights

def addEdgePairs(Source, Target, dictEdgePairs, dictWeights) :
    pair = (Source, Target)
    rev_pair = (Target,Source)
    if rev_pair in edgePairs :
        skip = True
        #print("reverse coauthor found: " + str(Target) + "," + str(Source))
    elif pair in edgePairs :
        # edge exist already, add weight values
        rowEdgePair = dictEdgePairs[pair]
        orderWeight = dictWeights['orderWeight']
        rowEdgePair["orderWeight"] = orderWeight + float(rowEdgePair["orderWeight"])
        citations = dictWeights['citations']
        rowEdgePair["citations"] = citations + int(rowEdgePair["citations"])
        year = dictWeights['year']
        yearPrev = int(rowEdgePair["year"])
        rowEdgePair["year"] = max(year, yearPrev)
        rowEdgePair["count"] = rowEdgePair["count"] + 1
        dictEdgePairs[pair] = rowEdgePair
    else :
        rowEdgePair = {}
        rowEdgePair["Source"] = Source
        rowEdgePair["Target"] = Target
        orderWeight = dictWeights['orderWeight']
        rowEdgePair["orderWeight"] = orderWeight
        citations = dictWeights['citations']
        rowEdgePair["citations"] = citations
        year = dictWeights['year']
        rowEdgePair["year"] = year
        rowEdgePair["count"] = 1
        dictEdgePairs[pair] = rowEdgePair
        edgePairs.append(pair)
        # Create set of authids that have a node.
        setAuthIds.add(Source)
        setAuthIds.add(Target)
            
    # end edgePairs

##########################################
# Set Author data
##########################################
#fileNameAuthFilter = os.path.join(dirData, "GS_Tourism_Authors_500_corrected+countries_6"  + ".csv")
#pdAuth500 = PTX.PandasTransform(fileNameAuthFilter, delim=PTX.COMMA, headers=True)
fileNameAuth = os.path.join(dirData, "GS_" + topic + "_Authors_" + str(MyEnv.N_AUTHORS) + ".csv")
pdAuthors = PTX.PandasTransform(fileNameAuth, delim=PTX.COMMA, headers=True)
pdAuthors.addColumn("country","")
pdAuthors.removeColumn("link")
pdAuthors.removeColumn("authorKey")

# reset column order
#dfAuthors = pdAuthors.df[["id", "name", "lastname", "university", "country","pub_2023", "pub_cit_2023", "tot_citations","curYear_citations","prevYear_citations", "otherTopics"]]
#print(dfAuthors.head())

if ("ALL" == MyEnv.TOPIC) : 
    multiTopic = MyEnv.TOPIC_ALL
    multiTopic = multiTopic.replace(" OR ","")
    topicList = multiTopic.split("label:")
    for mytopic in topicList :
        # Mark if the topic was listed in other topics.
        findtopic = mytopic.replace('_', ' ').strip()
        if (len(findtopic) > 0) :
            pdAuthors.addColumn(mytopic, 0)
            pdAuthors.df.loc[pdAuthors.df["otherTopics"].str.contains(findtopic, case=False) == True, mytopic] = 1
        
pdAuthors.removeColumn("otherTopics")


# write to AuthorData file
fileNameAuthData = os.path.join(dirData, "GS_" + topic + "_AuthorData_" + str(MyEnv.N_AUTHORS) + ".csv")
pdAuthors.df.to_csv(fileNameAuthData, encoding='utf-8', mode='w', header=True, index=False)
print("Write File: ", fileNameAuthData)

############################################
# Add CoAuthor Nodes
############################################
fileNameCoAuth = os.path.join(dirData, "GS_" + topic + "_CoAuth_" + str(MyEnv.N_AUTHORS) + ".csv")
fileNameCombAuth = os.path.join(dirData, "GS_" + topic + "_CombAuth_" + str(MyEnv.N_AUTHORS) + ".csv")
if (True == MyEnv.INCLUDE_NONTOPIC_COAUTH) : 
    csv_files = [fileNameAuthData, fileNameCoAuth]
    df_csv_append = pd.DataFrame()
    # append the CSV files
    for file in csv_files:
        df = pd.read_csv(file)
        df_csv_append = df_csv_append.append(df, ignore_index=True)

    df_csv_append.to_csv(fileNameCombAuth)
# end if

##########################################
# CREATE NODES from AUTHORS
##########################################
# build a list nodes from authors selected
id = 0
tableNodes = []
fieldsNodes = ['Id','Label', 'authid', "TopRank", 'University', 'Country', 'pub_2023', 'pub_cit_2023', 'tot_citations', 'curYear_citations', 'prevYear_citations']

fileNameAuthIn = fileNameAuthData
if (True == MyEnv.INCLUDE_NONTOPIC_COAUTH) : 
    fileNameAuthIn = fileNameCombAuth

with open(fileNameAuthIn, 'r', encoding='utf-8') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        # convert author data to Node data
        rowNode = {}
        id += 1
        #rowNode["Id"] = id
        
        authid = int(row["id"])
        rowNode["Id"] = authid  # Can I use authid as ID
        dictAuthNodeIds[authid] = id  # Save Node Id of authid
        rowNode["Label"] = row["name"]    # authors.getName(authid)
        rowNode["authid"] = authid
        if (id <= 10) :
            rowNode["TopRank"] = 5
        elif (id <= 20) :
            rowNode["TopRank"] = 4
        elif (id <= 50) :
            rowNode["TopRank"] = 3
        elif (id <= 100) :
            rowNode["TopRank"] = 2
        else :
            rowNode["TopRank"] = 1
        rowNode["University"] = row["university"]
        rowNode["Country"] = row["country"]
        rowNode["pub_2023"] = int(row["pub_2023"])
        rowNode["pub_cit_2023"] = int(row["pub_cit_2023"])
        rowNode["tot_citations"] = int(row["tot_citations"])
        rowNode["curYear_citations"] = int(row["curYear_citations"])
        rowNode["prevYear_citations"] = int(row["prevYear_citations"])

        if ("ALL" == MyEnv.TOPIC) : 
            multiTopic = MyEnv.TOPIC_ALL
            multiTopic = multiTopic.replace(" OR ","")
            topicList = multiTopic.split("label:")
            
            for mytopic in topicList :
                # Mark if the topic was listed in other topics.
                findtopic = mytopic.replace('_', ' ').strip()
                if (len(findtopic) > 0) :
                    if (mytopic not in fieldsNodes):
                        fieldsNodes.append(mytopic) # Add column name
                    # add column value
                    hasTopic = int(row[mytopic])
                    rowNode[mytopic] = hasTopic

        tableNodes.append(rowNode)
    # end for rows
# end with authors



##########################################
# WRITE node list
##########################################
#fieldsNodes = ['Id','Label', 'authid', "TopRank", 'University', 'Country', 'pub_2023', 'pub_cit_2023', 'tot_citations', 'curYear_citations', 'prevYear_citations']
fileNameNodes = os.path.join(dirGraph, "GS_" + topic + "_Nodes" + ".csv")
with open(fileNameNodes, 'w', newline='', encoding='utf-8') as write_csv:
    csvwriter = DictWriter(write_csv, fieldnames = fieldsNodes)
    csvwriter.writeheader()
    csvwriter.writerows(tableNodes)
    print("WRITE file: " + fileNameNodes)
# end with csv



##########################################
# Use PubAuthors to build edge list
##########################################
# read pubAuthors file
fileNamePubAuthors = os.path.join(dirData, "GS_" + topic + "_PubAuthors_" + str(MyEnv.N_AUTHORS) + ".csv")
pdPubAuth = PTX.PandasTransform(fileNamePubAuthors, delim=PTX.COMMA, headers=True)

# Filter to valid rows
print(pdPubAuth.df.head())
pdPubAuth.removeRows("coauthid", 0) # Only rows where coauthors are also in author list
print(pdPubAuth.df.head())

listValidStatus = ["COAUTH"]  # Valid Coauthors
pdPubAuth.df = pdPubAuth.selectRows("status", listValidStatus)
print(pdPubAuth.df.head())

#listValidAuthors = authors.listAuthFilter  # filter to top 500
#pdPubAuth.df = pdPubAuth.selectRows("authid", listValidAuthors)

#fileNamePubAuthFiltered = os.path.join(dirData, "GS_" + topic + "_PubAuthFiltered_" + str(MyEnv.N_AUTHORS) + ".csv")
#pdPubAuth.df.to_csv(fileNamePubAuthFiltered, encoding='utf-8', index=False, sep=',', header=True)


##########################################
# Convert pubAuth to edge rows
##########################################
rowsOutEdgeData = []
edgePairs = []
dictEdgePairs = {}
pubidPrev = 0
listPubCoauthors = []
dfPubRows = pdPubAuth.df
for pubRow, rowVal in dfPubRows.iterrows():
    # get pubid for publication
    authid = int(dfPubRows.loc[pubRow,'authid'])
    pubid = int(dfPubRows.loc[pubRow,'pubid'])
    coauthid = int(dfPubRows.loc[pubRow,'coauthid'])
    dictWeights = {}
    
    orderWeight = float(dfPubRows.loc[pubRow,'orderWeight'])
    dictWeights['orderWeight'] = orderWeight
    citations =  0 #int(row["citations"])
    dictWeights['citations'] = citations
    year = int(dfPubRows.loc[pubRow,'year'])
    dictWeights['year'] = year
    #Source = int(dictAuthNodeIds[authid])
    #Target = int(dictAuthNodeIds[coauthid])
    Source = authid
    Target = coauthid
        
    addEdgePairs(Source, Target, dictEdgePairs, dictWeights)
            
    # end edgePairsr

    ####
    # check for co-authors on same publication
    if (pubidPrev == pubid) :
        for coauthSource in listPubCoauthors :
            addEdgePairs(coauthSource, Target, dictEdgePairs, dictWeights)

        listPubCoauthors.append(Target)
    else :
        pubidPrev = pubid
        listPubCoauthors.clear()
        listPubCoauthors.append(Target)
        
# end for rows



##########################################
# WRITE edge list
##########################################
fieldsEdges = ['Source','Target', 'weight']
fileNameEdgeList = os.path.join(dirGraph, "GS_" + topic + "_Edges" + ".csv")
with open(fileNameEdgeList, 'w', newline='') as write_csv:
    csvwriter = DictWriter(write_csv, fieldnames = fieldsEdges)
    csvwriter.writeheader()
    rowsOutEdges = selectEdgeWeight(dictEdgePairs, "none")
    csvwriter.writerows(rowsOutEdges)
    print("WRITE file: " + fileNameEdgeList)
# end with csv

fileNameEdgeList = os.path.join(dirGraph, "GS_" + topic + "_Edges_" + "order" + ".csv")
with open(fileNameEdgeList, 'w', newline='') as write_csv:
    csvwriter = DictWriter(write_csv, fieldnames = fieldsEdges)
    csvwriter.writeheader()
    rowsOutEdges = selectEdgeWeight(dictEdgePairs, "orderWeight")
    csvwriter.writerows(rowsOutEdges)
    print("WRITE file: " + fileNameEdgeList)
# end with csv



fileNameEdgeListYear = os.path.join(dirGraph, "GS_" + topic + "_Edges_" + "year" + ".csv")
with open(fileNameEdgeListYear, 'w', newline='') as write_csv:
    csvwriter = DictWriter(write_csv, fieldnames = fieldsEdges)
    csvwriter.writeheader()
    rowsOutEdges = selectEdgeWeight(dictEdgePairs, "year")
    csvwriter.writerows(rowsOutEdges)
    print("WRITE file: " + fileNameEdgeListYear)
# end with csv


fileNameEdgeList = os.path.join(dirGraph, "GS_" + topic + "_Edges_" + "count" + ".csv")
with open(fileNameEdgeList, 'w', newline='') as write_csv:
    csvwriter = DictWriter(write_csv, fieldnames = fieldsEdges)
    csvwriter.writeheader()
    rowsOutEdges = selectEdgeWeight(dictEdgePairs, "count")
    csvwriter.writerows(rowsOutEdges)
    print("WRITE file: " + fileNameEdgeList)
# end with csv

##########################################
# WRITE node list for Only authors that have nodes.
##########################################
# copy Nodes of Authors to CoAuthNodes
fileNameNodeswEdges = os.path.join(dirGraph, "GS_" + topic + "_Nodes_wEdges" + ".csv")
shutil.copy(fileNameNodes, fileNameNodeswEdges)
pdCoAuthNodes = PTX.PandasTransform(fileNameNodeswEdges, delim=PTX.COMMA, headers=True)
pdCoAuthNodes.df =  pdCoAuthNodes.selectRows('authid',setAuthIds)
pdCoAuthNodes.write()
print("WRITE file: " + fileNameNodeswEdges)

#fieldsNodes = ['Id','Label', 'authid', "TopRank", 'University', 'Country', 'pub_2023', 'pub_cit_2023', 'tot_citations', 'curYear_citations', 'prevYear_citations']

if (MyEnv.YEARS_PRIOR > 0) :
    fileNameNodesbyYear = os.path.join(dirGraph, "GS_" + topic + "_Nodes_byYear" + ".csv")
    shutil.copy(fileNameNodeswEdges, fileNameNodesbyYear)
    pdEdgeYears = PTX.PandasTransform(fileNameEdgeListYear, delim=PTX.COMMA, headers=True)
    pdNodes = PTX.PandasTransform(fileNameNodesbyYear, delim=PTX.COMMA, headers=True)

    for year in range(MIN_YEAR, CURRENT_YEAR) :
        colYear = "deg_" + str(year)
        pdNodes.addColumn(colYear, 0)

    #MIN_YEAR = CURRENT_YEAR 
    dfNodes = pdNodes.df
    for row, rowVal in dfNodes.iterrows():
        # get pubid for publication
        Source = int(dfNodes.loc[row,'Id'])

        for year in range(MIN_YEAR, CURRENT_YEAR) :
            colYear = "deg_" + str(year)
            search = (pdEdgeYears.df['Source'] == Source) & (pdEdgeYears.df['weight'] <= year)
            dfMatch = pdEdgeYears.df[search ]
            YearCount = dfMatch.shape[0]
            search = (pdEdgeYears.df['Target'] == Source) & (pdEdgeYears.df['weight'] <= year)
            dfMatch = pdEdgeYears.df[search ]
            YearCount = dfMatch.shape[0] + YearCount
            updateMatch = (pdNodes.df['Id'] == Source)
            pdNodes.df.loc[updateMatch, colYear] = YearCount
        # end for each year
    # end for each node.
    pdNodes.write()
# end edge years
