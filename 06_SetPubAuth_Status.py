#############################################
# Set Pub Author Status
# Read the list of publications and authors
# Determine Status:
#	ONLY : CoAuthor is Original Author and NO other co-authors
#	DUP  : Same Title, different pubids.  Take pub id where Author is 1st co-author
#	MAUTH : Publication has 6th authors or more.  Some have large # of authors
#	COAUTH : This is a valid co-author with the original author.  Need Id of Co-author
#############################################

import os
import os.path
import MyEnvVars as MyEnv
from csv import DictReader
from csv import DictWriter
import PandasTransforms as PTX
import AuthTools as AUTHOR

wrksp = MyEnv.workspace # where to run from
dirOut = os.path.join(wrksp, "Data")
Downloads = MyEnv.downloads 

os.chdir(wrksp)

print(os.getcwd())
if (not os.path.exists(dirOut)) :
    os.mkdir(dirOut)

TAB = "\t"
EOL = '\n'

chromed = MyEnv.chromedriver_path
topic = MyEnv.TOPIC
print(MyEnv.TOPIC)

#################################
# DATA STRUCTURES
#################################
authors = AUTHOR.AuthorTools()
fieldsAuthors = ['id','name', 'authorKey', 'lastname', 'university', 'pub_2023', 'pub_cit_2023', 'tot_citations', 'curYear_citations', 'prevYear_citations','otherTopics', 'link']
fieldsPublications = ['authid', 'pubid', 'year', 'pub_cit', 'publink', 'title']
fieldsPubAuthors = ['authid', 'pubid', 'author', 'coauthor', 'coauthOrder', 'orderWeight', 'title', 'year']

fileNameAuth = os.path.join(dirOut, "GS_" + topic + "_Authors_" + str(MyEnv.N_AUTHORS) + ".csv")
fileNamePub = os.path.join(dirOut, "GS_" + topic + "_Publications_" + str(MyEnv.N_AUTHORS) + ".csv")
fileNamePubAuthors = os.path.join(dirOut, "GS_" + topic + "_PubAuthors_" + str(MyEnv.N_AUTHORS) + ".csv")

#################################
# Add Status Column
#################################
pandasTX = PTX.PandasTransform(fileNamePubAuthors, delim=PTX.COMMA, headers=True)
pandasTX.addColumn("status", "COAUTH")
pandasTX.addColumn("coauthid", -1)
pandasTX.df.loc[:,'status'] = 'COAUTH'  # reset status to COAUTH


#################################
# Determine Status:
#	ONLY : CoAuthor is Original Author and NO other co-authors
#	DUP  : Same Title, different pubids.  Take pub id where Author is 1st co-author
#	MAUTH : Coauthor is listed as 6th author or more.  Some have large # of authors
#   NOAUTH : Author not listed as an author or coauthor
#   SELF : Coauthor is the author
#	COAUTH : This is a valid co-author with the original author.  Need Id of Co-author
#############################################

print("pandas READ file " + fileNamePubAuthors)
for rowid, rowPubAuthor in pandasTX.df.iterrows():
        
    authid = rowPubAuthor["authid"]
    pubid = rowPubAuthor["pubid"]
    coauthor = rowPubAuthor["coauthor"]

    df = pandasTX.getDataFrame()
    dfAuthPub = df[(df['authid'] == authid) & (df['pubid'] == pubid) ]
    rowCount = dfAuthPub.shape[0]
    if ( 1 == rowCount) :  # Only 1 row
        df.loc[(df['authid'] == authid) & (df['pubid'] == pubid), "status" ] = "ONLY"
    elif (rowCount > 6) :
        df.loc[(df['authid'] == authid) & (df['pubid'] == pubid), "status" ] = "MAUTH"
    # end else rowCount

    if (authors.isNoAuth(authid, pubid)) :  # Not listed as an author
        df.loc[(df['authid'] == authid) & (df['pubid'] == pubid), "status" ] = "NOAUTH"

    coauthid = authors.findAuthorByName(coauthor)
    df.loc[df['coauthor'] == coauthor, 'coauthid'] = coauthid

    if (coauthid == authid) :  # Not listed as an author
        df.loc[(df['authid'] == authid) & (df['pubid'] == pubid) & (df['coauthid'] == authid), "status" ] = "SELF"
# end for rows


# Set Weights on Multi-author publications
pandasTX.df.loc[(pandasTX.df['status'] == "MAUTH") & (pandasTX.df['coauthOrder'] > 5), "orderWeight" ] = 0.25
pandasTX.df.loc[(pandasTX.df['status'] == "MAUTH") & (pandasTX.df['coauthOrder'] > 10), "orderWeight" ] = 0

print("pandas WRITE file " + fileNamePubAuthors)
pandasTX.write()

#################################
# Check for duplicates
#################################
for rowid, rowPubAuthor in pandasTX.df.iterrows():
        
    df = pandasTX.getDataFrame()
    authid = rowPubAuthor["authid"]
    pubid = rowPubAuthor["pubid"]
    title = rowPubAuthor["title"]
    year = rowPubAuthor["year"]

    # get publications with same title and year
    dfAuthPub = df[(df['title'] == title) & (df['year'] == year) ]
    # See if there are any other pubids not from this author
    dfPubIds = dfAuthPub[(dfAuthPub['pubid'] != pubid)]
    rowCount = dfPubIds.shape[0] 
    if ( rowCount >= 1) : 
        # Duplicate found. 
        # Determine original as lowest coauthOrder with a non-zero coauthid
        minOrder = 999
        minOrderPub = 999
        minAuthor = 99999
        minPubId = 9999999
        keepPubId = 0
        setDupPubIds = set()
        for duprowid, duprowPubAuthor in dfAuthPub.iterrows(): 
            dupauthid = duprowPubAuthor["authid"]
            duppubid = duprowPubAuthor["pubid"]
            coauthid = duprowPubAuthor["coauthid"]
            coauthOrder = duprowPubAuthor["coauthOrder"]
            setDupPubIds.add(duppubid)

            if (coauthOrder <= minOrder) and (coauthid != 0):
                if (coauthid < minAuthor) :
                    minOrder = coauthOrder
                    minAuthor = authid

            if (coauthid == dupauthid) :
                if (coauthOrder < minOrderPub) :
                    keepPubId = duppubid
                    minOrderPub = coauthOrder

            if (duppubid < minPubId) :
                minPubId = duppubid
            #print("dup title:", authid, pubid, dupauthid, duppubid, coauthid, coauthOrder, title)
        # end for rows
        if (0 != keepPubId) :
            setDupPubIds.remove(keepPubId) # remove the pubid we want to keep as original
        else :
            keepPubId = minPubId
            setDupPubIds.remove(keepPubId)
        # mark duplicate pubids
        for dupPubid in setDupPubIds :
            df.loc[(df['pubid'] == dupPubid), "status" ] = "DUP"
            
    # end dups found

# end for find dups

print("pandas WRITE FINAL file " + fileNamePubAuthors)
pandasTX.write()