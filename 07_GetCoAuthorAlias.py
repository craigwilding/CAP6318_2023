import os
import os.path
from csv import DictReader
from csv import DictWriter
import MyEnvVars as MyEnv
import PandasTransforms as PTX


#################################
# DATA STRUCTURES
#################################

class CoAuthTools:
  
  def __init__(self):
    self.listAuthID = []
    self.dictAuthNames = {}
    self.dictAuthLastNames = {}
    self.dictAuthAlias = {}
    self.rowsNoAuthor = []
    self.nextAuthID = 0
    #self.__loadNames()
    #self.dictAuthAlias = self.__loadAuthAlias()
    #self.listNoAuth = self.__loadNoAuth()
    #print('nameCount = ', str(len(self.dictAuthNames)))

  def getNextAuthID(self) :
    self.nextAuthID += 1
    return self.nextAuthID


  def __loadNames(self):
    wrksp = MyEnv.workspace # where to run from
    dirOut = os.path.join(wrksp, "Data")
    os.chdir(wrksp)
    topic = MyEnv.TOPIC
    fieldsAuthors = ['id','name', 'authorKey', 'lastname', 'university', 'pub_2023', 'pub_cit_2023', 'tot_citations', 'curYear_citations', 'prevYear_citations','otherTopics', 'link']
    fileNameAuth = os.path.join(dirOut, "GS_" + topic + "_Authors_" + str(MyEnv.N_AUTHORS) + ".csv")
    self.dictAuthNames = {}  # {authid, name}
    with open(fileNameAuth, 'r', newline='', encoding='utf-8') as read_obj:
        csv_dict_reader = DictReader(read_obj)
        for row in csv_dict_reader:
            authid = int(row["id"])
            if (12 == authid) :
              print("break")
            if (self.inFilter(authid)) :
              name = row["name"]
              name = self.__fixNames(name)
              lastname = self.__parseLastName(name)
              self.listAuthID.append(authid)
              self.dictAuthNames[authid] = name
              self.dictAuthLastNames[authid] = lastname
              # check for multiple names listed in name
              # TODO: Does it have multiple split chars?  Separate into function.
              self.__splitNames(name, authid)
    
    return self.dictAuthNames

  def __splitNames(self, name, authid) :
    # Convert all possible split characters to on common split character
    splitChar = ';'
    nameSplitChars = [',', '/', " or "]
    nameOut = name
    for splitItem in nameSplitChars :
       nameOut = nameOut.replace(splitItem, splitChar)
    bIsSplitName = False

    if (nameOut.find(splitChar) > 1) :
      bIsSplitName = True
      nameList = nameOut.split(splitChar)
      #self.dictAuthNames[authid] = nameList[0]
      for name2 in nameList :
        self.addAuthAlias(name2, authid)
      # end name 2
      
    if (False == bIsSplitName) :
      self.addAuthAlias(nameOut, authid)

  def __loadAuthAlias(self):
    wrksp = MyEnv.workspace # where to run from
    dirOut = os.path.join(wrksp, "Data")
    os.chdir(wrksp)
    topic = MyEnv.TOPIC
    fieldsAuthAlias = ['authname', 'authid']
    fileNameAuthAlias = os.path.join(dirOut, "GS_" + topic + "_AuthAlias_" + str(MyEnv.N_AUTHORS) + ".csv")
    self.dictAuthAlias = {}  # {authid, name}
    with open(fileNameAuthAlias, encoding='utf-8') as read_obj:
        csv_dict_reader = DictReader(read_obj)
        for row in csv_dict_reader:
            authname = str(row["authname"])
            authid = int(row["authid"])
            self.addAuthAlias(authname, authid)
        
        return self.dictAuthAlias
    
  def __loadNoAuth(self):
    wrksp = MyEnv.workspace # where to run from
    dirOut = os.path.join(wrksp, "Data")
    os.chdir(wrksp)
    topic = MyEnv.TOPIC
    fieldsNoAuthor = ['authid','pubid', 'authName', 'coauthList']
    fileNameNoAuthor = os.path.join(dirOut, "GS_" + topic + "_NoAuth_" + str(MyEnv.N_AUTHORS) + ".csv")
    self.listNoAuth = []  # [(authid, pubid)]
    with open(fileNameNoAuthor, encoding='utf-8') as read_obj:
        csv_dict_reader = DictReader(read_obj)
        for row in csv_dict_reader:
            authid = int(row["authid"])
            pubid = int(row["pubid"])
            tpNoAuth = (authid, pubid)
            self.listNoAuth.append(tpNoAuth)
        
        return self.listNoAuth
    
  def getName(self, authid):
    result = ""
    if (authid in self.dictAuthNames) : 
        result = self.dictAuthNames[authid]
    return result
  
  def getLastName(self, authid):
    result = ""
    if (authid in self.dictAuthLastNames) : 
        result = self.dictAuthLastNames[authid]
    return result

  def inFilter(self, authid):
    result = True
    #result = False
    if (authid in self.listAuthID) : 
        result = True
    return result
  
  def isNoAuth(self, authid, pubid):
    result = False
    tpNoAuth = (authid, pubid)
    if (tpNoAuth in self.listNoAuth) : 
        result = True
    return result
  
  def getAuthorIds(self) :
    return self.listAuthID

  def findAuthorByName(self, authName) :
    authid = 0
    if (authName != authName) :
       return 0
    findName = authName.strip()
    if (findName in self.dictAuthAlias) :
      authid = self.dictAuthAlias[findName]
    return authid
    
  def addAuthAlias(self, name, authid) : 
    alias = name.strip()
    if (len(alias) > 0) :
      if (alias not in self.dictAuthAlias) :
        self.dictAuthAlias[alias] = authid
        if (authid not in self.dictAuthNames) :
          self.dictAuthNames[authid] = alias
          lastname = self.__parseLastName(name)
          self.dictAuthLastNames[authid] = lastname
      else :
        dupId = self.dictAuthAlias[alias]
        if (dupId != authid) :
          print("duplicate author name:",alias, authid, dupId)

  def matchAuthorAlias(self, authid, coauth, pubid) :
    coauthid = 0

    if (921 == authid) and (4574 == pubid) :
        print("Test Name", authid, coauth)
    coauthid = self.findAuthorByName(coauth)
    if (0 != coauthid) :
        return coauthid
    else :
        testCoauth = self.__fixNames(coauth)
        testCoauth = self.normalize(testCoauth)
        coauthid = self.findAuthorByName(testCoauth)
        if (0 == coauthid) :
          testCoauth2 = testCoauth.replace('-',' ').replace("‐",' ')
          coauthid = self.findAuthorByName(testCoauth2)
        # end test 2

        if (0 == coauthid) :
          # No alias found, add alias
          coauthid = self.getNextAuthID()
        
        # add Alias - if coauthid was found, it adds alias, else create new name and alias.
        self.addAuthAlias(coauth, coauthid)

    #print("end if")

    return coauthid
  

  def saveNoAuthors(self) :
    wrksp = MyEnv.workspace # where to run from
    dirOut = os.path.join(wrksp, "Data")
    os.chdir(wrksp)
    topic = MyEnv.TOPIC
    fieldsNoAuthor = ['authid','pubid', 'authName', 'coauthList']
    fileNameNoAuthor = os.path.join(dirOut, "GS_" + topic + "_NoAuth_" + str(MyEnv.N_AUTHORS) + ".csv")
    with open(fileNameNoAuthor, 'w', newline='', encoding='utf-8') as write_csv:
        csvwriter = DictWriter(write_csv, fieldnames = fieldsNoAuthor)
        csvwriter.writeheader()
        csvwriter.writerows(self.rowsNoAuthor)
        print("WRITE file: " + fileNameNoAuthor)
    # end with csv
    

  
  def remove_non_ascii_1(self, text):
    return ''.join([i if ord(i) < 128 else '' for i in text])
  
  def normalize(self, text) :
     textOut = str(text)
     textOut = textOut.replace('O’',"").replace("O'","") # irish
     textOut = textOut.replace("ó",'o').replace("á",'a').replace("Ž",'Z').replace('ß', 'ss').replace('Ö','O')
     textOut = textOut.replace("IŞIK", "Işik").replace("şı","si").replace("ş",'S').replace('Ă','a')
     textOut = textOut.lower().strip()
     textOut = textOut.replace("á", 'a').replace('ç','c').replace("í",'i').replace('ž','z')
     textOut = textOut.replace("ó",'o').replace("á",'a').replace("í",'i').replace("é",'e').replace("ğ",'g')
     #textOut = textOut.replace('-',' ')
     #textOut = self.remove_non_ascii_1(textOut)
     return textOut
  
  def __fixNames(self, authName) :
      nameOut = authName
      # Remove titles
      nameOut = nameOut.replace('Ph.D.','').replace('Ph.D','').replace('PhD.','').replace('PhD','')
      nameOut = nameOut.replace('Dr ','').replace('Dr.','').replace('M.Si','')
      nameOut = nameOut.replace("Assoc.",'').replace('Professor', '').replace("Prof.",'').replace("A/Prof",'').replace("Associate",'')
      nameOut = nameOut.replace("Lamers, M", "M Lamers").replace("Tourism Leader", '')
      nameOut = nameOut.replace("Mariné Roig, E.", "E. Mariné Roig")
      nameOut = nameOut.strip()
      # remove links
      badEndData = ["(ORCID", "http", "CPA (", "(0000", "0000", " - Σ", " SE, MM, CMA.", " MM"]
      for badEnd in badEndData :
        ixTest = nameOut.find(badEnd)
        if (ixTest > 1) :
          nameOut = nameOut[0: ixTest]
      nameOut = nameOut.strip()
      # remove (alias) at end
      if (nameOut.endswith(')')) :
         ixLast = nameOut.find('(')
         nameOut = nameOut[0: ixLast]
      nameOut = nameOut.strip()
      if (nameOut.endswith(',')) :
         ixLast = len(nameOut) - 1
         nameOut = nameOut[0: ixLast]
      nameOut = nameOut.strip()
      return nameOut
  
  def __parseLastName(self, authName) :
    lastName = authName
    nameSplit = authName.split(' ')
    ixLast = len(nameSplit)
    lastName = nameSplit[ixLast - 1]
    return lastName.strip()
  
  def __parse2ndLastName(self, authName) :
    lastName = authName
    nameSplit = authName.split(' ')
    ixLast = len(nameSplit)
    if (ixLast > 2) :
      lastName = nameSplit[ixLast - 2]
    return lastName.strip()

  
# end class AuthTools



wrksp = MyEnv.workspace # where to run from
dirOut = os.path.join(wrksp, "Data")
topic = MyEnv.TOPIC
fieldsAuthAlias = ['authname', 'authid']
fileNameCoAuthAlias = os.path.join(dirOut, "GS_" + topic + "_CoAuthAlias_" + str(MyEnv.N_AUTHORS) + ".csv")
fileNameCoAuth = os.path.join(dirOut, "GS_" + topic + "_CoAuth_" + str(MyEnv.N_AUTHORS) + ".csv")
###################################
# READ AUTH Data
###################################
authors = CoAuthTools()
#  def __getMaxAuthID(self) :
fileNamePubAuthors = os.path.join(dirOut, "GS_" + topic + "_PubAuthors_" + str(MyEnv.N_AUTHORS) + ".csv")
pdPublications = PTX.PandasTransform(fileNamePubAuthors, delim=PTX.COMMA, headers=True)

maxAuthID = pdPublications.df['authid'].max() # get the max Auth ID
authors.nextAuthID = int(maxAuthID) # make sure it is integer.
     

###################################
# READ publications 
# Read publication file 
# for each author in publication, find the one that matches the auther
# add to alias list if doesn't match name.
###################################
# load publications file in Pandas

fileNamePubAuthors = os.path.join(dirOut, "GS_" + topic + "_PubAuthors_" + str(MyEnv.N_AUTHORS) + ".csv")
pdPublications = PTX.PandasTransform(fileNamePubAuthors, delim=PTX.COMMA, headers=True)


# Get rows where coauthors were not identified.
dfPubRows = pdPublications.df[(pdPublications.df['coauthid'] == 0) & pdPublications.df['status'].str.contains('COAUTH')]

dictFoundCoAuthIds = []  # coauth name -> authid
for pubRow, rowVal in dfPubRows.iterrows():
    # get pubid for publication
    coauthor_orig = str(dfPubRows.loc[pubRow,'coauthor'])
    coauthor = coauthor_orig.strip()

    coauthid = authors.findAuthorByName(coauthor)

    if (0 == coauthid) :
      authid = int(dfPubRows.loc[pubRow,'authid'])
      pubid = int(dfPubRows.loc[pubRow,'pubid'])
      coauthid = authors.matchAuthorAlias(authid, coauthor, pubid)

      # set all rows in PubAuthors to the coauthor id
      search = (pdPublications.df['coauthor'] == coauthor_orig) & (pdPublications.df['coauthid'] == 0)
      dfUpdate = pdPublications.df[search ]
      updateRows = dfUpdate.shape[0]
      pdPublications.df.loc[search, 'coauthid'] = coauthid
      dfUpdate = pdPublications.df[(pdPublications.df['coauthid'] == coauthid) ]
      updateRows = dfUpdate.shape[0]
      #print(updateRows)
      
		# end if coauthid match
	# end for pubid
# end for authid

pdPublications.write()

##############################
# Write alias dict to file
# DictWriter wants list of dict, not dict
with open(fileNameCoAuthAlias, 'w', encoding='utf-8') as f:
    f.write("%s,%s\n"%('authname','authid'))
    for key in authors.dictAuthAlias.keys():
        f.write("%s,%s\n"%(key,authors.dictAuthAlias[key]))

print("WRITE file: " + fileNameCoAuthAlias)

##############################
# Write coauthor ids to file
##############################
# Create table with a row for each coauthor
tableNodes = []
for authid in authors.dictAuthNames.keys():
    rowOut = {}
    rowOut['id'] = authid
    rowOut['name'] = authors.dictAuthNames[authid] 
    rowOut['lastname'] = authors.dictAuthLastNames[authid] 
    rowOut['university'] = "Unknown"
    rowOut['pub_2023'] = 0
    rowOut['pub_cit_2023'] = 0
    rowOut['tot_citations'] = 0
    rowOut['curYear_citations'] = 0
    rowOut['prevYear_citations'] = 0
    rowOut['country'] = "Unknown"
    tableNodes.append(rowOut)


fieldsNames = ['id',	'name',	'lastname',	'university',	'pub_2023',	'pub_cit_2023',	'tot_citations',	'curYear_citations',	'prevYear_citations',	'country']
with open(fileNameCoAuth, 'w', newline='', encoding='utf-8') as write_csv:
    csvwriter = DictWriter(write_csv, fieldnames = fieldsNames)
    csvwriter.writeheader()
    csvwriter.writerows(tableNodes)
    print("WRITE file: " + fileNameCoAuth)
# end with csv

