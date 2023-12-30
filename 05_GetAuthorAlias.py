import os
import os.path
from csv import DictReader
from csv import DictWriter
import MyEnvVars as MyEnv



#################################
# DATA STRUCTURES
#################################

class AuthorTools:
  
  def __init__(self):
    #self.listAuthFilter = self.__loadFilter()
    self.listAuthID = []
    self.dictAuthNames = {}
    self.dictAuthLastNames = {}
    self.dictAuthAlias = {}
    self.rowsNoAuthor = []
    self.__loadNames()
    #self.dictAuthAlias = self.__loadAuthAlias()
    #self.listNoAuth = self.__loadNoAuth()
    #print('nameCount = ', str(len(self.dictAuthNames)))

  def __loadFilter(self) :
    wrksp = MyEnv.workspace # where to run from
    dirOut = os.path.join(wrksp, "Data")
    os.chdir(wrksp)
    topic = MyEnv.TOPIC
    fieldsAuthFilter = ['id','name', 'authorKey', 'lastname', 'affiliation', 'university', 'country', 'tot_citations', 'curYear_citations', 'prevYear_citations','otherTopics', 'link']
    fileNameAuthFilter = os.path.join(dirOut, "GS_" + topic + "_AuthFilt_" + str(MyEnv.N_AUTHORS) + ".csv")
    if ("Tourism" == topic) :
      fileNameAuthFilter = os.path.join(dirOut, "GS_Tourism_Authors_500_corrected+countries_6"  + ".csv")
    locallistAuthFilter = []
    with open(fileNameAuthFilter, 'r', newline='', encoding='utf-8') as read_obj:
        csv_dict_reader = DictReader(read_obj)
        for row in csv_dict_reader:
            authid = int(row["id"])
            locallistAuthFilter.append(authid)
          
    return locallistAuthFilter  

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
      else :
        dupId = self.dictAuthAlias[alias]
        if (dupId != authid) :
          print("duplicate author name:",alias, authid, dupId)

  def matchAuthorAlias(self, authid, listCoauthors, pubid) :
    alias = ""
    authName = self.getName(authid)
    lastName = self.getLastName(authid)
    authCandidates = []
    if (921 == authid) and (4574 == pubid) :
        print("Test Name", authid, authName)
    for coauth in listCoauthors :
        if (authid == self.findAuthorByName(coauth)) :
          return coauth
        testLastName = self.normalize(lastName)
        testLastName2 = testLastName.replace('-',' ').replace("‐",' ')
        test2ndLast = self.__parse2ndLastName(authName)
        test2ndLast = self.normalize(test2ndLast)
        testCoauth = self.normalize(coauth)
        testCoauth2 = testCoauth.replace('-',' ').replace("‐",' ')
        tCoLN = self.normalize(self.__parseLastName(coauth))
        ix1 = testCoauth.find(testLastName)
        ix2 = testCoauth.find(testLastName2)
        ix3 = testCoauth2.find(testLastName2)
        if (testCoauth.find(testLastName) >= 0) or (testCoauth.find(testLastName2) >= 0) or (testCoauth.find(test2ndLast) >= 0) \
            or (testCoauth2.find(testLastName2) >= 0) or (testLastName.find(tCoLN) >= 0)  :
            if (coauth in self.dictAuthAlias) :
                return coauth
            else :
                alias = coauth
                authCandidates.append(coauth)
            #print("end else")
        #print("end if")
    #print("end for")

    if (len(authCandidates) > 1) :
      # found multiple authors with last name
      
      nameSplit = authName.split(' ')
      firstName = nameSplit[0].replace('.','')
      for coauth in authCandidates :
        testFirstName = firstName.lower().strip()
        testCoauth = str(coauth)
        testCoauth = testCoauth.lower().strip()
        if (testCoauth.find(testFirstName) >= 0) :
            alias = coauth
            self.addAuthAlias(coauth, authid)
            break
        # end if firstname
      # end for
      # check if no match found
    elif (len(authCandidates) == 1)  : # only one lastName match
      coauth = authCandidates[0]
      alias = coauth
      self.addAuthAlias(coauth, authid)
    else :
      alias = lastName
      rowNoAuthor = {}
      rowNoAuthor['authid'] = authid
      rowNoAuthor['pubid'] = pubid
      rowNoAuthor['authName'] = authName
      coauthList = ""
      for coauth in listCoauthors :
        coauthList = coauth + "|"
      rowNoAuthor['coauthList'] = coauthList
      self.rowsNoAuthor.append(rowNoAuthor)
         
      # print("No author alias found:", authid, pubid, authName,  listCoauthors)
    return alias
  

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

authors = AuthorTools()


wrksp = MyEnv.workspace # where to run from
dirOut = os.path.join(wrksp, "Data")
topic = MyEnv.TOPIC
fieldsAuthAlias = ['authname', 'authid']
fileNameAuthAlias = os.path.join(dirOut, "GS_" + topic + "_AuthAlias_" + str(MyEnv.N_AUTHORS) + ".csv")
###################################
# READ AUTH Data
###################################

     

###################################
# READ publications 
# Read publication file 
# for each author in publication, find the one that matches the auther
# add to alias list if doesn't match name.
###################################
# load publications file in Pandas
import PandasTransforms as PTX
fileNamePubAuthors = os.path.join(dirOut, "GS_" + topic + "_PubAuthors_" + str(MyEnv.N_AUTHORS) + ".csv")
pdPublications = PTX.PandasTransform(fileNamePubAuthors, delim=PTX.COMMA, headers=True)

listFoundPubIds = []
for authid in authors.getAuthorIds() :
    authorName = authors.getName(authid)
    # For each author, get their publications
    listSearchValues = [authid]
    dfPubRows = pdPublications.selectRows('authid', listSearchValues)
    for pubRow, rowVal in dfPubRows.iterrows():
        # get pubid for publication
        pubid = int(dfPubRows.loc[pubRow,'pubid'])
        if (pubid not in listFoundPubIds) :
            listFoundPubIds.append(pubid)
            # get only coauthors for the same pub id
            listpubid = [pubid]
            listCoauthors = []
            dfPubIdRows = dfPubRows[dfPubRows['pubid'].isin(listpubid)]
            for pubIdRow, rowIdVal in dfPubIdRows.iterrows(): 
                coauthor = str(dfPubIdRows.loc[pubIdRow,'coauthor'])
                coauthor = coauthor.strip()
                listCoauthors.append(coauthor)
                
            if (authorName not in listCoauthors) :
                alias = authors.matchAuthorAlias(authid, listCoauthors, pubid)
                #print(authorName, ' ', alias)
            # end get alias
		# end if pub id look up
	# end for pubid
# end for authid

authors.saveNoAuthors()

##############################
# Write alias dict to file
# DictWriter wants list of dict, not dict
with open(fileNameAuthAlias, 'w', encoding='utf-8') as f:
    f.write("%s,%s\n"%('authname','authid'))
    for key in authors.dictAuthAlias.keys():
        f.write("%s,%s\n"%(key,authors.dictAuthAlias[key]))

print("WRITE file: " + fileNameAuthAlias)
