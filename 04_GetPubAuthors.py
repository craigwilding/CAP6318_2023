import os
import os.path
import shutil
import datetime
import time
import MyEnvVars as MyEnv
from csv import DictReader
from csv import DictWriter
from selenium import webdriver
from selenium.webdriver.common.by import By

#################################
# Get Co-authors from publications
#################################
wrksp = MyEnv.workspace # where to run from
dirOut = os.path.join(wrksp, "Data")
Downloads = MyEnv.downloads 

today = datetime.date.today()
CURRENT_YEAR = today.year  # get current year

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

fieldsAuthors = ['id','name', 'authorKey', 'lastname', 'university', 'pub_2023', 'pub_cit_2023', 'tot_citations', 'curYear_citations', 'prevYear_citations','otherTopics', 'link']
fieldsPublications = ['authid', 'pubid', 'year', 'pub_cit', 'publink', 'title']
fieldsPubAuthors = ['authid', 'pubid', 'author', 'coauthor', 'coauthOrder', 'orderWeight', 'title', 'year']

fileNameAuth = os.path.join(dirOut, "GS_" + topic + "_Authors_" + str(MyEnv.N_AUTHORS) + ".csv")
fileNamePub = os.path.join(dirOut, "GS_" + topic + "_Publications_" + str(MyEnv.N_AUTHORS) + ".csv")
fileNamePubAuthors = os.path.join(dirOut, "GS_" + topic + "_PubAuthors_" + str(MyEnv.N_AUTHORS) + ".csv")

def getPubAuthors(browser, pubRow) :
    pubid = pubRow['pubid']
    print(pubid)
    publink = pubRow['publink']
    browser.get(publink)
    time.sleep(1)
    authors = ""
    
	# publication page has a table of field with key and value
	# each table row has class key gs_scl
	# each table entry has class="gsc_oci_field" for field name
	# and gsc_oci_value for the field value
    fieldElements = browser.find_elements(By.CLASS_NAME, "gs_scl")
    for fieldElem in fieldElements :
        fieldName = fieldElem.find_element(By.CLASS_NAME, "gsc_oci_field").text
        fieldValue = fieldElem.find_element(By.CLASS_NAME, "gsc_oci_value").text
        if ("Authors" == fieldName) : 
            authors = fieldValue
            break
    return authors
    
 
def updatePubAuthorsFile(rowsOut) :
    addHeader = False
    if (not os.path.exists(fileNamePubAuthors)) : 
        addHeader = True
    with open(fileNamePubAuthors, 'a', newline='', encoding='utf-8') as f_object: # use append mode
        csvwriter = DictWriter(f_object, fieldnames=fieldsPubAuthors)
        if (addHeader) :
            csvwriter.writeheader()

        csvwriter.writerows(rowsOut)
        print("pandas UPDATE file " + fileNamePub)
        f_object.close()
    # end with

def resetBrowser(browserIn) :
    currentURL = browserIn.current_url
    browserIn.close()
    #browser.quit()
    browser = webdriver.Chrome(service=service)
    browser.get(currentURL)
    return browser


#########################################
# START: Create web driver
#########################################
# latest method as of 10/2023
chromedriver_path = MyEnv.chromedriver_path
service = webdriver.ChromeService(executable_path=chromedriver_path)
device = webdriver.Chrome(service=service)
browser = device

##########################################
# READ Authors Table
##########################################
# get id and name
dictAuthorNames = {}
with open(fileNameAuth, 'r', newline='', encoding='utf-8') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        
        authid = int(row["id"])
        name = row["name"]
        dictAuthorNames[authid] = name
    # end for rows
# end with authors

##########################################
# READ Publications list
##########################################
#fieldsPublications = ['authid', 'pubid', 'year', 'pub_cit', 'publink', 'title']
#fieldsPubAuthors = ['authid', 'pubid', 'author', 'coauthor', 'coauthOrder', 'orderWeight', 'title', 'year']
current_N = 0
with open(fileNamePub, 'r', newline='', encoding='utf-8') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    rowsOut = []
    for row in csv_dict_reader:
        authid = int(row["authid"])

        authors = getPubAuthors(browser, row)
        listCoauthors = authors.split(',')
        coauthOrder = 0
        for coauthor in listCoauthors :
            coauthOrder += 1
            rowOut = {}
            rowOut['authid'] = authid
            rowOut['pubid'] = int(row["pubid"])
            rowOut['author'] = dictAuthorNames[authid]
            rowOut['coauthor'] = coauthor
            rowOut['coauthOrder'] = coauthOrder
            orderWeight = 0.5 if coauthOrder > 1 else 1
            rowOut['orderWeight'] = orderWeight
            rowOut['title'] = row["title"]
            rowOut['year'] = row["year"]
            rowsOut.append(rowOut)
        # end for coauthors

        current_N += 1
        if (current_N > 25) : 
            browser = resetBrowser(browser)
            updatePubAuthorsFile(rowsOut)
            rowsOut = []
            current_N = 0
		# end reset

	# end for rows
    updatePubAuthorsFile(rowsOut)
	
# end with publications