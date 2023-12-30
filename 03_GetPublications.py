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
# Get Publications for Authors
# For each author, get all current year publications and save the link
#################################

wrksp = MyEnv.workspace # where to run from
dirOut = os.path.join(wrksp, "Data")
Downloads = MyEnv.downloads 

today = datetime.date.today()
CURRENT_YEAR = today.year  # get current year
MIN_YEAR = CURRENT_YEAR - MyEnv.YEARS_PRIOR

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
# Local directory for where files are stored
rowsOutPublications = []  # used for writing to csv
fieldsAuthors = ['id','name', 'authorKey', 'lastname', 'university', 'pub_2023', 'pub_cit_2023', 'tot_citations', 'curYear_citations', 'prevYear_citations','otherTopics', 'link']
fieldsPublications = ['authid', 'pubid', 'year', 'pub_cit', 'publink', 'title']
dictAuthors = {}
setExistingPubAuthors = set()
fileNameAuth = os.path.join(dirOut, "GS_" + topic + "_Authors_" + str(MyEnv.N_AUTHORS) + ".csv")
fileNamePub = os.path.join(dirOut, "GS_" + topic + "_Publications_" + str(MyEnv.N_AUTHORS) + ".csv")

#######################################################
# class PubID
# used to increment ID index of publications
# Python doesn't like passing a global variable through functions
# so it is wrapped in a class
#######################################################
class PubID() :
    pubId = 0
    def getNextPubID(self) :
        self.pubId += 1
        return self.pubId
# end class PubID
myPubID = PubID()


def only_numerics(seq):
    seq_type= type(seq)
    return seq_type().join(filter(seq_type.isdigit, seq))

def getIntIfExist(element) :
    # Get int value, check for blank or bad chars
    out = 0
    text = element.get_attribute("textContent")
    if len(text) > 0 :
        out = int(only_numerics(text))
    return out

def sortPublicationsByYear(browser) :
    sortOptions = browser.find_elements(By.CLASS_NAME, "gs_md_li")
    for sortOption in sortOptions :
        #linkElem = sortOption.find_element(By.TAG_NAME, "a")
        href = sortOption.get_attribute('href')
        if (href.find("sortby=pubdate") > 1) :
            browser.get(href)
            time.sleep(1)
            break
        # endif
    # end sort by year
    
    # Show pages for all in current year
    year = CURRENT_YEAR + 10
    

    while (year >= MIN_YEAR) :
        publications = browser.find_elements(By.CLASS_NAME, "gsc_a_tr")
        listCount = len(publications)
        if (listCount > 0) :
            lastPub = publications[listCount - 1]
            yearElem = lastPub.find_element(By.CLASS_NAME, "gsc_a_y")
            year = getIntIfExist(yearElem)
        else :
            year = MIN_YEAR -1
        # Click More button to get all for current year
        if (year >= MIN_YEAR) :
            moreButton = browser.find_element(By.ID, "gsc_bpf_more")
            if (moreButton.is_enabled()) :
                moreButton.click()
                time.sleep(1)
            else :
                year = MIN_YEAR -1
        # end if more pages
    # end while


#################################
# Get Publications from Author Page 
#################################
def getpublications(id, myPubID) :
    print(id)
    authorLink = dictAuthors[id]
    # go to author's page
    browser.get(authorLink)
    time.sleep(1)

    # Sort Publications by Year
    sortPublicationsByYear(browser)
    
	# Get publications for current Year
    publications = browser.find_elements(By.CLASS_NAME, "gsc_a_tr")
    for pub in publications :
        # There is a table of publications with columns: title, citations, year
        titleElem = pub.find_element(By.CLASS_NAME, "gsc_a_t")
        citationElem = pub.find_element(By.CLASS_NAME, "gsc_a_c")
        citCountElem = citationElem.find_element(By.TAG_NAME, "a")
        pub_cit = getIntIfExist(citCountElem)
        yearElem = pub.find_element(By.CLASS_NAME, "gsc_a_y")
        year = getIntIfExist(yearElem)
        if (MIN_YEAR <= year) :
            # Publication is for the current year
			# Get the publication links
			# fieldsPublications = ['id', 'year', 'publink',  'title']
            rowOut = {}
            rowOut['authid'] = id
            rowOut['pubid'] = myPubID.getNextPubID()
            rowOut['year'] = year
            rowOut['pub_cit'] = pub_cit
            linkElem = titleElem.find_element(By.TAG_NAME, "a")
            publink = linkElem.get_attribute('href')
            rowOut['publink'] = publink
            title = linkElem.get_attribute("textContent")
            rowOut['title'] = title
            rowsOutPublications.append(rowOut)

        # end if current year
    # end for publications
# end getpublications


def resetBrowser(browserIn) :
    currentURL = browserIn.current_url
    browserIn.close()
    #browser.quit()
    browser = webdriver.Chrome(service=service)
    browser.get(currentURL)
    return browser

def updatePubFile(rowsOut) :
    addHeader = False
    if (not os.path.exists(fileNamePub)) : 
        addHeader = True
    with open(fileNamePub, 'a', newline='', encoding='utf-8') as f_object: # use append mode
        csvwriter = DictWriter(f_object, fieldnames=fieldsPublications)
        if (addHeader) :
            csvwriter.writeheader()

        csvwriter.writerows(rowsOut)
        print("pandas UPDATE file " + fileNamePub)
        f_object.close()
    # end with


#########################################
# START: Create web driver
#########################################
# latest method as of 10/2023
chromedriver_path = MyEnv.chromedriver_path
service = webdriver.ChromeService(executable_path=chromedriver_path)
device = webdriver.Chrome(service=service)
browser = device


##########################################
# READ original Authors list
##########################################
# get id and link
id = 0
tableAuthorsALL = {}

with open(fileNameAuth, 'r', newline='', encoding='utf-8') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        
        id = int(row["id"])
        authorLink = row["link"]
        dictAuthors[id] = authorLink
    # end for rows
# end with authors

##########################################
# READ current Publications list
##########################################

if (os.path.exists(fileNamePub)) :
    with open(fileNamePub, 'r', newline='', encoding='utf-8') as read_obj: 
        csv_dict_reader = DictReader(read_obj)
        
		# get list of author ids that we already got publications for.
        for row in csv_dict_reader:
            id = int(row["authid"])
            myPubID.getNextPubID()
            setExistingPubAuthors.add(id)
		# end for rows
	# end with authors
# end if file exists

current_N = 0
for id in dictAuthors :
    if (id not in setExistingPubAuthors) :
        getpublications(id, myPubID)
        current_N += 1
        if (current_N > 25) : 
            browser = resetBrowser(browser)
            updatePubFile(rowsOutPublications)
            rowsOutPublications = []
            current_N = 0
        # end reset
    # end if not retrieved already
# end for each id

# Save any that haven't been updated yet.
updatePubFile(rowsOutPublications)  
    
