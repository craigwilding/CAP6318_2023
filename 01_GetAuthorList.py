##################################################################
# This gets the original list of Authors and the links to their page
##################################################################

import os
import os.path
import shutil
import datetime
import time
import MyEnvVars as MyEnv
from csv import DictWriter
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
# import selenium.webdriver.support.ui as ui
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

#################################
# DATA STRUCTURES
#################################
rowsOutAuthors = []  # used for writing to csv
fieldsAuthors = ['id','name', 'authorKey', 'lastname', 'university', 'pub_2023', 'pub_cit_2023', 'tot_citations', 'curYear_citations', 'prevYear_citations','otherTopics', 'link']
rowsOutCoAuthors = []  # used for writing to csv
fieldsCoAuthors = ['id', 'authorKey', 'coauthor', 'coauthOrder', 'orderWeight', 'title', 'co_citations', 'year']

# Local directory for where files are stored
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
searchTopic = "label:" + topic
if ("ALL" == topic) :
    searchTopic = MyEnv.TOPIC_ALL
print("topic", topic)
print("searchTopic", searchTopic)

def getTextIfExist(element, myBy, key) :
    # Use this if an element may not exist
    text = ""
    # passing the By.CLASS_NAME is not working
    elementList = element.find_elements(By.CLASS_NAME, key)
    if len(elementList) > 0 :
        text = elementList[0].text
        print(text)
    return remove_non_ascii_1(text)

def getLastName(lastNameIn) :
    lastName = lastNameIn
    # check for first last, credentials
    ixC = lastName.find(',')
    if (ixC > 0) :
        lastName = lastName[0:ixC]
    # end if

    ixSp = lastName.rfind(' ')
    if (ixSp > 0) :
        lastName = lastName[ixSp +1:]
    # end if

    return lastName

def getAuthor(listCoAuthors, lastName) :
    author = ""
    # check for a coauthor that includes the lastName
    for coauth in listCoAuthors :
        coAuthName = coauth.strip() # remove leading/trailing spaces
        ixL = coAuthName.find(lastName)
        ixC = coAuthName.find(lastName.capitalize())
        if (ixL > 0) or (ixC > 0) :
            author = coAuthName
            break
        # end if
    # end for

    
    if ("" == author) and (lastName.find('-')>0) :
        ln_split = lastName.split('-')
        for ln_part in ln_split :
            author = getAuthor(listCoAuthors, ln_part)
            if ("" != author) :
                break;
    

    return author

def remove_non_ascii_1(text):
    # this removes non ascii text i.e. spanish or other language chars.
    return text
    #return ''.join([i if ord(i) < 128 else '' for i in text])

def resetBrowser(browserIn) :
    currentURL = browserIn.current_url
    browserIn.close()
    #browser.quit()
    browser = webdriver.Chrome(service=service)
    browser.get(currentURL)
    return browser

###############################################
# GET AUTHOR LINK 
# This creates an author record, or dictionary 
# that holds the column values stored for each author.
# name, url link, university, citation counts
###############################################
def getAuthorLink(author) :
    rowOut = {}
    # Get Name and link to author's page
    nameElem = author.find_element(By.CLASS_NAME, "gs_ai_name")
    name = remove_non_ascii_1(nameElem.text)
    rowOut["name"] = name
    print(name)
    # parse name to lastname
    rowOut["lastname"] = getLastName(name)
    # set author to blank for now
    rowOut["authorKey"] = ""
    # get link to author's page
    linkElem = nameElem.find_element(By.TAG_NAME, "a")
    href = linkElem.get_attribute('href')
    print(href)
    rowOut["link"] = href
    # get university
    university = getTextIfExist(author, By.CLASS_NAME, "gs_ai_aff")
    rowOut["university"] = university
    # get citation count
    citedBy = getTextIfExist(author, By.CLASS_NAME, "gs_ai_cby")
    tot_citations = 0
    if (len(citedBy) > 0) :
        citedBy = citedBy.replace("Cited by ", '') # remove prefix
        tot_citations = int(citedBy)
        print("tot_citations:", tot_citations)
    # end if
    rowOut["tot_citations"] = tot_citations
    rowOut["pub_2023"] = 0
    rowOut["pub_cit_2023"] = 0
    rowOut["curYear_citations"] = -1
    rowOut["prevYear_citations"] = 0

    # get other TOPICs listed
    otherTopics = author.find_elements(By.CLASS_NAME, "gs_ai_one_int")
    TOPICList = ""
    for MyEnv.TOPIC in otherTopics :
        otherTopic = remove_non_ascii_1(MyEnv.TOPIC.text)
        if (otherTopic != MyEnv.TOPIC) :
            if (len(TOPICList) > 0) :
                TOPICList += "|" + otherTopic
            else:
                TOPICList += otherTopic
            # end if
        # end if
    
    #end for
    
    #print(TOPICList)
    rowOut["otherTopics"] = TOPICList
    return rowOut





#########################################
# START: Create web driver
#########################################
# latest method as of 10/2023
chromedriver_path = MyEnv.chromedriver_path
service = webdriver.ChromeService(executable_path=chromedriver_path)
device = webdriver.Chrome(service=service)


#########################################
# GET Authors
# This opens the google scholar page, searches for the TOPIC
# and goes through the list of returned authors collecting the
# url to the authors page and assigning an ID number. 
#########################################
def getTopNAuthors(N, browser) :
    #########################################
    # Open Google Scholar page
    #########################################
    print("Opening web page")
    browser.get('https://scholar.google.com/citations?view_op=search_authors&hl=en')
    time.sleep(2)

    ### Search by MyEnv.TOPIC label
    searchBox = browser.find_element(By.ID, "gs_hdr_tsi")
    # searchTopic = "label:" + topic
    searchBox = searchBox.send_keys(searchTopic)
    searchButton = browser.find_element(By.ID, "gs_hdr_tsb").click()
    time.sleep(3)
    #########################################
    ### Get top N author nodes
    #########################################
    id = 0
    current_N = 0
    while id < N :
        
        # Get list of authors
        authorsList = browser.find_elements(By.CLASS_NAME, "gsc_1usr")
        for author in authorsList :
            # create new author record
            # append author record to rowsOutAuthors list
            # rowsOutAuthors will be written to the csv file

            rowOut = getAuthorLink(author)
            id += 1
            current_N += 1
            rowOut["id"] = id
            rowsOutAuthors.append(rowOut)
        # end for
        
        if (current_N > 100) :
            # reset browser to avoid captcha
            currentURL = browser.current_url
            print(currentURL)
            browser = resetBrowser(browser) 
            current_N = 0
        # end if max authors

        nextButton = browser.find_element(By.XPATH, "//button[@aria-label='Next']")
        if (nextButton.is_enabled()) :
            nextButton.click()
        else :
            id = N # end 
        
        
    # end while


    return browser
# end getTopNAuthors()
browser = device
browser = getTopNAuthors(MyEnv.N_AUTHORS, browser)


#################################
# WRITE AUTHOR TITLES and Links
#################################
fileNameAuth = os.path.join(dirOut, "GS_" + topic + "_Authors_" + str(MyEnv.N_AUTHORS) + ".csv")
with open(fileNameAuth, 'w', newline='', encoding='utf-8') as write_csv:
    csvwriter = DictWriter(write_csv, fieldnames = fieldsAuthors)
    csvwriter.writeheader()
    csvwriter.writerows(rowsOutAuthors)
    print("WRITE file: " + fileNameAuth)
# end with csv



time.sleep(2)
browser.close()
browser.quit()
del browser
print("Finished")

