import os
import os.path
import shutil
import datetime
import time
import pandas as pd 
import PandasTransforms as PTX
import MyEnvVars as MyEnv

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
    return text


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
    return ''.join([i if ord(i) < 128 else '' for i in text])

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
    while (year >= CURRENT_YEAR) :
        publications = browser.find_elements(By.CLASS_NAME, "gsc_a_tr")
        listCount = len(publications)
        if (listCount > 0) :
            lastPub = publications[listCount - 1]
            yearElem = lastPub.find_element(By.CLASS_NAME, "gsc_a_y")
            year = getIntIfExist(yearElem)
        else :
            year = CURRENT_YEAR -1
        # Click More button to get all for current year
        if (year >= CURRENT_YEAR) :
            moreButton = browser.find_element(By.ID, "gsc_bpf_more")
            if (moreButton.is_enabled()) :
                moreButton.click()
                time.sleep(1)
            else :
                year = CURRENT_YEAR -1
        # end if more pages
    # end while


#################################
# Get Author Data from Author Page
#################################
def getAuthorData(rowid, rowAuthor) :
    id = rowAuthor["id"]
    name = rowAuthor["name"]
    print(id, ' ', name)
    if (350 == id) :
        print("debug")
    author = rowAuthor["authorKey"]
    lastname = rowAuthor["lastname"]
    link = rowAuthor["link"]
    # go to author's page
    browser.get(link)
    time.sleep(1)

    pub_2023_count = 0
    pub_cit_2023_count = 0

    # Get Publications by Year
    sortPublicationsByYear(browser)
    publications = browser.find_elements(By.CLASS_NAME, "gsc_a_tr")
    for pub in publications :
        # There is a table of publications with columns: title, citations, year
        titleElem = pub.find_element(By.CLASS_NAME, "gsc_a_t")
        citeElem = pub.find_element(By.CLASS_NAME, "gsc_a_c")
        yearElem = pub.find_element(By.CLASS_NAME, "gsc_a_y")
        year = getIntIfExist(yearElem)
        if (CURRENT_YEAR == year) :
            pub_2023_count = pub_2023_count + 1
            citeCount = getIntIfExist(citeElem)

            pub_cit_2023_count = pub_cit_2023_count + citeCount
        # end if current year
    # end for publications
    pandasTX.df.loc[rowid, 'pub_2023'] = pub_2023_count
    pandasTX.df.loc[rowid, 'pub_cit_2023'] = pub_cit_2023_count


    # Get citations by year
    #browser.maximize_window()
    yearly_citations = browser.find_elements(By.CLASS_NAME, "gsc_g_al")
    len_citations = len(yearly_citations)
    if (len_citations > 1) :
        curYearElem = yearly_citations[len_citations - 1]
        curYear_citations = getIntIfExist(curYearElem)
        pandasTX.df.loc[rowid, 'curYear_citations'] = curYear_citations
    # end curYearCitations

    if (len_citations > 2) :
        prevYearElem = yearly_citations[len_citations - 2]
        prevYear_citations =  getIntIfExist(prevYearElem)
        pandasTX.df.loc[rowid, 'prevYear_citations'] = prevYear_citations
    # end curYearCitations 
    
# end getAuthorData


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

#################################
# FOR EACH AUTHOR GET AUTHOR DATA
# Reset browser every 100 authors and save to Author file.
#################################
current_N = 0
fileNameAuth = os.path.join(dirOut, "GS_" + topic + "_Authors_" + str(MyEnv.N_AUTHORS) + ".csv")
pandasTX = PTX.PandasTransform(fileNameAuth, delim=PTX.COMMA, headers=True)
print("pandas READ file " + fileNameAuth)
for rowid, rowAuthor in pandasTX.df.iterrows():
        
    id = rowAuthor["id"]
    link = rowAuthor["link"]
    curYear_citations = rowAuthor["curYear_citations"]

    # skip rows already coded
    if (-1 == curYear_citations)  :
        current_N += 1
        getAuthorData(rowid, rowAuthor)
        if (current_N > 25) :
            print("pandas UPDATE file " + fileNameAuth)
            pandasTX.write()
            # reset browser to avoid captcha
            currentURL = browser.current_url
            print(currentURL)
            browser = resetBrowser(browser) 
            current_N = 0
        # end if max authors   
    # end if not coded.


# end for each author row

print("pandas WRITE FINAL file " + fileNameAuth)
pandasTX.write()

time.sleep(2)
browser.close()
browser.quit()
del browser
print("Finished")

