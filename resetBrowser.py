"""

def resetBrowser() :
    currentURL = browser.current_url
    browser = webdriver.Chrome(service=service)
    browser.close()
    browser.quit()
    browser = webdriver.Chrome(service=service)
    browser.get(currentURL)
"""