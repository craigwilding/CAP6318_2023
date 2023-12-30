import os
import time
from selenium import webdriver
import MyEnvVars as MyEnv

#########################################
# START: Create web driver
#########################################
# latest method as of 10/2023
chromedriver_path = MyEnv.chromedriver_path
service = webdriver.ChromeService(executable_path=chromedriver_path)
browser = webdriver.Chrome(service=service)
browser.get("https://selenium-python.readthedocs.io/locating-elements.html")

time.sleep(5)
browser.close()
browser.quit()
del browser