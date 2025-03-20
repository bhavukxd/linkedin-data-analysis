from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from time import sleep
from lxml import etree
import csv
parser = etree.HTMLParser()
print('- Finish importing packages')
driver = webdriver.Chrome()
sleep(2)
url = 'https://www.linkedin.com/login'
driver.get(url)
print('- Finish initializing a driver')
sleep(2)
credential = open('credentials.txt')
line = credential.readlines()
username = line[0]
password = line[1]
print('- Finish importing the login credentials')
sleep(2)
email_field = driver.find_element(By.ID,'username')
email_field.send_keys(username)
print('- Finish keying in email')
sleep(3)
password_field = driver.find_element(By.NAME,'session_password')
password_field.send_keys(password)
print('- Finish keying in password')
sleep(2)
signin_field = driver.find_element(By.XPATH,'//button[@type="submit"]')
signin_field.click()
sleep(3)
print('- Finish Task 1: Login to Linkedin')
def GetURL():
    page_source = BeautifulSoup(driver.page_source)
    profiles = page_source.find_all('a', class_ = 'app-aware-link') #('a', class_ = 'search-result__result-link ember-view')
    all_profile_URL = []
    for profile in profiles:
        profile_URL = profile.get('href')
        if profile_URL not in all_profile_URL:
            all_profile_URL.append(profile_URL)
    return all_profile_URL
URLs_all_page = ["https://www.linkedin.com/in/sundarpichai","https://www.linkedin.com/in/kevin-ichhpurani-92822b1","https://www.linkedin.com/in/alexander-schiffhauer"]
print('- Finish Task 3: Scrape the URLs')
with open('output.csv', 'w',  newline = '') as file_output:
        headers = ['Name', 'Job Title','Job Duration','Education Degree','Education Institute']
        writer = csv.DictWriter(file_output, delimiter=',', lineterminator='\n',fieldnames=headers)
        writer.writeheader()
        for linkedin_URL in URLs_all_page:
            try:
                driver.get(linkedin_URL)
                print('- Accessing profile: ', linkedin_URL)
                sleep(5)
                page_source = BeautifulSoup(driver.page_source, "html.parser")
                info_div = page_source
                tree = etree.fromstring(driver.page_source, parser)
                name = info_div.find('h1', class_='text-heading-xlarge inline t-24 v-align-middle break-words').get_text().strip()
                print('--- Profile name is: ', name)
                position= info_div.find_all('li', class_='bXJoURpeaiFbcaiFEEFQcBOTfgUEKCiofGmzQSQ')[1].get_text().strip().split("\n")[0]
                position = position[:len(position)//2]
                print('--- Profile position is: ',position)
                duration = info_div.find('span', class_='pvs-entity__caption-wrapper').get_text().strip()
                print('--- Profile duration is: ',duration)
                inst = driver.find_element(By.XPATH,'//div[@id="education"]/text()')
                print('--- Profile inst is: ',inst)
                degree = info_div.find('span', class_='t-14 t-normal').get_text().strip()
                print('--- Profile deg is: ',degree)
                skills = info_div.find('span', class_='t-14 t-normal').get_text().strip()
                print('--- Profile deg is: ',degree)
                writer.writerow({headers[0]:name, headers[1]:position, headers[2]:duration, headers[3]:degree,headers[4]:inst})
                print('\n')
            except:
                pass
print('Mission Completed!')
