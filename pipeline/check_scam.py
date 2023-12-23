from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.edge.service import Service as EdgeService
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import time
from bs4 import BeautifulSoup


from selenium.webdriver.chrome.options import Options
chrome_options = Options()
chrome_options.add_argument('--headless')
# run Selenium in headless mode
chrome_options.add_argument('--no-sandbox')
# overcome limited resource problems
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument("lang=en")
# open Browser in maximized mode
chrome_options.add_argument("start-maximized")
# disable infobars
chrome_options.add_argument("disable-infobars")
# disable extension
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--incognito")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument('user-agent=Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Mobile Safari/537.36')


def check_scam(addr):
    driver = webdriver.Chrome(options=chrome_options)

    # url = "https://www.chainabuse.com/reports"
    url = f"https://www.chainabuse.com/address/{addr}"
    driver.get(url)
    # check_box = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "#react-aria-9")))
    # # check_box.send_keys("0x0F57fe246bdd6C7C1ea05D3896e3ffD1ade02f61")
    # check_box.send_keys(addr)
    # check_box.send_keys(Keys.ENTER)
    # time.sleep(1)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    results = soup.find(class_ = 'create-ResultsSection__results-title')
    # results = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CLASS_NAME, "create-ResultsSection__results-title")))
    if results.text == 'No Reports':
        msg = "No Reports found!"
    else:
        reports = soup.find_all(class_="create-ScamReportCard__main")
        msg = f"Found {len(reports)} scam reported on this address"
        # print("number of reports: ", len(reports))
        for i in range(len(reports)):
            # try:

                # print("reports: ", report)
            category = reports[i].find(class_="create-Text type-body-lg-heavy create-ScamReportCard__category-label").text
            description = reports[i].find(class_="create-ScamReportCard__preview-description").text
            vote_count = reports[i].find_all(class_="create-Text type-body-md create-BidirectionalVoting__vote-count")
            if len(vote_count) == 0:
                msg = "Unverified report!" 
            else: 
                submitted = reports[i].find(class_="create-ScamReportCard__submitted-info").text   
                msg += f'\n{i+1}. Category: {category} \n- Description: {description} \n- Vote count: {vote_count[0].text} \n- {submitted}'
                # msg += f'\n- Category: {category} \n- Vote count: {vote_count[0].text} \n- {submitted}'            
    
    driver.quit()
    print(msg)
    return msg

if __name__ == "__main__":
    addr = '0xb91f8b533c63286b2ca428ef338fe4b198a44d15'
    print(check_scam(addr))