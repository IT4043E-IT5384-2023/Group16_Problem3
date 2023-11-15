from selenium import webdriver 
from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchWindowException, NoSuchElementException
from selenium.common import exceptions 

from time import sleep
import json, os


class ChainAbuseScrapper:
    base_url = "https://www.chainabuse.com/api/auth/login?returnTo=%2Fchain%2F{}%3Fpage%3D0%26sort%3Doldest%26postAuth%3D1"
    chain_type_file_path = "chain_types.json"

    def __init__(
            self,  
            timeout = 3, 
            credential = None, 
            web_browser = "chrome", option_str = None,
    ) -> None: 
        self.timeout = timeout # Timeout when a search of element failed.

        self.credential = credential
        if web_browser == "chrome":
            options = webdriver.ChromeOptions() 
            if option_str is not None:
                options.add_argument(option_str) 
            self.driver = webdriver.Chrome(options = options)
        else:
            options = webdriver.FirefoxOptions()
            if option_str is not None:
                options.add_argument(option_str) 
            self.driver = webdriver.Firefox()
        i = 1
        self.driver.get(self.base_url.format(self.chain_types[i]))
        self.saved_file = open(f"{self.chain_types[i]}.json", "w")
    
    def crawl_chain_types(self):
        self.driver.get("https://www.chainabuse.com/_next/data/hoUxeqYxwHHMgSJOYfkus/en/reports.json")
        json_element = self.driver.find_element(By.XPATH, "/html/body/pre")
        data = json.loads(json_element.text)
        page_props = data["pageProps"]
        reports = page_props["initialApolloState"]
        for key in reports:
            if "Chain:" in key:
                yield reports[key]['kind']

    @property
    def chain_types(self):
        if not os.path.exists(self.chain_type_file_path) or os.stat(self.chain_type_file_path).st_size == 0:
            # If file not exists or empty, then write into new one
            chain_types = [chain_type for chain_type in self.crawl_chain_types()]
            with open(self.chain_type_file_path, "w") as f: 
                f.write(json.dumps(chain_types, indent=4))
            return chain_types
        else:
            with open(self.chain_type_file_path, "r") as f: 
                return json.load(f)

    def login(self):
        name_input = self.driver.find_element(By.ID, 'username')
        name_input.send_keys(self.credential['email'])

        continue_button = self.driver.find_element(By.XPATH, "/html/body/main/section/div/div/div/div[1]/div/form/div[2]/button")
        continue_button.click()

        pw_input = self.driver.find_element(By.ID, "password")
        pw_input.send_keys(self.credential['password'])

        submit_button = self.driver.find_element(By.XPATH, "/html/body/main/section/div/div/div/form/div[3]/button")
        submit_button.click()
        

    def init_content(self):
        content = {
            "id": None,
            "vote_count": 0,
            "category": None,
            "description": [],
            "amount_loss": None,
            "reported_addresses": {},
            "reported_domains": [],
        }
        return content

    def parse_report_card(self):
        content = self.init_content()
        WebDriverWait(self.driver, self.timeout).until(EC.presence_of_element_located((By.CLASS_NAME, "create-ScamReportDetails__category"))) 
        # WebDriverWait(self.driver, self.timeout).until(EC.presence_of_element_located((By.CLASS_NAME, "create-ScamReportDetails__category"))) 
        ## Wait until completely loading content

        
        report_id_element = self.driver.find_element(By.XPATH, "/html/head/link[2]")
        href = report_id_element.get_attribute("href")
        content['id'] = href.split("/")[-1]

        description_element =  self.driver.find_element(By.CLASS_NAME, "create-ScamReportDetails__description")
        content['description'] = description_element.text

        vote_count_element = self.driver.find_element(By.CLASS_NAME, "create-BidirectionalVoting__vote-count")
        content['vote_count'] += int(vote_count_element.text)

        category_element = self.driver.find_element(By.CLASS_NAME, "create-ScamReportDetails__category")
        content['category'] = category_element.text

        damage_element = self.driver.find_elements(By.CSS_SELECTOR, "div.create-LossesSection.create-ScamReportDetails__report-info-section > p.create-Text.type-body-md")
        if len(damage_element) == 1:
            content['amount_loss'] = damage_element[0].text
        
        addr_elements = self.driver.find_elements(By.CLASS_NAME, "create-ReportedSection__address-section") 
        if len(addr_elements) > 0:
            for element in addr_elements:
                imgs = element.find_elements(By.CSS_SELECTOR, "img")
                if len(imgs) == 1:
                    src_img = imgs[0].get_attribute("src")
                    report_item_type = (src_img.split("/")[-1]).split(".")[-2] ## Get 'btc' from 'https://www.chainabuse.com/images/chains/btc.svg'
                    content['reported_addresses'][element.text]  = report_item_type
                else:
                    domain_element = element.find_element(By.CLASS_NAME, "create-ReportedSection__domain")
                    content["reported_domains"].append(domain_element.text)
        return content
    
    def crawl(self):
        while True:
            WebDriverWait(self.driver, self.timeout).until(
                lambda driver:
                    len(driver.find_elements(By.CLASS_NAME, "nprogress-busy")) == 0
            )

            WebDriverWait(self.driver, self.timeout).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "create-ScamReportCard__main"))
            )
            current_state_of_page = self.driver.find_element(By.CLASS_NAME, "create-PageSelect__results-label").text

            report_boxes = self.driver.find_elements(By.CLASS_NAME, "create-ScamReportCard__main")
            for i in range(len(report_boxes)):
                report_box = self.driver.find_elements(By.CLASS_NAME, "create-ScamReportCard__main")[i] ## When comeback to the source link, elements will be lost, it need to retrived again.
               
                self.driver.execute_script("arguments[0].click();", report_box)  ## Click into element without scroll into position of element by excuting script.
                if i == 0:
                    pivot = self.parse_report_card()
                    yield pivot
                else:
                    yield self.parse_report_card()

                self.driver.back()

                WebDriverWait(self.driver, self.timeout).until(EC.presence_of_element_located((By.CLASS_NAME, "create-ScamReportCard__main"))) ## Wait for start_url loading

            try:
                next_page_element = self.driver.find_element(By.CSS_SELECTOR, "div.create-ResultsSection__pagination > nav > li:nth-child(9) > button")
                self.driver.execute_script("arguments[0].click();", next_page_element)
            except NoSuchElementException:
                break

            WebDriverWait(self.driver, self.timeout).until(
                EC.presence_of_element_located(
                    locator = (By.CLASS_NAME, "create-PageSelect__results-label"), 
                )
            )
            WebDriverWait(self.driver, self.timeout).until(
                EC.none_of(EC.text_to_be_present_in_element(
                    locator = (By.CLASS_NAME, "create-PageSelect__results-label"), 
                    text_ = current_state_of_page,
                ))
            )
            

        
    def safeClick(self, By, query):
        element_present = EC.element_to_be_clickable((By, query))
        clicked_element = WebDriverWait(self.driver,  self.timeout).until(element_present)
        self.driver.execute_script("arguments[0].click();", clicked_element)

    def store(self, item):
        json_object = json.dumps(item, indent=4)
        self.saved_file.write(",\n" + json_object)

    def scrape(self):
        self.saved_file.write("[\"BOF\"")
        print(self.base_url.format(self.chain_types[1]))
        try:
            for item in self.crawl():
                self.store(item)
        except (NoSuchWindowException, KeyboardInterrupt, TimeoutException) as e:
            self.terminate()
            raise e
            # print("Element did not show up")
            
    def terminate(self):
        self.saved_file.write("\n]")
        self.driver.close()
        self.saved_file.close()

    def execute(self):
        self.login()
        self.scrape()
        self.terminate()
  
if __name__ == "__main__":
    import yaml
    with open('./config.yml', 'r') as f:
        config = yaml.safe_load(f)
    scraper = ChainAbuseScrapper(**config)
    # print(scraper.chain_types)
    # scraper.scrape_chain_urls()
    scraper.execute()
    