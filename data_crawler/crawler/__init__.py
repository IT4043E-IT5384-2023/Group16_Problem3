from selenium import webdriver 
from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchWindowException, NoSuchElementException
from selenium.common import exceptions 

from time import sleep
import json, os, sys
import logging


class ChainAbuseScrapper:
    base_url = "https://www.chainabuse.com/api/auth/login?returnTo=%2Fchain%2F{}%3Fpage%3D{}%26sort%3Doldest%26postAuth%3D1"
    chain_type_file_path = "chain_types.json"

    def __init__(
            self,  
            page_index,
            timeout = 3, retry = 2,
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
        self.driver.get(
            self.base_url.format(
                self.chain_types[page_index['chain_id']], 
                page_index['start_num'],
            )
        )
        self.saved_file = open(f"./{self.chain_types[page_index['chain_id']]}_{page_index['start_num']}.json", "w")
    
        self.retry = retry



    def crawl_chain_types(self):
        self.driver.get("https://www.chainabuse.com/_next/data/hoUxeqYxwHHMgSJOYfkus/en/reports.json")
        json_element = self.safe_find_element(By.XPATH, "/html/body/pre")
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
        name_input = self.safe_find_element(By.ID, 'username')
        name_input.send_keys(self.credential['email'])

        continue_button = self.safe_find_element(By.XPATH, "/html/body/main/section/div/div/div/div[1]/div/form/div[2]/button")
        continue_button.click()

        pw_input = self.safe_find_element(By.ID, "password")
        pw_input.send_keys(self.credential['password'])

        submit_button = self.safe_find_element(By.XPATH, "/html/body/main/section/div/div/div/form/div[3]/button")
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
        # WebDriverWait(self.driver, self.timeout).until(EC.presence_of_element_located((By.CLASS_NAME, "create-ScamReportDetails__category"))) 
        print("Waiting for category: ")
        # WebDriverWait(self.driver, self.timeout).until(EC.visibility_of_element_located((By.CLASS_NAME, "create-ScamReportDetails__category")))
        self.safe_wait(event = EC.visibility_of_element_located((By.CLASS_NAME, "create-ScamReportDetails__category")))
        category_element = self.safe_find_element(By.CLASS_NAME, "create-ScamReportDetails__category")
        content['category'] = category_element.text
        print("Done waiting")
        
        # description_element = self.safe_find_element(By.CLASS_NAME, "create-ScamReportDetails__description")
        self.safe_wait(event = EC.presence_of_element_located((By.CSS_SELECTOR, "html > head > meta[name='description']")))
        description_element = self.driver.find_element(By.CSS_SELECTOR, "html > head > meta[name='description']")

        content['description'] = description_element.get_attribute("content") #text
        ## Wait until completely loading content

        
        # report_id_element = self.safe_find_element(By.CSS_SELECTOR, "head > link:nth-child(15)")
        # href = report_id_element.get_attribute("href")
        href = self.driver.current_url
        content['id'] = href.split("/")[2]

        vote_count_element = self.safe_find_element(By.CLASS_NAME, "create-BidirectionalVoting__vote-count")
        content['vote_count'] += int(vote_count_element.text)

        category_element = self.safe_find_element(By.CLASS_NAME, "create-ScamReportDetails__category")
        content['category'] = category_element.text
        
        ## Not always damage_element presented in web, therefore we dont use safe_wait.
        damage_element = self.driver.find_elements(By.CSS_SELECTOR, "div.create-LossesSection.create-ScamReportDetails__report-info-section > p.create-Text.type-body-md")
        if len(damage_element) == 1:
            content['amount_loss'] = damage_element[0].text
        
        addr_elements = self.safe_find_elements(By.CLASS_NAME, "create-ReportedSection__address-section") 
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
        
        print(content)
        return content
    
    def safe_find_element(self, By, value):
        retry = self.retry
        while True:
            try:
                WebDriverWait(self.driver, self.timeout).until(EC.visibility_of_element_located((By, value)))
                element = self.driver.find_element(By, value)
            except TimeoutException as e:
                if retry > 0:
                    self.driver.refresh()
                    retry -= 1
                    logging.info(f"{retry} time retry left to locate {value}")
                else:
                    raise e
            else:
                return element

    def safe_find_elements(self, By, value):
        retry = self.retry
        while True:
            try:
                WebDriverWait(self.driver, self.timeout).until(EC.presence_of_element_located((By, value)))
                elements = self.driver.find_elements(By, value)
            except TimeoutException as e:
                if retry > 0:
                    self.driver.refresh()
                    retry -= 1
                    logging.info(f"{retry} time retry left to locate {value}")
                else:
                    raise e
            else:
                return elements


    def crawl(self):
        while True:
            logging.info(f"Current_url: {self.driver.current_url}")
            self.safe_wait(event = EC.element_to_be_clickable((By.CLASS_NAME, "create-ScamReportCard__main")))
            current_state_of_page = self.safe_find_element(By.CLASS_NAME, "create-PageSelect__results-label").text
            
            report_boxes = self.safe_find_elements(By.CLASS_NAME, "create-ScamReportCard__main")
            for i in range(len(report_boxes)):
                logging.info(f"Start crawling page at {i+1}/{len(report_boxes)}:")
                report_box = self.safe_find_elements(By.CLASS_NAME, "create-ScamReportCard__main")[i] ## When comeback to the source link, elements will be lost, it need to retrived again.
               
                self.driver.execute_script("arguments[0].click();", report_box)  ## Click into element without scroll into position of element by excuting script.

                yield self.parse_report_card()
                print("output of parsing")
                self.driver.back()

                self.safe_wait(EC.presence_of_element_located((By.CLASS_NAME, "create-ScamReportCard__main")))

            try:
                next_page_element = self.safe_find_element(By.CSS_SELECTOR, "div.create-ResultsSection__pagination > nav > li:nth-child(9) > button")
                self.driver.execute_script("arguments[0].click();", next_page_element)
            except NoSuchElementException as e:
                yield None
                raise e
                # break

            self.safe_wait(event = EC.presence_of_element_located(locator = (By.CLASS_NAME, "create-PageSelect__results-label")))
            self.safe_wait(
                event = EC.none_of(EC.text_to_be_present_in_element(
                    locator = (By.CLASS_NAME, "create-PageSelect__results-label"), 
                    text_ = current_state_of_page,
                ))
            )
    def safe_wait(self, event):
        retry = self.retry
        while True:
            try:
                WebDriverWait(self.driver, self.timeout).until(event)
            except TimeoutException as e:
                if retry > 0:
                    self.driver.refresh()
                    retry -= 1
                    logging.info(f"{retry} retry left for waiting {event}")
                else:
                    raise e
            else:
                break # return is ok, as well.
            

        
    def safeClick(self, By, query):
        element_present = EC.element_to_be_clickable((By, query))
        clicked_element = WebDriverWait(self.driver,  self.timeout).until(element_present)
        self.driver.execute_script("arguments[0].click();", clicked_element)

    def store(self, item):
        json_object = json.dumps(item, indent=4)
        self.saved_file.write(",\n" + json_object)

    def scrape(self):
        self.saved_file.write("[\"BOF\"")
        retry = self.retry
        item_iterator = iter(self.crawl())
        while True:
            try:
                item=next(item_iterator)
                self.store(item)
            except (NoSuchWindowException, KeyboardInterrupt) as e:
                self.log_error()
                self.terminate()
                raise e
            except (NoSuchElementException) as e:
                logging.info("Finishing crawling")
                self.terminate()
            
            
            # logging.info("Element did not show up")
    def log_error(self):
        logging.info("An error has been occured!!!!!")
        logging.info(f"Stop at:  {self.driver.current_url}") 
        with open("./error.html", "w") as f:
            f.write(self.driver.page_source)

    def terminate(self):
        self.saved_file.write("\n]")
        self.driver.close()
        self.saved_file.close()
        sys.exit()

    def execute(self):
        self.login()
        self.scrape()
        self.terminate()
        
  
if __name__ == "__main__":
    import yaml
    with open('./config.yml', 'r') as f:
        config = yaml.safe_load(f)
    scraper = ChainAbuseScrapper(**config)
    # logging.info(scraper.chain_types)
    # scraper.scrape_chain_urls()
    scraper.execute()
    