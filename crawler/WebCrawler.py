from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
import requests, os, logging

class WebCrawler:
    def __init__(self, base_url, exclude_urls, driver_path=None ):
        chrome_options = Options()
        # Run Chrome in headless mode
        chrome_options.add_argument("--headless")

        # Disable GPU hardware acceleration
        chrome_options.add_argument("--disable-gpu")

        # Disable infobars on startup
        chrome_options.add_argument("--disable-infobars")

        # Disable notifications
        chrome_options.add_argument("--disable-notifications")

        # Disable pop-up blocking
        chrome_options.add_argument("--disable-popup-blocking")

        # Disable automatic software updates
        chrome_options.add_argument("--disable-software-rasterizer")

        # Disable prompt for user data sync
        chrome_options.add_argument("--disable-sync")

        # Disable translate UI
        chrome_options.add_argument("--disable-translate")

        # Disable save password bubbles
        chrome_options.add_argument("--disable-save-password-bubble")

        # Disable autoplay of embedded videos
        chrome_options.add_argument("--autoplay-policy=user-gesture-required")

        # Additional options to speed up Chrome
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        self.driver = webdriver.Chrome(options=chrome_options)
        self.base_url = base_url
        self.exclude_urls = exclude_urls

    def visit_url(self, url):
        try:
            self.driver.set_page_load_timeout(20)  # Set timeout to 10 seconds
            self.driver.get(url)
        except Exception as e:
            logging.error(f"Error occured for page to load {url}")


    def get_elements(self, strategy, element_selector):
        try:
            elements = self.driver.find_elements(strategy, element_selector)
            return elements
        except Exception as e:
            logging.error(f"Error: {e}")
            return None
        
    def parse_tables(self):
        tables = self.get_elements(By.TAG_NAME, "table")
        table_dict = {}

        for table in tables:
            rows = table.find_elements(By.TAG_NAME, "tr")
            header = []
            #links = []

            for row in rows:
                row_dict = {}
                row_dict["metadata"] = {}
                ref_links = []

                cols = row.find_elements(By.TAG_NAME,"td")

                if cols:
                    key_links  = self.get_links(cols[0], exclude=True)
                    if key_links:
                       ref_links.extend(key_links)
                    else:
                        key_links = self.get_solicitation_links(cols[3])

                    ref_links.extend(self.get_links(cols[2], exclude=True))
                    ref_links.extend(self.get_links(cols[3], exclude=True))
                
                if not header:
                    header = [col.text.strip() for col in row.find_elements(By.TAG_NAME,"th")]
                row_data = [col.text for col in cols]
                
                if row_data:
                    for i in range(len(header)):
                        if i < len(row_data):
                            row_dict["metadata"][header[i]] = row_data[i].strip()
                    
                    if ref_links:
                        #refined_links = [item for item in ref_links if not any(item.startswith(prefix) for prefix in self.exclude_urls)]
                        #print(refined_links)
                        deduped_links = list(set(ref_links))
                        row_dict["links"] = deduped_links

                    if row_dict:
                        if key_links:
                            table_dict[key_links[0].strip()] = row_dict
            
            #refined_links = [item for item in links if not any(item.startswith(prefix) for prefix in self.exclude_urls)]
            #deduped_links = list(set(refined_links))

        return table_dict


    """     def get_links(self, element, exclude=False):
        links = []

        ref_links = element.find_elements(By.TAG_NAME, "a")

        if len(ref_links) > 0:
            for ref_link in ref_links:
                links.append(ref_link.get_attribute("href").strip())

        if exclude:
            links = [link for link in links if not any(link.startswith(prefix) for prefix in self.exclude_urls)]
        
        return links """
    
    def get_links(self, element, exclude=False):
        links = []

        ref_links = element.find_elements(By.TAG_NAME, "a")

        if len(ref_links) > 0:
            for ref_link in ref_links:
                link = ref_link.get_attribute("href").strip()
                if not link.startswith('mailto:') and not (exclude and any(link.startswith(prefix) for prefix in self.exclude_urls)):
                    links.append(link)
        
        return links

    def get_solicitation_links(self, element):
        links = []

        ref_links = element.find_elements(By.LINK_TEXT, "Solicitation Document")

        if len(ref_links) > 0:
            for ref_link in ref_links:
                links.append(ref_link.get_attribute("href").strip())
        
        return links
    
    def get_pdf(self, url):
        response = requests.get(url)
        return response

    def parse_page(self):
        main_content = self.driver.find_element(By.TAG_NAME, "body")
        return main_content.text

    def close(self):
        self.driver.quit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver.quit()
